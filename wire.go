package pgscratch

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PgError is an error the server reported.
type PgError struct {
	Severity string
	Code     string // SQLSTATE, such as "55006"
	Message  string
	Detail   string
	Hint     string
}

func (e *PgError) Error() string {
	msg := fmt.Sprintf("%s: %s (SQLSTATE %s)", e.Severity, e.Message, e.Code)
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// SQLSTATE codes pgscratch reacts to.
const (
	codeObjectInUse      = "55006" // source database is being accessed by other users
	codeDuplicateDB      = "42P04"
	codeInvalidCatalog   = "3D000" // database does not exist
	codeCannotConnectNow = "57P03" // the database system is starting up
	codeAdminShutdown    = "57P01"
)

func errCode(err error) string {
	var pgErr *PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// Conn is a minimal PostgreSQL connection speaking the simple-query and
// COPY parts of the wire protocol over trust authentication. It is meant
// for setup work (Func steps, DDL); use a real driver for anything else.
// A Conn is not safe for concurrent use.
type Conn struct {
	nc        net.Conn
	r         *bufio.Reader
	w         *bufio.Writer
	dsn       string
	network   string
	address   string
	backendID [8]byte
	closeOnce sync.Once
}

// Connect opens a Conn to database on the cluster, over the unix socket when
// there is one.
func (c *Cluster) Connect(ctx context.Context, database string) (*Conn, error) {
	network, address := "tcp", fmt.Sprintf("%s:%d", c.Host(), c.port)
	if c.socketDir != "" {
		network, address = "unix", fmt.Sprintf("%s/.s.PGSQL.%d", c.socketDir, c.port)
	}
	return dial(ctx, network, address, c.opts.User, database, c.DSN(database))
}

func dial(ctx context.Context, network, address, user, database, dsn string) (*Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	conn := &Conn{nc: nc, r: bufio.NewReaderSize(nc, 32<<10), w: bufio.NewWriterSize(nc, 32<<10), dsn: dsn, network: network, address: address}
	stop := conn.watch(ctx)
	err = conn.startup(user, database)
	if stop() && err == nil {
		err = ctx.Err()
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// DSN is a postgres:// URL for the same database, for opening it with a
// real driver.
func (cn *Conn) DSN() string { return cn.dsn }

// Close closes the connection.
func (cn *Conn) Close() error {
	var err error
	cn.closeOnce.Do(func() {
		cn.w.Write([]byte{'X', 0, 0, 0, 4})
		cn.w.Flush()
		err = cn.nc.Close()
	})
	return err
}

// watch cancels the running query and closes the connection when ctx ends.
// The returned stop reports whether that happened.
func (cn *Conn) watch(ctx context.Context) (stop func() bool) {
	if ctx.Done() == nil {
		return func() bool { return false }
	}
	var fired bool
	var mu sync.Mutex
	cancel := context.AfterFunc(ctx, func() {
		mu.Lock()
		fired = true
		mu.Unlock()
		cn.sendCancel()
		cn.nc.SetDeadline(time.Now())
	})
	return func() bool {
		if cancel() {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		return fired
	}
}

func (cn *Conn) sendCancel() {
	if cn.backendID == [8]byte{} {
		return
	}
	nc, err := net.DialTimeout(cn.network, cn.address, time.Second)
	if err != nil {
		return
	}
	defer nc.Close()
	msg := make([]byte, 16)
	binary.BigEndian.PutUint32(msg[0:], 16)
	binary.BigEndian.PutUint32(msg[4:], 80877102)
	copy(msg[8:], cn.backendID[:])
	nc.SetDeadline(time.Now().Add(time.Second))
	nc.Write(msg)
}

func (cn *Conn) startup(user, database string) error {
	var body []byte
	body = binary.BigEndian.AppendUint32(body, 196608) // protocol 3.0
	for _, kv := range [][2]string{{"user", user}, {"database", database}, {"application_name", "pgscratch"}, {"client_encoding", "UTF8"}} {
		body = append(body, kv[0]...)
		body = append(body, 0)
		body = append(body, kv[1]...)
		body = append(body, 0)
	}
	body = append(body, 0)
	msg := binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))
	if _, err := cn.w.Write(append(msg, body...)); err != nil {
		return err
	}
	if err := cn.w.Flush(); err != nil {
		return err
	}
	var startErr error
	for {
		typ, payload, err := cn.receive()
		if err != nil {
			return err
		}
		switch typ {
		case 'R':
			if len(payload) < 4 {
				return errors.New("pgscratch: short authentication message")
			}
			if code := binary.BigEndian.Uint32(payload); code != 0 {
				return fmt.Errorf("pgscratch: server asked for authentication method %d; pgscratch clusters use trust", code)
			}
		case 'K':
			if len(payload) >= 8 {
				copy(cn.backendID[:], payload[:8])
			}
		case 'E':
			// Startup errors end the connection without ReadyForQuery.
			startErr = parseError(payload)
			return startErr
		case 'Z':
			return nil
		}
	}
}

func (cn *Conn) receive() (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(cn.r, header[:]); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(header[1:])) - 4
	if n < 0 || n > 1<<30 {
		return 0, nil, fmt.Errorf("pgscratch: bad message length %d", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(cn.r, payload); err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}

func (cn *Conn) send(typ byte, payload []byte) error {
	var header [5]byte
	header[0] = typ
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)+4))
	if _, err := cn.w.Write(header[:]); err != nil {
		return err
	}
	_, err := cn.w.Write(payload)
	return err
}

func parseError(payload []byte) *PgError {
	e := &PgError{}
	for len(payload) > 0 && payload[0] != 0 {
		field := payload[0]
		end := 1
		for end < len(payload) && payload[end] != 0 {
			end++
		}
		value := string(payload[1:end])
		switch field {
		case 'S':
			e.Severity = value
		case 'C':
			e.Code = value
		case 'M':
			e.Message = value
		case 'D':
			e.Detail = value
		case 'H':
			e.Hint = value
		}
		if end >= len(payload) {
			break
		}
		payload = payload[end+1:]
	}
	return e
}

// Exec runs one or more SQL statements with the simple-query protocol.
func (cn *Conn) Exec(ctx context.Context, sql string) error {
	_, err := cn.query(ctx, sql, nil)
	return err
}

// Query runs sql and returns every row as text. NULL reads as "".
func (cn *Conn) Query(ctx context.Context, sql string) ([][]string, error) {
	return cn.query(ctx, sql, nil)
}

// CopyFrom runs a COPY ... FROM STDIN statement, streaming data from r, and
// returns the number of rows copied.
func (cn *Conn) CopyFrom(ctx context.Context, copySQL string, r io.Reader) (int64, error) {
	var copied int64
	rows, err := cn.query(ctx, copySQL, r)
	if err == nil && len(rows) == 1 && len(rows[0]) == 1 {
		copied, _ = strconv.ParseInt(rows[0][0], 10, 64)
	}
	return copied, err
}

// query sends sql and reads until ReadyForQuery. For a COPY FROM STDIN it
// streams copyData and returns the COPY row count as a single cell.
func (cn *Conn) query(ctx context.Context, sql string, copyData io.Reader) (rows [][]string, err error) {
	stop := cn.watch(ctx)
	defer func() {
		if stop() {
			cn.nc.Close()
			err = ctx.Err()
		}
	}()
	if err := cn.send('Q', append([]byte(sql), 0)); err != nil {
		return nil, err
	}
	if err := cn.w.Flush(); err != nil {
		return nil, err
	}
	var queryErr error
	for {
		typ, payload, err := cn.receive()
		if err != nil {
			return nil, err
		}
		switch typ {
		case 'D':
			rows = append(rows, parseDataRow(payload))
		case 'C':
			tag := strings.TrimRight(string(payload), "\x00")
			if strings.HasPrefix(tag, "COPY ") && copyData != nil {
				rows = [][]string{{strings.TrimPrefix(tag, "COPY ")}}
			}
		case 'E':
			if queryErr == nil {
				queryErr = parseError(payload)
			}
		case 'G':
			if err := cn.streamCopy(copyData); err != nil {
				return nil, err
			}
		case 'H':
			cn.send('f', append([]byte("pgscratch does not support COPY TO STDOUT"), 0))
			cn.w.Flush()
		case 'Z':
			return rows, queryErr
		}
	}
}

func (cn *Conn) streamCopy(r io.Reader) error {
	if r == nil {
		if err := cn.send('f', append([]byte("no COPY data supplied"), 0)); err != nil {
			return err
		}
		return cn.w.Flush()
	}
	buf := make([]byte, 64<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if werr := cn.send('d', buf[:n]); werr != nil {
				return werr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			cn.send('f', append([]byte("reading COPY data: "+err.Error()), 0))
			return cn.w.Flush()
		}
	}
	if err := cn.send('c', nil); err != nil {
		return err
	}
	return cn.w.Flush()
}

func parseDataRow(payload []byte) []string {
	if len(payload) < 2 {
		return nil
	}
	n := int(binary.BigEndian.Uint16(payload))
	payload = payload[2:]
	row := make([]string, n)
	for i := 0; i < n && len(payload) >= 4; i++ {
		size := int32(binary.BigEndian.Uint32(payload))
		payload = payload[4:]
		if size < 0 {
			continue
		}
		row[i] = string(payload[:size])
		payload = payload[size:]
	}
	return row
}

// QuoteIdentifier quotes an SQL identifier: QuoteIdentifier(`a"b`) is `"a""b"`.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteLiteral quotes an SQL string literal (standard_conforming_strings on).
func QuoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
