package pgscratch

import "syscall"

// tmpfsMinFree is the free space a tmpfs needs before clusters go there.
// Docker's default 64MB /dev/shm is too small and is skipped.
const tmpfsMinFree = 512 << 20

// tmpfsBaseDir returns /dev/shm/pgscratch when /dev/shm is a tmpfs with
// room, so data files never touch a disk.
func tmpfsBaseDir() string {
	var st syscall.Statfs_t
	if err := syscall.Statfs("/dev/shm", &st); err != nil {
		return ""
	}
	const tmpfsMagic = 0x01021994
	if int64(st.Type) != tmpfsMagic {
		return ""
	}
	if uint64(st.Bavail)*uint64(st.Bsize) < tmpfsMinFree {
		return ""
	}
	return "/dev/shm/pgscratch"
}
