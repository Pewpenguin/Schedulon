//go:build !windows

package worker

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// detectHostResources returns logical CPU count and total system memory in MiB.
// On Unix-like systems memory is read from /proc/meminfo when available.
func detectHostResources() (cpuCount uint32, memoryMBTotal uint64) {
	cpuCount = uint32(runtime.NumCPU())

	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return cpuCount, 0
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseUint(fields[1], 10, 64)
				if err == nil {
					memoryMBTotal = kb / 1024
				}
			}
			break
		}
	}
	return cpuCount, memoryMBTotal
}
