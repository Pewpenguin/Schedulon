//go:build windows

package worker

import (
	"runtime"
	"syscall"
	"unsafe"
)

var (
	kernel32                               = syscall.NewLazyDLL("kernel32.dll")
	procGetPhysicallyInstalledSystemMemory = kernel32.NewProc("GetPhysicallyInstalledSystemMemory")
)

func physicalMemoryKB() (uint64, error) {
	var memKB uint64
	r, _, err := procGetPhysicallyInstalledSystemMemory.Call(uintptr(unsafe.Pointer(&memKB)))
	if r == 0 {
		return 0, err
	}
	return memKB, nil
}

func detectHostResources() (cpuCount uint32, memoryMBTotal uint64) {
	cpuCount = uint32(runtime.NumCPU())
	kb, err := physicalMemoryKB()
	if err != nil || kb == 0 {
		return cpuCount, 0
	}
	return cpuCount, kb / 1024
}
