package ipc

import "unsafe"

// unsafe_Pointer converts a UTF-16 name for a kernel32 call. It keeps the one
// unsafe import on Windows next to the calls that need it.
func unsafe_Pointer[T any](p *T) unsafe.Pointer { return unsafe.Pointer(p) }
