module github.com/wow-look-at-my/go-ipc/interop

go 1.26

require github.com/wow-look-at-my/go-ipc v0.0.0 // go-toolchain:auto-branch

require (
	github.com/stretchr/testify v1.11.1
	github.com/wow-look-at-my/go-containers v0.0.0 // go-toolchain:auto-branch
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/wow-look-at-my/go-mmap v0.0.0 // indirect; go-toolchain:auto-branch
	github.com/wow-look-at-my/go-shm v0.0.0 // indirect; go-toolchain:auto-branch
	golang.org/x/sys v0.33.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/wow-look-at-my/go-ipc => ../

// ipcgen generates internal/demo/demo.go from the same tree, so it resolves locally.
require github.com/wow-look-at-my/go-ipc/ipcgen v0.0.0

replace github.com/wow-look-at-my/go-ipc/ipcgen => ../ipcgen

tool github.com/wow-look-at-my/go-ipc/ipcgen
