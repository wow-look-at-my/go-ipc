module github.com/wow-look-at-my/go-ipc/codegen/go

go 1.26

require (
	github.com/stretchr/testify v1.12.1
	github.com/wow-look-at-my/go-containers v0.0.0 // go-toolchain:auto-branch
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect

// ipcgen generates demo/demo.go from the same tree, so it resolves locally.
require github.com/wow-look-at-my/go-ipc/ipcgen v0.0.0

replace github.com/wow-look-at-my/go-ipc/ipcgen => ../../ipcgen

tool github.com/wow-look-at-my/go-ipc/ipcgen
