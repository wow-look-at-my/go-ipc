module github.com/wow-look-at-my/go-ipc/codegen/go // go-toolchain:generate=da0ee3a978cd

go 1.26

require (
	github.com/stretchr/testify v1.12.1
	github.com/wow-look-at-my/go-containers v0.0.0 // go-toolchain:auto-branch
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/cobra v1.10.2 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)

// ipcgen generates demo/demo.go from the same tree, so it resolves locally.
require github.com/wow-look-at-my/go-ipc/ipcgen v0.0.0 // indirect

replace github.com/wow-look-at-my/go-ipc/ipcgen => ../../ipcgen

tool github.com/wow-look-at-my/go-ipc/ipcgen
