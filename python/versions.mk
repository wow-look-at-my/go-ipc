# Every CPython the Python code supports. python/ and codegen/ both test on
# each of them, and `make -C python interpreters` installs them.
PY_VERSIONS ?= 3.8 3.9 3.10 3.11 3.12 3.13 3.14
