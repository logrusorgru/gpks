gpks
=====

[![GoDoc](https://godoc.org/github.com/logrusorgru/gpks?status.svg)](https://godoc.org/github.com/logrusorgru/gpks)
[![WTFPL License](https://img.shields.io/badge/license-wtfpl-blue.svg)](http://www.wtfpl.net/about/)
[![Build Status](https://travis-ci.org/logrusorgru/gpks.svg)](https://travis-ci.org/logrusorgru/gpks)
[![Coverage Status](https://coveralls.io/repos/logrusorgru/gpks/badge.svg?branch=master)](https://coveralls.io/r/logrusorgru/gpks?branch=master)

Pure golang k-v storage with in-memory index based on protocol buffers v3

This project will be the alpha until you reach a stable Protocol Buffers v3.
Future versions of the project are likely to be incompatible with the current version.

### Install

Get or update

```bash
go get github.com/logrusorgru/gpks
```

Test

```bash
cd $GOPATH/src/github.com/logrusorgru/gpks
go test
```

### Usage

See `gpks_test.go` for example

### Licensing

Copyright &copy; 2015 Konstantin Ivanov <ivanov.konstantin@logrus.org.ru>  
This work is free. You can redistribute it and/or modify it under the
terms of the Do What The Fuck You Want To Public License, Version 2,
as published by Sam Hocevar. See the LICENSE.md file for more details.