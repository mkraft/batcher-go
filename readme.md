# Batcher Go

Converts a stream into categorized batches.

## Description

`(*batcher).In` receives a stream of messages (which are any `interface{}` instances as defined by the client). 

Each message goes through the `(*Handler).Match` method of each handler until one or none match. If `(*Handler).Match` returns `true` then the message is put into an in-memory queue as named by the string that `(*Handler).Match` also returned. 

After the handler's `(*Handler).Wait` duration elapses, that queue's messages are sent out as a batch (an `[]interface{}`) via the `(*batcher).Out` channel.

## Installation

```shell
$ go get github.com/mkraft/batcher-go
```

```go
import "github.com/mkraft/batcher-go"
```

## Example usage

https://github.com/mkraft/batcher-go-example

## Test/benchmark

```shell
$ make test
$ make benchmark
```

## Godoc

https://pkg.go.dev/github.com/mkraft/batcher-go
