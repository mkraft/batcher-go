# Batcher Go

#### Converts a stream into categorized batches.

A `*batcher` receives a stream of messages (which are any `interface{}` instances as defined by the client) via `(*batcher).In`. 

Each message goes through the `Match` method of each handler until one or none match. If `Match` returns `true` then the message is put into an in-memory queue as named by the string returned by `Match`. 

After the handler's `Wait` duration elapses, that queue's messages are sent out in a batch (`[]interface{}`) via the `(*batcher).Out` channel.

## Install

```shell
$ go get github.com/mkraft/batcher-go
```

## Example

https://github.com/mkraft/wsproxy

## Tests

```shell
$ make test
$ make benchmark
```

## Godoc

https://pkg.go.dev/github.com/mkraft/batcher-go
