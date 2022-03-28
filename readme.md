# Batchelor Go

A message batching and/or buffering module for incoming or outgoing messages.

Send messages in, if they meet some criteria then queue them with the other matched messages. When the duration elapses since the first message in that queue was received then send them back out as a batch.

## Install

```shell
$ go get github.com/mkraft/batchelorgo
```

## Example

https://github.com/mkraft/wsproxy

## Tests

```shell
$ make test
$ make benchmark
```

## Godoc

https://pkg.go.dev/github.com/mkraft/batchelorgo
