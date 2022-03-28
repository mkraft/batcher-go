package batchelor_test

import (
	"context"
	"fmt"
	"time"

	batchelor "github.com/mkraft/batchelorgo"
)

func ExampleNewProxy() {
	myHandler := &batchelor.Handler{
		Wait: 3 * time.Second,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "some-type" {
				return "", false
			}
			return "some-type-queue", true
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{myHandler})

	proxy.In(&testMessage{id: "some-type", data: "data1"})
	proxy.In(&testMessage{id: "some-type", data: "data2"})

	cancel()

	fmt.Printf("%+v", <-proxy.Out)

	// Output: [id: some-type, data: data1 id: some-type, data: data2]
}
