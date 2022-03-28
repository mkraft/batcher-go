package batcher_test

import (
	"context"
	"fmt"
	"time"

	"github.com/mkraft/batcher-go"
)

func ExampleNewProxy() {
	myHandler := &batcher.Handler{
		Wait: 3 * time.Second,
		Match: func(msg batcher.Message) (string, bool) {
			if msg.ID() != "some-type" {
				return "", false
			}
			return "some-type-queue", true
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	proxy := batcher.NewBatcher(ctx, []*batcher.Handler{myHandler})

	proxy.In(&testMessage{id: "some-type", data: "data1"})
	proxy.In(&testMessage{id: "some-type", data: "data2"})

	cancel()

	fmt.Printf("%+v", <-proxy.Out)

	// Output: [id: some-type, data: data1 id: some-type, data: data2]
}
