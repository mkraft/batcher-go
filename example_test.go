package batcher_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mkraft/batcher-go"
)

func ExampleNewBatcher() {
	myHandler := &batcher.Handler{
		Wait: 3 * time.Second,
		Match: func(raw interface{}) (string, bool) {
			msg, ok := raw.(*testMessage)
			if !ok {
				log.Printf("wrong type: %v", raw)
				return "", false
			}
			if msg.id != "some-type" {
				return "", false
			}
			return "some-type-queue", true
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	batcher := batcher.NewBatcher(ctx, []*batcher.Handler{myHandler})

	batcher.In(&testMessage{id: "some-type", data: "data1"})
	batcher.In(&testMessage{id: "some-type", data: "data2"})

	cancel()

	fmt.Printf("%+v", <-batcher.Out)

	// Output: [id: some-type, data: data1 id: some-type, data: data2]
}
