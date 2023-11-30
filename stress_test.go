// Copyright (c) 2023 BVK Chaitanya

package topic

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestStress(t *testing.T) {
	const numReceivers = 10
	const numMessages = 10000

	topic := New[int64]()
	defer topic.Close()

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(numReceivers)
	for i := 0; i < numReceivers; i++ {
		recvr, rch, err := topic.Subscribe(0, true /* includeRecent */)
		if err != nil {
			t.Fatal(err)
		}

		go func(id int) {
			for stop := false; stop == false; {
				select {
				case v := <-rch:
					// log.Printf("%d: %v", id, v)
					_ = v
				case <-time.After(time.Second):
					stop = true
				}
			}
			recvr.Unsubscribe()
			wg.Done()
		}(i)
	}

	for i := 0; i < numMessages; i++ {
		topic.SendCh() <- rand.Int63()
	}
}
