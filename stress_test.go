// Copyright (c) 2023 BVK Chaitanya

package topic

import (
	"math/rand"
	"sync"
	"testing"
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
			for v := range rch {
				_ = v
			}
			recvr.Unsubscribe()
			wg.Done()
		}(i)
	}

	for i := 0; i < numMessages; i++ {
		topic.SendCh() <- rand.Int63()
	}

	topic.Close()
}

func BenchmarkDispatch(b *testing.B) {
	topic := New[int64]()
	defer topic.Close()

	recvr, rch, err := topic.Subscribe(0, true /* includeRecent */)
	if err != nil {
		b.Fatal(err)
	}
	defer recvr.Unsubscribe()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Send(rand.Int63())
		<-rch
	}
	b.StopTimer()
}
