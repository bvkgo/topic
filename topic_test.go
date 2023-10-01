// Copyright (c) 2019 BVK Chaitanya

package topic

import (
	"errors"
	"math/rand"
	"os"
	"sync"
	"testing"
)

func TestTopic(t *testing.T) {
	topic := New[int64]()
	defer topic.Close()

	var wg sync.WaitGroup
	defer wg.Wait()

	numMembers := 5
	for i := 0; i < numMembers; i++ {
		i := i

		m, receiveCh, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			for msg := range receiveCh {
				t.Logf("%d: received %+v", i, msg)
			}

			m.Unsubscribe()
		}()
	}

	numMsgs := 5
	for i := 0; i < numMsgs; i++ {
		topic.SendCh() <- rand.Int63()
	}

	if err := topic.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := topic.Subscribe(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("want os.ErrClosed, got %v", err)
	}
}
