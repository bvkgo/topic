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

		m, receiveCh, err := topic.Subscribe(0, false)
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

	if _, _, err := topic.Subscribe(0, false); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("want os.ErrClosed, got %v", err)
	}
}

func TestEmptyTopic(t *testing.T) {
	topic := New[int64]()
	defer topic.Close()

	numMsgs := 5
	for i := 0; i < numMsgs; i++ {
		topic.SendCh() <- rand.Int63()
	}
}

func TestQueueLimitRecent(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sub, subCh, err := topic.Subscribe(1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	numMsgs := 5
	for i := 1; i <= numMsgs; i++ {
		topic.SendCh() <- i
	}

	if v := <-subCh; v != numMsgs {
		t.Fatalf("want %d, got %d", numMsgs, v)
	}

	sub.Unsubscribe()

	if v := <-subCh; v != 0 {
		t.Fatalf("want 0, got %d", v)
	}
}

func TestQueueLimitOldest(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sub, subCh, err := topic.Subscribe(-1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	numMsgs := 5
	for i := 1; i <= numMsgs; i++ {
		topic.SendCh() <- i
	}

	if v := <-subCh; v != 1 {
		t.Fatalf("want %d, got %d", 1, v)
	}

	sub.Unsubscribe()

	if v := <-subCh; v != 0 {
		t.Fatalf("want 0, got %d", v)
	}
}

func TestIncludeRecent(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sub1, sub1Ch, err := topic.Subscribe(0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Unsubscribe()

	for i := 0; i < 5; i++ {
		topic.SendCh() <- i
	}

	sub2, sub2Ch, err := topic.Subscribe(0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Unsubscribe()

	for i := 5; i < 10; i++ {
		topic.SendCh() <- i
	}

	if v := <-sub1Ch; v != 0 {
		t.Fatalf("want %d, got %d", 0, v)
	}
	if v := <-sub2Ch; v != 4 {
		t.Fatalf("want %d, got %d", 4, v)
	}
}
