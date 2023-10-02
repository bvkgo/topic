// Copyright (c) 2023 BVK Chaitanya

package topic

import "testing"

func TestRecent(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	if _, ok := Recent(topic); ok {
		t.Fatalf("want false, got true")
	}

	numMsgs := 5
	for i := 1; i <= numMsgs; i++ {
		topic.SendCh() <- i
	}

	if v, ok := Recent(topic); !ok {
		t.Fatalf("want true, got false")
	} else if v != numMsgs {
		t.Fatalf("want %d, got %d", numMsgs, v)
	}
}
