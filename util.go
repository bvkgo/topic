// Copyright (c) 2023 BVK Chaitanya

package topic

// Recent returns the most recent message sent to the Topic, along with a boolean
// indicating whether a message was available. If no messages have been sent or
// the Topic is closed, it returns the zero value of T and false.
//
// Example:
//
//	topic := topic.New[string]()
//	topic.SendCh() <- "latest"
//	if msg, ok := topic.Recent(topic); ok {
//	    fmt.Println("Recent message:", msg)
//	}
func Recent[T any](t *Topic[T]) (_ T, _ bool) {
	if v := t.recentValue.Load(); v != nil {
		return v.(T), true
	}
	return
}
