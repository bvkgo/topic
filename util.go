// Copyright (c) 2023 BVK Chaitanya

package topic

// Recent returns the latest message sent to the topic. Returns false if no
// message was sent to the topic.
func Recent[T any](t *Topic[T]) (_ T, _ bool) {
	if v := t.recentValue.Load(); v != nil {
		return v.(T), true
	}
	return
}
