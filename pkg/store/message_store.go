package store

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"time"
)

const (
	X_SECONDS         = 10
	ReconcileInterval = 10 * time.Second
)

type MessageStore struct {
	messages map[string]*Message
	timeHeap *MessageHeap
}

func NewMessageStore() *MessageStore {
	return &MessageStore{
		messages: make(map[string]*Message),
		timeHeap: &MessageHeap{},
	}
}

func (ms *MessageStore) AddMessage(msg *Message) {
	ms.messages[msg.CompositeKey] = msg
	heap.Push(ms.timeHeap, msg)
}

type Message struct {
	CompositeKey string
	SourceData1  *map[string]interface{}
	SourceData2  *map[string]interface{}
	CreatedAt    time.Time
}

func NewMessage(compositeKey string) *Message {
	return &Message{
		CompositeKey: compositeKey,
		CreatedAt:    time.Now(),
	}
}

type MessageHeap []*Message

func (h MessageHeap) Len() int           { return len(h) }
func (h MessageHeap) Less(i, j int) bool { return h[i].CreatedAt.Before(h[j].CreatedAt) } // Min heap
func (h MessageHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MessageHeap) Peek() *Message {
	if h.Len() == 0 {
		return nil
	}
	return (*h)[0]
}

func (h *MessageHeap) Push(x interface{}) {
	*h = append(*h, x.(*Message))
}

func (h *MessageHeap) Pop() interface{} { // TODO revist this method for potential copy performance between stack and heap
	old := *h         // save the old heap on the stack
	n := len(old)     // get the length of the old heap from the stack
	x := old[n-1]     // get the last element from the old heap
	*h = old[0 : n-1] // remove the last element from the old heap
	return x          // return the last element
}

func (m *Message) AddSource(queryState *map[string]interface{}) error {
	//m.jsonMsg = append(m.jsonMsg, msg)
	if m.SourceData1 == nil {
		m.SourceData1 = queryState
		return nil
	} else if m.SourceData2 == nil {
		m.SourceData2 = queryState
		return nil
	}
	return fmt.Errorf("source one and two already set: messages already exists in store: %v", m)
}

func (m *MessageStore) StoreMessage(key string, queryState *map[string]interface{}) (*Message, error) {
	// check if the message already exists in the store
	message, ok := m.messages[key]
	if !ok {
		// create a new message and add the source
		message = NewMessage(key)
		m.AddMessage(message)
		m.messages[key] = message // pointer to the message
	}

	err := message.AddSource(queryState)
	if err != nil {
		log.Printf("error: %v", err)
		return nil, fmt.Errorf("error adding source to message: %v", err)
	}
	return message, nil
}

func (m *MessageStore) LoadMessage(key string) (*Message, bool) {
	actual, ok := m.messages[key]
	if !ok {
		return nil, ok
	}
	return actual, ok
}

func (m *MessageStore) RemoveMessage(key string) {
	message, ok := m.messages[key]
	if !ok {
		return
	}
	message.SourceData1 = nil
	message.SourceData2 = nil
	delete(m.messages, key)
}

func (ms *MessageStore) Reconcile() {
	now := time.Now()
	log.Printf("reconciling messages iteration started: %v\n", now)
	for ms.timeHeap.Len() > 0 {
		item := ms.timeHeap.Peek() // Peek at the top item without removing it
		if now.Sub(item.CreatedAt) > X_SECONDS*time.Second {
			// TODO debug messaging
			log.Printf("reconciling message: %v\n given time: %v", item, item.CreatedAt)
			heap.Pop(ms.timeHeap) // Remove the item from the heap
			ms.RemoveMessage(item.CompositeKey)
		} else {
			break
		}
	}
}

func (ms *MessageStore) StartReconcile(ctx context.Context) {
	ticker := time.NewTicker(ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ms.Reconcile()
		case <-ctx.Done():
			log.Println("Stopping reconcile loop")
			//close(ms.reconcileDone)
			return
		}
	}
}
