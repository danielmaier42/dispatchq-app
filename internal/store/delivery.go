package store

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// PendingDelivery is a notification one channel has not taken yet: Pushover
// while the Mac had no network, the desktop while macOS refused the banner.
// The daemon tries it again with growing pauses and drops it once the channel
// takes it or the retry window ends, so a message raised in a short wake
// without network still reaches the phone once the Mac is online.
type PendingDelivery struct {
	// ID is unique per notification and channel.
	ID string `json:"id"`
	// Channel names the channel still to deliver to ("pushover", "desktop").
	Channel string `json:"channel"`
	// Notification is the message as the channel is to receive it. It is kept
	// as the notifier package encodes it, so the store does not restate its
	// fields.
	Notification json.RawMessage `json:"notification"`
	// Created is when the notification was first sent.
	Created time.Time `json:"created"`
	// Attempts counts the tries so far, NextTry is when the next one is due.
	Attempts int       `json:"attempts"`
	NextTry  time.Time `json:"next_try"`
	// LastError is why the last try failed.
	LastError string `json:"last_error,omitempty"`
}

const deliveriesFile = "deliveries.json"

// deliveryList is the on-disk delivery outbox. It is removed when empty, which
// it is unless a channel is failing.
var deliveryList = jsonList[PendingDelivery]{file: deliveriesFile, field: "pending", dropWhenEmpty: true}

// PendingDeliveries returns the deliveries still waiting, oldest first.
func (s *Store) PendingDeliveries() ([]PendingDelivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return deliveryList.load(s)
}

// AddPendingDeliveries queues deliveries. An id already queued is replaced,
// so queueing the same delivery twice keeps one.
func (s *Store) AddPendingDeliveries(add ...PendingDelivery) error {
	for _, d := range add {
		if d.ID == "" || d.Channel == "" {
			return fmt.Errorf("add pending delivery: missing id or channel")
		}
	}
	return deliveryList.update(s, func(list *[]PendingDelivery) error {
		for _, d := range add {
			*list = slices.DeleteFunc(*list, func(q PendingDelivery) bool { return q.ID == d.ID })
			*list = append(*list, d)
		}
		return nil
	})
}

// UpdatePendingDeliveries atomically applies fn to the outbox.
func (s *Store) UpdatePendingDeliveries(fn func(*[]PendingDelivery) error) error {
	return deliveryList.update(s, fn)
}
