package store

import "time"

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type Data struct {
	Value      string
	List       []string
	Stream     []StreamEntry
	Type       string // "string", "list", "stream"
	Expiration time.Time
}
