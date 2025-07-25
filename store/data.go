package store

import "time"

type Data struct {
	Value      string
	List       []string
	Type       string // "string" or "list"
	Expiration time.Time
}
