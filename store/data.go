package store

import "time"

type Data struct {
	Value      string
	Expiration time.Time
}
