package store

import (
	"sync"
	"time"
)

type KeyValueStore struct {
	mu   sync.RWMutex
	data map[string]Data
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]Data),
	}
}

func (kv *KeyValueStore) Set(key, value string, expiration time.Time){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = Data{Value : value, Expiration: expiration}
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	data, ok := kv.data[key]
	if !ok {
		return "", false
	}

	if !data.Expiration.IsZero() && time.Now().After(data.Expiration) {
		delete(kv.data, key)
		return "", false
	}

	return data.Value, true
}