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

func (kv *KeyValueStore) SET(key, value string, expiration time.Time) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = Data{Value: value, Expiration: expiration}
}

func (kv *KeyValueStore) GET(key string) (string, bool) {
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

// LPUSH inserts all the specified values at the head of the list stored at key.
func (kv *KeyValueStore) LPUSH(key string, elements []string) (int, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data, ok := kv.data[key]
	if !ok || data.Type != "list" {
		data = Data{Type: "list", List: []string{}}
	}
	// Prepend elements in order
	for i := len(elements) - 1; i >= 0; i-- {
		data.List = append([]string{elements[i]}, data.List...)
	}
	kv.data[key] = data
	return len(data.List), nil
}

// LRANGE returns the specified elements of the list stored at key.
func (kv *KeyValueStore) LRANGE(key string, start, end int) ([]string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	data, ok := kv.data[key]
	if !ok || data.Type != "list" {
		return []string{}, nil
	}
	l := len(data.List)
	if start < 0 {
		start = l + start
	}
	if end < 0 {
		end = l + end
	}
	if start < 0 {
		start = 0
	}
	if end >= l {
		end = l - 1
	}
	if start > end || start >= l {
		return []string{}, nil
	}
	return data.List[start : end+1], nil
}

// LLEN returns the length of the list stored at key.
func (kv *KeyValueStore) LLEN(key string) (int, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	data, ok := kv.data[key]
	if !ok || data.Type != "list" {
		return 0, nil
	}
	return len(data.List), nil
}

// LPOP removes and returns the first element of the list stored at key.
func (kv *KeyValueStore) LPOP(key string) (string, bool, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data, ok := kv.data[key]
	if !ok || data.Type != "list" || len(data.List) == 0 {
		return "", false, nil
	}
	val := data.List[0]
	data.List = data.List[1:]
	kv.data[key] = data
	return val, true, nil
}

// BLPOP is a blocking LPOP. For simplicity, this implementation is non-blocking and just calls LPOP on the first key with a non-empty list.
func (kv *KeyValueStore) BLPOP(keys []string, timeout time.Duration) ([]string, string, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, key := range keys {
		data, ok := kv.data[key]
		if ok && data.Type == "list" && len(data.List) > 0 {
			val := data.List[0]
			data.List = data.List[1:]
			kv.data[key] = data
			return []string{val}, key, nil
		}
	}
	return nil, "", nil
}
