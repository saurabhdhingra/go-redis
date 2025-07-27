package store

import (
	"fmt"
	"strconv"
	"strings"
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

// TYPE returns the type of the value stored at key.
func (kv *KeyValueStore) TYPE(key string) string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	data, ok := kv.data[key]
	if !ok {
		return "none"
	}
	return data.Type
}

// XADD adds an entry to a stream, creating the stream if it doesn't exist.
func (kv *KeyValueStore) XADD(key, id string, fields map[string]string) (string, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data, ok := kv.data[key]
	if !ok || data.Type != "stream" {
		data = Data{Type: "stream", Stream: []StreamEntry{}}
	}
	// Generate ID if needed
	if id == "*" {
		id = generateStreamID(data.Stream)
	} else {
		if !validateStreamID(id) {
			return "", fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
		if !isIDGreater(id, data.Stream) {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream's last entry")
		}
	}
	entry := StreamEntry{ID: id, Fields: fields}
	data.Stream = append(data.Stream, entry)
	kv.data[key] = data
	return id, nil
}

// XRANGE returns entries in a stream between start and end IDs (inclusive), with optional count.
func (kv *KeyValueStore) XRANGE(key, start, end string, count int) ([]StreamEntry, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	data, ok := kv.data[key]
	if !ok || data.Type != "stream" {
		return nil, nil
	}
	result := []StreamEntry{}
	for _, entry := range data.Stream {
		if inRange(entry.ID, start, end) {
			result = append(result, entry)
			if count > 0 && len(result) >= count {
				break
			}
		}
	}
	return result, nil
}

// XREAD reads entries from multiple streams, starting from given IDs. Blocking not implemented yet.
func (kv *KeyValueStore) XREAD(streams map[string]string, count int, block time.Duration) (map[string][]StreamEntry, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	result := make(map[string][]StreamEntry)
	for key, startID := range streams {
		data, ok := kv.data[key]
		if !ok || data.Type != "stream" {
			continue
		}
		entries := []StreamEntry{}
		for _, entry := range data.Stream {
			if isIDGreaterOrEqual(entry.ID, startID) {
				entries = append(entries, entry)
				if count > 0 && len(entries) >= count {
					break
				}
			}
		}
		if len(entries) > 0 {
			result[key] = entries
		}
	}
	return result, nil
}

// Helper: generate a new stream ID (simple implementation: increment last ID or use timestamp)
func generateStreamID(stream []StreamEntry) string {
	if len(stream) == 0 {
		return fmt.Sprintf("%d-0", time.Now().UnixMilli())
	}
	last := stream[len(stream)-1].ID
	parts := strings.Split(last, "-")
	ms, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.Atoi(parts[1])
	if ms == time.Now().UnixMilli() {
		seq++
	} else {
		ms = time.Now().UnixMilli()
		seq = 0
	}
	return fmt.Sprintf("%d-%d", ms, seq)
}

// Helper: validate stream ID format
func validateStreamID(id string) bool {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return false
	}
	_, err1 := strconv.ParseInt(parts[0], 10, 64)
	_, err2 := strconv.Atoi(parts[1])
	return err1 == nil && err2 == nil
}

// Helper: check if id is greater than last entry
func isIDGreater(id string, stream []StreamEntry) bool {
	if len(stream) == 0 {
		return true
	}
	last := stream[len(stream)-1].ID
	return compareStreamIDs(id, last) > 0
}

// Helper: check if id >= startID
func isIDGreaterOrEqual(id, startID string) bool {
	return compareStreamIDs(id, startID) >= 0
}

// Helper: compare two stream IDs
func compareStreamIDs(a, b string) int {
	ap := strings.Split(a, "-")
	bp := strings.Split(b, "-")
	ams, _ := strconv.ParseInt(ap[0], 10, 64)
	aseq, _ := strconv.Atoi(ap[1])
	bms, _ := strconv.ParseInt(bp[0], 10, 64)
	bseq, _ := strconv.Atoi(bp[1])
	if ams < bms {
		return -1
	} else if ams > bms {
		return 1
	}
	if aseq < bseq {
		return -1
	} else if aseq > bseq {
		return 1
	}
	return 0
}

// Helper: check if id is in [start, end] range
func inRange(id, start, end string) bool {
	if (start == "-" || compareStreamIDs(id, start) >= 0) && (end == "+" || compareStreamIDs(id, end) <= 0) {
		return true
	}
	return false
}
