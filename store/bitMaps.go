package store

import (
	"errors"
	"sync"
)

// StringBitMap stores bit arrays for strings
type StringBitMap struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewStringBitMap() *StringBitMap {
	return &StringBitMap{
		data: make(map[string][]byte),
	}
}

// SetBit sets the bit at position pos for the given key
func (sb *StringBitMap) SetBit(key string, pos uint64, value bool) error {
	if pos > 1<<32 {
		return errors.New("position exceeds maximum allowed value")
	}

	sb.mu.Lock()
	defer sb.mu.Unlock()

	bytePos := pos / 8
	bitOffset := pos % 8

	// Initialize or expand the byte slice if needed
	if _, exists := sb.data[key]; !exists {
		sb.data[key] = make([]byte, bytePos+1)
	} else if bytePos >= uint64(len(sb.data[key])) {
		newData := make([]byte, bytePos+1)
		copy(newData, sb.data[key])
		sb.data[key] = newData
	}

	// Set or clear the bit
	if value {
		sb.data[key][bytePos] |= 1 << bitOffset
	} else {
		sb.data[key][bytePos] &^= 1 << bitOffset
	}

	return nil
}

// GetBit returns the bit value at position pos for the given key
func (sb *StringBitMap) GetBit(key string, pos uint64) (bool, error) {
	if pos > 1<<32 {
		return false, errors.New("position exceeds maximum allowed value")
	}

	sb.mu.Lock()
	defer sb.mu.Unlock()

	data, exists := sb.data[key]
	if !exists {
		return false, nil
	}

	bytePos := pos / 8
	if bytePos >= uint64(len(data)) {
		return false, nil
	}

	bitOffset := pos % 8
	return (data[bytePos] & (1 << bitOffset)) != 0, nil
}

// PopCount returns the number of set bits (population count) for the given key
func (sb *StringBitMap) PopCount(key string) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	data, exists := sb.data[key]
	if !exists {
		return 0, nil
	}

	count := 0
	for _, b := range data {
		// Use Brian Kernighan's algorithm for counting bits
		x := b
		for x != 0 {
			x &= (x - 1)
			count++
		}
	}
	return count, nil
}

