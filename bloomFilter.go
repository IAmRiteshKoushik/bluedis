package main

import (
	"fmt"
	"hash"
	"hash/fnv"
	"strconv"

	"github.com/pierrec/xxHash/xxHash32"
	"github.com/twmb/murmur3"
)

type BloomFilter struct {
	filter []bool
	size   uint32
}

// var mHasher hash.Hash32 = murmur3.SeedNew32(uint32(time.Now().Unix()))
// TODO : Change the seeds to time.Now().Unix() AFTER Testing

// All the hashfunctions will be stored in this map
var hashFunctions = map[string]hash.Hash32{
	"murmurhash": murmur3.SeedNew32(uint32(12)),
	"fnvhash":    fnv.New32(),
	"xxhash":     xxHash32.New(19),
}

// A common hash function that takes in the hasher and returns the hash
func getHash(hasher hash.Hash32, data []byte) uint32 {
	hasher.Write(data)
	hashedSum := hasher.Sum32()
	hasher.Reset()
	return hashedSum
}

// For each idx given by hash%size, mark that bloom filter bucket as true
// TODO: Instead of using bool, use a uint8 and change just the bit to either 1 or 0
func (bf *BloomFilter) Add(data []byte) {
	for _, hasher := range hashFunctions {
		hashIdx := getHash(hasher, data) % bf.size
		bf.filter[hashIdx] = true
	}
}

// IF there exists "true" for every hash%size in the filter, it means the element exists in the filter
// Even if one hash is off, the element doesn't exist, return false
func (bf *BloomFilter) Exists(data []byte) bool {
	for _, hasher := range hashFunctions {
		hashIdx := getHash(hasher, data) % bf.size
		if !bf.filter[hashIdx] {
			return false
		}
	}
	return true
}

// TODO: Instead of using bool, use a uint8 and change just the bit to either 1 or 0
func NewBloomFilter(size uint32) BloomFilter {
	return BloomFilter{
		filter: make([]bool, size),
		size:   size,
	}
}

func bloomFilter() {
	bloom := NewBloomFilter(1000)
	fmt.Println(bloom.Exists([]byte("770")))
	for i := 0; i < 750; i++ {
		bloom.Add([]byte(strconv.Itoa(i)))
	}
	fmt.Println(bloom.Exists([]byte("770")))
}
