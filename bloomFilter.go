package main

import (
	"fmt"
	"hash"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/pierrec/xxHash/xxHash32"
	"github.com/twmb/murmur3"
)

type BloomFilter struct {
	filter []uint8
	size   uint32
}

// All the hashfunctions will be stored in this map
var hashFunctions = map[string]hash.Hash32{
	"murmurhash": murmur3.SeedNew32(uint32(time.Now().Unix())),
	"fnvhash":    fnv.New32(),
	"xxhash":     xxHash32.New(uint32(time.Now().Unix())),
}

// A common hash function that takes in the hasher and returns the hash
func getHash(hasher hash.Hash32, data string) uint32 {
	hasher.Write([]byte(data))
	hashedSum := hasher.Sum32()
	hasher.Reset()
	return hashedSum
}

// For each idx given by hash%size, mark that bloom filter bucket as true
// We use bit manipulation to propogate the bitIdx to the appropriate bit and set it to 1

// Idx Calculations
// The total size of the bloom filter is the size*8, as we are storing the 1's in BITS
// byteIdx is hashIdx/8, as technically we only have "size" no.of array indexes not "size*8"
// bitIdx is hashIdx%8 to get the exact bit in the byte
func (bf *BloomFilter) Add(data string) {
	for _, hasher := range hashFunctions {
		hashIdx := getHash(hasher, data) % (bf.size * 8)
		byteIdx := hashIdx / 8
		bitIdx := hashIdx % 8
		// fmt.Println(hashIdx, byteIdx, bitIdx)
		bf.filter[byteIdx] |= (1 << bitIdx)
	}
}

// IF there exists "true" for every hash%size in the filter, it means the element exists in the filter
// Even if one hash is off, the element doesn't exist, return false
func (bf *BloomFilter) Exists(data string) bool {
	for _, hasher := range hashFunctions {
		hashIdx := getHash(hasher, data) % (bf.size * 8)
		byteIdx := hashIdx / 8
		bitIdx := hashIdx % 8
		if (bf.filter[byteIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

func NewBloomFilter(size uint32) BloomFilter {
	return BloomFilter{
		filter: make([]uint8, size),
		size:   size,
	}
}

func bloomFilter() {
	// Testing to see how the false-positivity rate scales as compared to bloomfilter size
	for j := 75000; j < 100000; j += 500 {
		bloom := NewBloomFilter(uint32(j))
		// Adding 75000 elements
		// Checking false positivity for 25000 elements
		for i := 0; i < 75000; i++ {
			bloom.Add(strconv.Itoa(i))
		}

		falsePositives := 0.0
		for i := 75000; i < 100000; i++ {
			if bloom.Exists(strconv.Itoa(i)) {
				falsePositives++
			}
		}
		fmt.Println(falsePositives / 25000)
	}

}
