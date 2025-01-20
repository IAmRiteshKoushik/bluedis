package cuckoo

import (
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
)

type CuckooFilter struct {
	buckets    [][]uint32
	maxKicks   int
	size       uint
	bucketSize int
	capacity   uint
	mu         sync.RWMutex //this is for the safety of the thread
}

type Config struct { //this is for the configuration of the new cuckoo filter
	Capacity   uint
	BucketSize int
	MaxKicks   int
}

func DefaultConfig() *Config { //for the default config
	return &Config{
		Capacity:   1000000,
		BucketSize: 6,
		MaxKicks:   700,
	}
}

// this creates a new cuckooFilter
func New(config *Config) *CuckooFilter {
	if config == nil {
		config = DefaultConfig()
	}
	numBuckets := PowNextof2(config.Capacity / uint(config.BucketSize))
	return &CuckooFilter{
		buckets:    make([][]uint32, numBuckets),
		size:       0,
		maxKicks:   config.MaxKicks,
		bucketSize: config.BucketSize,
		capacity:   config.Capacity,
	}
}

// this adds an item to the filter
func (cf *CuckooFilter) Add(item []byte) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	if cf.size >= cf.capacity {
		return false
	}

	fingpr := fingerprint(item)
	i1 := hash(item) % uint32(len(cf.buckets))
	i2 := alternateIndex(i1, fingpr, uint32(len(cf.buckets)))

	//inserting in either bucket
	if cf.insertIntoBucket(i1, fingpr) || cf.insertIntoBucket(i2, fingpr) {
		cf.size++
		return true
	}

	//cuckoo hashing
	i := i1
	for k := 0; k < cf.maxKicks; k++ {
		j := rand.Intn(cf.bucketSize)
		fingpr, cf.buckets[i][j] = cf.buckets[i][j], fingpr
		i = alternateIndex(i, fingpr, uint32(len(cf.buckets)))

		if cf.insertIntoBucket(i, fingpr) {
			cf.size++ //incremejt the sizee
			return true
		}
	}
	return false
}

// this checks if there is an item in the filter or not
func (cf *CuckooFilter) Contains(item []byte) bool {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	fingpr := fingerprint(item)
	i1 := hash(item) % uint32(len(cf.buckets))
	i2 := alternateIndex(i1, fingpr, uint32(len(cf.buckets)))

	return cf.containsFingerprint(i1, fingpr) || cf.containsFingerprint(i2, fingpr)
}

// removes an item fromt eh filter
func (cf *CuckooFilter) Remove(item []byte) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	fingpr := fingerprint(item)
	i1 := hash(item) % uint32(len(cf.buckets))
	i2 := alternateIndex(i1, fingpr, uint32(len(cf.buckets)))

	if cf.deleteFromBucket(i1, fingpr) || cf.deleteFromBucket(i2, fingpr) {
		cf.size-- //now decrement the size
		return true
	}
	return false
}

// the helper function
func (cf *CuckooFilter) insertIntoBucket(i uint32, fingpr uint32) bool {
	if cf.buckets[i] == nil {
		cf.buckets[i] = make([]uint32, cf.bucketSize)
	}
	for j := 0; j < cf.bucketSize; j++ {
		if cf.buckets[i][j] == 0 {
			cf.buckets[i][j] = fingpr
			return true
		}
	}
	return false
}

func (cf *CuckooFilter) containsFingerprint(i uint32, fingpr uint32) bool {
	if cf.buckets[i] == nil {
		return false
	}
	for j := 0; j < cf.bucketSize; j++ {
		if cf.buckets[i][j] == fingpr {
			return true
		}
	}
	return false
}

func (cf *CuckooFilter) deleteFromBucket(i uint32, fingpr uint32) bool {
	if cf.buckets[i] == nil {
		return false
	}
	for j := 0; j < cf.bucketSize; j++ {
		if cf.buckets[i][j] == fingpr {
			cf.buckets[i][j] = 0
			return true
		}
	}
	return false
}

func fingerprint(data []byte) uint32 {
	h := fnv.New32a()
	h.Write(data)
	return h.Sum32()
}

func hash(data []byte) uint32 {
	h := fnv.New32a()
	h.Write(data)
	return h.Sum32()
}

func alternateIndex(i uint32, fp uint32, numBuckets uint32) uint32 {
	return (i ^ (fp * 0x5bd1e995)) % numBuckets
}

func PowNextof2(n uint) uint {
	return uint(math.Pow(2, math.Ceil(math.Log2(float64(n)))))
}
