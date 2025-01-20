package cuckoo

import (
    "strconv"
    "testing"
)

func TestCuckooFilter(t *testing.T) {
    tests := []struct {
        name       string
        config     *Config
        itemsToAdd int
    }{
        {
            name: "small filter",
            config: &Config{
                Capacity:   100,
                BucketSize: 4,
                MaxKicks:   500,
            },
            itemsToAdd: 50,
        },
        {
            name: "medium filter",
            config: &Config{
                Capacity:   1000,
                BucketSize: 4,
                MaxKicks:   500,
            },
            itemsToAdd: 500,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            cf := New(tt.config)

            //insertion test
            for i := 0; i < tt.itemsToAdd; i++ {
                item := []byte("item" + strconv.Itoa(i))
                if !cf.Add(item) {
                    t.Errorf("Failed to insert item %d", i)
                }
            }

            //lookup of test
            for i := 0; i < tt.itemsToAdd; i++ {
                item := []byte("item" + strconv.Itoa(i))
                if !cf.Contains(item) {
                    t.Errorf("Item %d not found", i)
                }
            }

            //Test deletion
            for i := 0; i < tt.itemsToAdd/2; i++ {
                item := []byte("item" + strconv.Itoa(i))
                if !cf.Remove(item) {
                    t.Errorf("Faileing o delete item %d", i)
                }
                if cf.Contains(item) {
                    t.Errorf("Item %d still present after deletion", i)
                }
            }
        })
    }
}

func BenchmarkCuckooFilter(b *testing.B) {
    cf := New(DefaultConfig())
    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        item := []byte("item" + strconv.Itoa(i))
        cf.Add(item)
        cf.Contains(item)
    }
}