package store

import (
    "math/rand"
    "sync"

    "golang.org/x/exp/constraints"
)

const SKIPLIST_MAXLEVEL = 32 
const SKIPLIST_P = 0.25

type SortedSet[K constraints.Ordered, SCORE constraints.Ordered, V any] struct {
    Mu     sync.Mutex
    Header *SortedSetNode[K, SCORE, V]
    Tail   *SortedSetNode[K, SCORE, V]
    Length int64
    Level  int
    Dict   map[K]*SortedSetNode[K, SCORE, V]
}

type SortedSetNode[K constraints.Ordered, SCORE constraints.Ordered, V any] struct {
    Score  SCORE
    Key    K
    Value  V
    Level  []SortedSetLevel[K, SCORE, V]
}

type SortedSetLevel[K constraints.Ordered, SCORE constraints.Ordered, V any] struct {
    Forward  *SortedSetNode[K, SCORE, V]
    Span     int64
    Backward *SortedSetNode[K, SCORE, V]
}

func createNode[K constraints.Ordered, SCORE constraints.Ordered, V any](Level int, score SCORE, key K, value V) *SortedSetNode[K, SCORE, V] {
    node := SortedSetNode[K, SCORE, V]{
        Score: score,
        Key:   key,
        Value: value,
        Level: make([]SortedSetLevel[K, SCORE, V], Level),
    }
    return &node
}

func randomLevel() int {
    Level := 1
    for float64(rand.Int31()&0xFFFF) < float64(SKIPLIST_P*0xFFFF) {
        Level += 1
    }
    if Level < SKIPLIST_MAXLEVEL {
        return Level
    }
    return SKIPLIST_MAXLEVEL
}

func NewSortedSet[K constraints.Ordered, SCORE constraints.Ordered, V any]() *SortedSet[K, SCORE, V] {
    sortedSet := SortedSet[K, SCORE, V]{
        Level: 1,
        Dict:  make(map[K]*SortedSetNode[K, SCORE, V]),
    }
    var emptyKey K
    var emptyScore SCORE
    var emptyValue V
    sortedSet.Header = createNode(SKIPLIST_MAXLEVEL, emptyScore, emptyKey, emptyValue)
    return &sortedSet
}

func (this *SortedSet[K, SCORE, V]) insertNode(score SCORE, key K, value V) *SortedSetNode[K, SCORE, V] {
    var update [SKIPLIST_MAXLEVEL]*SortedSetNode[K, SCORE, V]
    var rank [SKIPLIST_MAXLEVEL]int64
    x := this.Header
    for i := this.Level - 1; i >= 0; i-- {
        if this.Level-1 == i {
            rank[i] = 0
        } else {
            rank[i] = rank[i+1]
        }

        for x.Level[i].Forward != nil &&
            (x.Level[i].Forward.Score < score ||
                (x.Level[i].Forward.Score == score && x.Level[i].Forward.Key < key)) {
            rank[i] += x.Level[i].Span
            x = x.Level[i].Forward
        }
        update[i] = x
    }
    Level := randomLevel()
    if Level > this.Level {
        for i := this.Level; i < Level; i++ {
            rank[i] = 0
            update[i] = this.Header
            update[i].Level[i].Span = this.Length
        }
        this.Level = Level
    }
    x = createNode(Level, score, key, value)
    for i := 0; i < Level; i++ {
        x.Level[i].Forward = update[i].Level[i].Forward
        update[i].Level[i].Forward = x

        x.Level[i].Span = update[i].Level[i].Span - (rank[0] - rank[i])
        update[i].Level[i].Span = (rank[0] - rank[i]) + 1
    }
    for i := Level; i < this.Level; i++ {
        update[i].Level[i].Span++
    }
    if update[0] == this.Header {
        x.Level[0].Backward = nil
    } else {
        x.Level[0].Backward = update[0]
    }
    if x.Level[0].Forward != nil {
        x.Level[0].Forward.Level[0].Backward = x
    } else {
        this.Tail = x
    }
    this.Length++
    return x
}

func (this *SortedSet[K, SCORE, V]) deleteNode(x *SortedSetNode[K, SCORE, V], update [SKIPLIST_MAXLEVEL]*SortedSetNode[K, SCORE, V]) {
    for i := 0; i < this.Level; i++ {
        if update[i].Level[i].Forward == x {
            update[i].Level[i].Span += x.Level[i].Span - 1
            update[i].Level[i].Forward = x.Level[i].Forward
        } else {
            update[i].Level[i].Span -= 1
        }
    }
    if x.Level[0].Forward != nil {
        x.Level[0].Forward.Level[0].Backward = x.Level[0].Backward
    } else {
        this.Tail = x.Level[0].Backward
    }
    for this.Level > 1 && this.Header.Level[this.Level-1].Forward == nil {
        this.Level--
    }
    this.Length--
    delete(this.Dict, x.Key)
}

func (this *SortedSet[K, SCORE, V]) delete(score SCORE, key K) bool {
    var update [SKIPLIST_MAXLEVEL]*SortedSetNode[K, SCORE, V]
    x := this.Header
    for i := this.Level - 1; i >= 0; i-- {
        for x.Level[i].Forward != nil &&
            (x.Level[i].Forward.Score < score ||
                (x.Level[i].Forward.Score == score && x.Level[i].Forward.Key < key)) {
            x = x.Level[i].Forward
        }
        update[i] = x
    }
    x = x.Level[0].Forward
    if x != nil && score == x.Score && x.Key == key {
        this.deleteNode(x, update)
        return true
    }
    return false
}

func (this *SortedSet[K, SCORE, V]) AddOrUpdate(key K, score SCORE, value V) bool {
    this.Mu.Lock()
    defer this.Mu.Unlock()
    var newNode *SortedSetNode[K, SCORE, V] = nil
    found := this.Dict[key]
    if found != nil {
        if found.Score == score {
            found.Value = value
        } else {
            this.delete(found.Score, found.Key)
            newNode = this.insertNode(score, key, value)
        }
    } else {
        newNode = this.insertNode(score, key, value)
    }
    if newNode != nil {
        this.Dict[key] = newNode
    }
    return found == nil
}

func (this *SortedSet[K, SCORE, V]) Remove(key K) *SortedSetNode[K, SCORE, V] {
    this.Mu.Lock()
    defer this.Mu.Unlock()
    found := this.Dict[key]
    if found != nil {
        this.delete(found.Score, found.Key)
        return found
    }
    return nil
}
func (this *SortedSet[K, SCORE, V]) GetRangeByRank(start int, end int, remove bool) []*SortedSetNode[K, SCORE, V] {
    this.Mu.Lock()
    defer this.Mu.Unlock()
    start, end, reverse := this.sanitizeIndexes(start, end)
    var nodes []*SortedSetNode[K, SCORE, V]
    traversed, x, update := this.findNodeByRank(start, remove)
    traversed++
    x = x.Level[0].Forward
    for x != nil && traversed <= end {
        next := x.Level[0].Forward
        nodes = append(nodes, x)
        if remove {
            this.deleteNode(x, update)
        }
        traversed++
        x = next
    }
    if reverse {
        for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
            nodes[i], nodes[j] = nodes[j], nodes[i]
        }
    }
    return nodes
}

func (this *SortedSet[K, SCORE, V]) FindRank(key K, fromFront bool) (int, bool) {
    this.Mu.Lock()
    defer this.Mu.Unlock()

    node, exists := this.Dict[key]
    if !exists {
        return 0, false 
    }
    rank := 0
    x := this.Header
    if fromFront {
        for i := this.Level - 1; i >= 0; i-- {
            for x.Level[i].Forward != nil &&
                (x.Level[i].Forward.Score < node.Score ||
                    (x.Level[i].Forward.Score == node.Score && x.Level[i].Forward.Key < node.Key)) {
                rank += int(x.Level[i].Span)
                x = x.Level[i].Forward
            }
        }
        return rank + 1, true 
    } else {
        totalElements := this.Length
        for i := this.Level - 1; i >= 0; i-- {
            for x.Level[i].Forward != nil &&
                (x.Level[i].Forward.Score < node.Score ||
                    (x.Level[i].Forward.Score == node.Score && x.Level[i].Forward.Key < node.Key)) {
                rank += int(x.Level[i].Span)
                x = x.Level[i].Forward
            }
        }
        return int(totalElements) - rank, true
    }
}

func (this *SortedSet[K, SCORE, V]) sanitizeIndexes(start int, end int) (int, int, bool) {
    if start < 0 {
        start = int(this.Length) + start + 1
    }
    if end < 0 {
        end = int(this.Length) + end + 1
    }
    if start <= 0 {
        start = 1
    }
    if end <= 0 {
        end = 1
    }
    reverse := start > end
    if reverse {
        start, end = end, start
    }
    return start, end, reverse
}

func (this *SortedSet[K, SCORE, V]) findNodeByRank(start int, remove bool) (traversed int, x *SortedSetNode[K, SCORE, V], update [SKIPLIST_MAXLEVEL]*SortedSetNode[K, SCORE, V]) {
    x = this.Header
    for i := this.Level - 1; i >= 0; i-- {
        for x.Level[i].Forward != nil && traversed+int(x.Level[i].Span) < start {
            traversed += int(x.Level[i].Span)
            x = x.Level[i].Forward
        }
        if remove {
            update[i] = x
        } else {
            if traversed+1 == start {
                break
            }
        }
    }
    return
}