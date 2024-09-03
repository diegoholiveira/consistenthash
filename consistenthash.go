package cache

import (
	"crypto/sha1"
	"sort"
	"strconv"
	"sync"
)

type ConsistentHash struct {
	replicas int            // Number of virtual nodes per physical node
	keys     []int          // Sorted list of hash keys (virtual nodes)
	hashMap  map[int]string // Mapping from virtual node hash to physical node
	mutex    sync.RWMutex   // To handle concurrent access to ConsistentHash
}

func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
}

func hash(data []byte) int {
	hash := sha1.Sum(data)

	// extracts the first 4 bytes from a hash,
	// shifts them to the correct positions,
	// and combines them into a single 32-bit integer
	return int(uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3]))
}

func (m *ConsistentHash) AddNode(key string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for i := 0; i < m.replicas; i++ {
		hash := hash([]byte(strconv.Itoa(i) + key))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = key
	}

	sort.Ints(m.keys)
}

func (m *ConsistentHash) RemoveNode(key string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for i := 0; i < m.replicas; i++ {
		hash := hash([]byte(strconv.Itoa(i) + key))
		index := sort.SearchInts(m.keys, hash)
		if index < len(m.keys) && m.keys[index] == hash {
			m.keys = append(m.keys[:index], m.keys[index+1:]...)
			delete(m.hashMap, hash)
		}
	}
}

func (m *ConsistentHash) FindNodeByCacheKey(key string) string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	hash := hash([]byte(key))

	// Binary search for the first key >= hash
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// Wrap around if needed
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
