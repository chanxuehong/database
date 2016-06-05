package lru

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNotFound  = errors.New("item not found")
	ErrNotStored = errors.New("item not stored")
)

type (
	Key   string // can changes to explicit type you want
	Value Stmt   // can changes to explicit type you want
)

type payload struct {
	key   Key
	value Value
}

//                             front                           back
//                           +-------+       +-------+       +-------+
//                           |       |------>|       |------>|       |
// lruList:*list.List        |payload|       |payload|       |payload|
//                           |       |<------|       |<------|       |
//                           +-------+       +-------+       +-------+
//                               ^               ^               ^
// itemMap:                      |               |               |
// map[Key]*list.Element         |               |               |
//     +-----+---------------+   |               |               |
//     | Key | *list.Element +---+               |               |
//     +-----+---------------+                   |               |
//     | Key | *list.Element +-------------------+               |
//     +-----+---------------+                                   |
//     | Key | *list.Element +-----------------------------------+
//     +-----+---------------+
//
// Principle:
//   1. len(itemMap) == lruList.Len();
//   2. for Element of lruList, we get
//      itemMap[Element.Value.(*payload).key] == Element;
//   3. in the list lruList, the younger element is always in front of the older elements;
//

// Cache is a thread-safe fixed size LRU cache.
type Cache struct {
	mutex   sync.Mutex
	size    int
	lruList *list.List
	itemMap map[Key]*list.Element
}

// New creates an LRU cache of the given size. if size<=0, will panic.
func New(size int) *Cache {
	if size <= 0 {
		panic(fmt.Sprintf("size must be > 0 and now == %d", size))
	}
	return &Cache{
		size:    size,
		lruList: list.New(),
		itemMap: make(map[Key]*list.Element, size),
	}
}

// Size returns the size of cache.
func (cache *Cache) Size() (size int) {
	cache.mutex.Lock()
	size = cache.size
	cache.mutex.Unlock()
	return
}

// SetSize sets a new size for the cache. if size <=0, we do nothing.
func (cache *Cache) SetSize(size int) {
	if size <= 0 {
		return
	}

	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	if n := cache.lruList.Len() - size; n > 0 {
		for i, e := n, cache.lruList.Back(); i > 0; i, e = i-1, cache.lruList.Back() {
			cache.remove(e)
		}
	}

	cache.size = size
	return
}

// Len returns the number of items in the cache.
func (cache *Cache) Len() (n int) {
	cache.mutex.Lock()
	n = cache.lruList.Len()
	cache.mutex.Unlock()
	return
}

// Purge is used to completely clear the cache
func (cache *Cache) Purge() {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	cache.lruList = list.New()
	cache.itemMap = make(map[Key]*list.Element, cache.size)
}

// add adds key-value to cache.
// Please ensure that there is no item with the same key in cache
func (cache *Cache) add(key Key, value Value) (err error) {
	if cache.lruList.Len() >= cache.size {
		e := cache.lruList.Back() // e != nil, for cache.size > 0
		payload := e.Value.(*payload)

		delete(cache.itemMap, payload.key)

		payload.key = key
		payload.value = value

		cache.itemMap[key] = e
		cache.lruList.MoveToFront(e)
		return
	} else {
		cache.itemMap[key] = cache.lruList.PushFront(&payload{
			key:   key,
			value: value,
		})
		return
	}
}

// remove removes the Element e from cache.lruList.
// Please ensure that e != nil and e is an element of list lruList.
func (cache *Cache) remove(e *list.Element) {
	delete(cache.itemMap, e.Value.(*payload).key)
	cache.lruList.Remove(e)
}

// Add adds key-value to cache.
// if there already exists a item with the same key, it returns ErrNotStored.
//
//  NOTE: the comparison operators == and != must be fully defined for
//        operands of the key type.
func (cache *Cache) Add(key Key, value Value) (err error) {
	cache.mutex.Lock()
	if _, hit := cache.itemMap[key]; hit {
		err = ErrNotStored
		cache.mutex.Unlock() // Unlock
		return
	} else {
		err = cache.add(key, value)
		cache.mutex.Unlock() // Unlock
		return
	}
}

// Set sets key-value to cache, unconditional
//
//  NOTE: the comparison operators == and != must be fully defined for
//        operands of the key type.
func (cache *Cache) Set(key Key, value Value) (err error) {
	cache.mutex.Lock()
	if e, hit := cache.itemMap[key]; hit {
		payload := e.Value.(*payload)
		// payload.Key = key
		payload.value = value
		cache.lruList.MoveToFront(e)
		cache.mutex.Unlock() // Unlock
		return
	} else {
		err = cache.add(key, value)
		cache.mutex.Unlock() // Unlock
		return
	}
}

// Get looks up a key's value from the cache.
// if there is no such item with the key it returns ErrNotFound.
//
//  NOTE: the comparison operators == and != must be fully defined for
//        operands of the key type.
func (cache *Cache) Get(key Key) (value Value, err error) {
	cache.mutex.Lock()
	if e, hit := cache.itemMap[key]; hit {
		cache.lruList.MoveToFront(e)
		value = e.Value.(*payload).value
		cache.mutex.Unlock() // Unlock
		return
	} else {
		err = ErrNotFound
		cache.mutex.Unlock() // Unlock
		return
	}
}

// Remove removes the provided key from the cache.
// if there is no such item with the key it returns ErrNotFound,
// normally you can ignore this error.
//
//  NOTE: the comparison operators == and != must be fully defined for
//        operands of the key type.
func (cache *Cache) Remove(key Key) (err error) {
	cache.mutex.Lock()
	if e, hit := cache.itemMap[key]; hit {
		cache.remove(e)
		cache.mutex.Unlock() // Unlock
		return
	} else {
		err = ErrNotFound
		cache.mutex.Unlock() // Unlock
		return
	}
}
