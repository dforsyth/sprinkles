package ds

import (
	"fmt"
	"sync"
)

// A locked map of interfaces{}
type InterfaceMap struct {
	_map map[string]interface{}
	lk   sync.RWMutex
}

// Create a new InterfaceMap
func NewInterfaceMap(initial map[string]interface{}) *InterfaceMap {
	m := &InterfaceMap{
		_map: make(map[string]interface{}),
	}
	for key, val := range initial {
		m._map[key] = val
	}
	return m
}

// Get a value from the map
func (m *InterfaceMap) Get(key string) interface{} {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return m._map[key]
}

// Check whether a key exists in the map
func (m *InterfaceMap) Contains(key string) bool {
	m.lk.RLock()
	defer m.lk.RUnlock()
	_, ok := m._map[key]
	return ok
}

// Remove a key from the map
func (m *InterfaceMap) Delete(key string) interface{} {
	m.lk.Lock()
	defer m.lk.Unlock()
	v := m._map[key]
	delete(m._map, key)
	return v
}

// Put a key, value into the map
func (m *InterfaceMap) Put(key string, value interface{}) interface{} {
	m.lk.Lock()
	defer m.lk.Unlock()
	old := m._map[key]
	m._map[key] = value
	return old
}

// Take an extended lock over the map
func (m *InterfaceMap) RangeLock() map[string]interface{} {
	m.lk.RLock()
	return m._map
}

// Release extended lock
func (m *InterfaceMap) RangeUnlock() {
	m.lk.RUnlock()
}

// Copy the map into a normal map
func (m *InterfaceMap) GetCopy() map[string]interface{} {
	m.lk.RLock()
	defer m.lk.RUnlock()
	_m := make(map[string]interface{})
	for k, v := range m._map {
		_m[k] = v
	}
	return _m
}

// Clear th map
func (m *InterfaceMap) Clear() {
	m.lk.Lock()
	defer m.lk.Unlock()
	m._map = make(map[string]interface{})
}

// Get the size of the map
func (m *InterfaceMap) Len() int {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return len(m._map)
}

// Dump the map as a string
func (m *InterfaceMap) Dump() string {
	m.lk.RLock()
	defer m.lk.RUnlock()
	rval := ""
	for k, v := range m._map {
		rval += fmt.Sprintf("(key: %s, value: %v)\n", k, v)
	}
	return rval
}

// List all keys in the map
func (m *InterfaceMap) Keys() (keys []string) {
	_m := m.RangeLock()
	defer m.RangeUnlock()
	for k := range _m {
		keys = append(keys, k)
	}
	return
}

// a locked map of strings
type StringMap struct {
	_m map[string]string
	lk sync.RWMutex
}
