package bulong_filter

import "testing"

func TestNewBloomFilter(t *testing.T) {
	filter := NewBloomFilter(0.6, "filterBloomTest", "filterBloomTest", 1000, 9)
	err := filter.AddElem([]byte("test"))
	ok := filter.BoolElem([]byte("test"))
	ok1 := filter.BoolElem([]byte("test1"))
	ok2 := filter.BoolElem([]byte("test2"))
	ok3 := filter.BoolElem([]byte("test3"))
	t.Log(ok, ok1, ok2, ok3, err)
}