package utils

import "fmt"

type Snapshot struct {
	sid   string
	value float64
}

func (s *Snapshot) NewSnapshot(sid string, value float64) {
	s.sid = sid
	s.value = value
}

func (s Snapshot) CompareTo(sp Snapshot) int {
	if s.value < sp.value {
		return 1
	} else if s.value > sp.value {
		return -1
	}
	return 0
}

func (s Snapshot) String() string {
	return (fmt.Sprintf("Snapsot{id: %v, value: %v}", s.sid, s.value))
}
