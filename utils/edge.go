package utils

import (
	"fmt"
	"strconv"
)

type Edge struct {
	label    string
	weight   string
	otherEnd string
	start    string
	end      string
}

// returns a new Edge struct
func NewEdge(l string, w string, oe string, s string, e string) Edge {
	return Edge{l, w, oe, s, e}
}

func (e Edge) String() string {
	return fmt.Sprintf("label: %v\nweight: %v\notherEnd: %v\nstart: %v\nend: %v", e.label, e.weight, e.otherEnd, e.start, e.end)
}

func (e Edge) EistsInterval(f string, l string) bool {
	first, _ := strconv.ParseInt(f, 0, 64)
	last, _ := strconv.ParseInt(f, 0, 64)
	start, _ := strconv.ParseInt(e.start, 0, 64)
	end, _ := strconv.ParseInt(e.end, 0, 64)

	return (first >= start && first < end || last >= start && last < end)
}
