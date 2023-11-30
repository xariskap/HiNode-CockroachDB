package utils

import (
	"fmt"
	"strconv"
	"strings"
)

type Interval struct {
	value string
	start string
	end   string
}

func NewInterval(value string, start string, end string) Interval {
	if strings.EqualFold("infinity", value) {
		value = "2147483647" // int32 max value
	}
	return (Interval{value, start, end})
}

func (i Interval) String() string {
	return (fmt.Sprintf("Interval{value: %v, start: %v, end: %v}", i.value, i.start, i.end))
}

func (i Interval) CompareTo(interv Interval) int {
	istart, _ := strconv.Atoi(i.start)
	iend, _ := strconv.Atoi(i.end)
	intervstart, _ := strconv.Atoi(interv.start)
	intervend, _ := strconv.Atoi(interv.end)

	if iend <= intervstart {
		return -1
	} else if intervend <= istart {
		return 1
	}
	return 0
}

// TODO Stab()
