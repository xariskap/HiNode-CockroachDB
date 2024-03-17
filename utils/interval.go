package utils

import (
	"fmt"
	"strconv"
	"strings"
)

type Interval struct {
	value string
	Start string
	end   string
}



func NewInterval(value string, start string, end string) Interval {
	if strings.EqualFold("infinity", end) {
		end = "2147483647" // int32 max value
	}
	return (Interval{value, start, end})
}

func (i Interval) String() string {
	return (fmt.Sprintf("Interval{value: %v, start: %v, end: %v}", i.value, i.Start, i.end))
}

func (i Interval) CompareTo(interv Interval) int {
	istart, _ := strconv.Atoi(i.Start)
	iend, _ := strconv.Atoi(i.end)
	intervstart, _ := strconv.Atoi(interv.Start)
	intervend, _ := strconv.Atoi(interv.end)

	if iend <= intervstart {
		return -1
	} else if intervend <= istart {
		return 1
	}
	return 0
}

func (i Interval) Stab(timestamp string) bool {
	start, _ := strconv.Atoi(i.Start)
	end, _ := strconv.Atoi(i.end)
	ts, _ := strconv.Atoi(timestamp)

	return ts >= start && ts < end
}