package utils

import (
	"fmt"
	"sort"
	"strconv"
)

type Dianode struct {
	vid            string
	start          string
	end            string
	attributes     map[string][]Interval
	outgoing_edges map[string][]Edge
	incoming_edges map[string][]Edge
}

func NewDianode(id string, s string, e string, attr map[string][]Interval, out_edges map[string][]Edge, in_edges map[string][]Edge) Dianode {
	return Dianode{id, s, e, attr, out_edges, in_edges}
}


func (d Dianode) convertToVertex(timestamp string) Vertex {

	ver := NewVertex()
	ver.SetVid(d.vid)
	ver.SetTimestamp(timestamp)
	ts, _ := strconv.Atoi(timestamp)

	// copy attributes
	for attrName, intervals := range(d.attributes){
		value := intervals[d.Search(attrName, timestamp)].value
		ver.SetValue(attrName, value)
	}

	//copy outgoing edges
	for targetID := range(d.outgoing_edges){
		for _, edge := range(d.outgoing_edges[targetID]){
			estart, _ := strconv.Atoi(edge.start)
			eend, _ := strconv.Atoi(edge.end)
			if ts >= estart && ts < eend{
				ver.AddOutgoingEdge(targetID, edge)
				break
			}
		}
	}
	// copy incoming edges
	for targetID := range d.incoming_edges {
		for _, edge := range(d.incoming_edges[targetID]){
			estart, _ := strconv.Atoi(edge.start)
			eend, _ := strconv.Atoi(edge.end)
			if ts >= estart && ts < eend{
				ver.AddOutgoingEdge(targetID, edge)
				break
			}
		}
	}

	return ver
}

func (d Dianode) GetVid() string {
	return d.vid
}

func (d *Dianode) SetVid(vid string) {
	d.vid = vid
}

func (d Dianode) GetStart() string {
	return d.start
}

func (d *Dianode) SetStart(start string) {
	d.start = start
}

func (d Dianode) GetEnd() string {
	return d.end
}

func (d *Dianode) SetEnd(end string) {
	d.end = end
}

func (d Dianode) GetAttributes() map[string][]Interval {
	return d.attributes
}

func (d *Dianode) SetAttributes(attributes map[string][]Interval) {
	d.attributes = attributes
}

func (d Dianode) GetOutgoingEdges() map[string][]Edge {
	return d.outgoing_edges
}

func (d *Dianode) SetOutgoingEdges(outgoing_edges map[string][]Edge) {
	d.outgoing_edges = outgoing_edges
}

func (d Dianode) GetIncomingEdges() map[string][]Edge {
	return d.incoming_edges
}

func (d *Dianode) SetIncomingEdges(incoming_edges map[string][]Edge) {
	d.incoming_edges = incoming_edges
}

// TOCHECK insert interval on specific index
func (d *Dianode) InsertAttribute(attrName string, newIntervalValues []Interval) {
	attrList, ok := d.attributes[attrName]
	if ok {
		d.attributes[attrName] = append(attrList, newIntervalValues...)
	} else {
		d.attributes[attrName] = newIntervalValues
	}
}

// TOCHECK intervals should be sorted
func (d *Dianode) Search(attrName, timestamp string) int {
	attrList, ok := d.attributes[attrName]
	if !ok {
		return -1
	}

	index := sort.Search(len(attrList), func(i int) bool {
		return attrList[i].start >= timestamp
	})

	if attrList[index].Stab(timestamp) {
		return index
	}

	if index-1 > 0 && attrList[index-1].Stab(timestamp) {
		return index - 1
	}

	if index+1 < len(attrList)-1 && attrList[index+1].Stab(timestamp) {
		return index + 1
	}

	return -1
}

func (d *Dianode) String() string {
	return fmt.Sprintf("Dianode{vid: %v, start: %v, end: %v, attributes: %v, outgoing_edges: %v, incoming_edges: %v}", d.vid, d.start, d.end, d.attributes, d.outgoing_edges, d.incoming_edges)
}

// NOT FINISHED
func (d *Dianode) KeepValuesInInterval(first, last string){
	startInt, _ := strconv.Atoi(d.start)
	firstInt, _ := strconv.Atoi(first)
	endInt, _ := strconv.Atoi(d.end)
	lastInt, _ := strconv.Atoi(last)

	d.start = func() string {
		if startInt < firstInt {
			return first
		}
		return d.start
	}()

	d.start = func() string {
		if endInt > lastInt {
			return last
		}
		return d.end
	}()
}

func (d *Dianode) Merge(dn Dianode){
	d.end = dn.end

	for attrName, intervals := range dn.attributes {
		if existingIntervals, ok := d.attributes[attrName]; ok {
			d.attributes[attrName] = append(existingIntervals, intervals...)
		} else {
			d.attributes[attrName] = intervals
		}
	}

	for sourceID, edges := range dn.incoming_edges {
		if existingEdges, ok := d.incoming_edges[sourceID]; ok {
			d.incoming_edges[sourceID] = append(existingEdges, edges...)
		} else {
			d.incoming_edges[sourceID] = edges
		}
	}


}