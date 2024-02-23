package utils

import "fmt"

type Vertex struct {
	vid            string
	timestamp      string
	attributes     map[string]string
	outgoing_edges map[string]Edge
	incoming_edges map[string]Edge
}

func NewVertex() Vertex {
	attr := make(map[string]string)
	out_edges := make(map[string]Edge)
	inc_edges := make(map[string]Edge)
	return Vertex{"-1", "0", attr, out_edges, inc_edges}
}

func (v Vertex) GetOutgoingEdges() map[string]Edge {
	return v.outgoing_edges
}

func (v Vertex) GetIncomingEdges() map[string]Edge {
	return v.incoming_edges
}

func (v *Vertex) SetOutgoingEdges(out_edges map[string]Edge) {
	for key, value := range out_edges {
		v.outgoing_edges[key] = value
	}
}

func (v *Vertex) SetIncomingEdges(inc_edges map[string]Edge) {
	for key, value := range inc_edges {
		v.outgoing_edges[key] = value
	}
}

func (v *Vertex) AddOutgoingEdge(vid string, edge Edge) {
	v.outgoing_edges[vid] = edge
}

func (v *Vertex) AddIncomingEdge(vid string, edge Edge) {
	v.incoming_edges[vid] = edge
}

func (v Vertex) GetVid() string {
	return (v.vid)
}

func (v *Vertex) SetVid(vid string) {
	v.vid = vid
}

func (v Vertex) GetTimestamp() string {
	return (v.timestamp)
}

func (v *Vertex) SetTimestamp(timestamp string) {
	v.timestamp = timestamp
}

func (v Vertex) GetAttributes() map[string]string {
	return (v.attributes)
}

func (v *Vertex) SetAttributes(attributes map[string]string) {
	v.attributes = attributes
}

func (v Vertex) GetNeighbors() []string {
	var neighbors []string
	for key := range v.outgoing_edges {
		neighbors = append(neighbors, key)
	}
	return neighbors
}

func (v *Vertex) SetValue(attrName string, attrValue string) {
	v.attributes[attrName] = attrValue
}

func (v Vertex) String() string {
	return fmt.Sprintf("Vertex{vid: %v, timestamp: %v, attributes: %v, noutgoing_edges: %v, incoming_edges: %v}", v.vid, v.timestamp, v.attributes, v.outgoing_edges, v.incoming_edges)
}
