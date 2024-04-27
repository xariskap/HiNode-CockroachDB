package gremlin

import (
	"regexp"
	"strings"
)

var data []string

func GremlinParse(s string) []string {
	data = []string{}
	re := regexp.MustCompile(`'([^']*)'`)
	matches := re.FindAllStringSubmatch(s, -1)
	if strings.HasPrefix(s, "g.insertE") {
		data = append(data, matches[0][0], matches[1][0], matches[2][0], matches[5][0], matches[3][0], matches[4][0])

	} else if strings.HasPrefix(s, "g.addV") {
		data = append(data, matches[1][0], matches[0][0], matches[2][0], matches[3][0])

	} else if strings.HasPrefix(s, "g.deleteV") {
		data = append(data, matches[0][0], matches[1][0])

	} else if strings.HasPrefix(s, "g.deleteE") {
		data = append(data, matches[0][0], matches[1][0], matches[2][0])
	}
	return data
}
