package gremlin

import (
	"regexp"
	"strings"
)

type Vertex struct {
	Label      string
	ID         string
	Start      string
	End        string
	Attributes map[string]string
}

type Edge struct {
	Label    string
	SourceID string
	TargetID string
	Start    string
	End      string
	Weight   string
}

func GremlinParse(s string) ([]string, map[string]string) {
	data := make([]string, 0)
	var vattr map[string]string
	if strings.HasPrefix(s, "g.insertE") {

		e := parseInsertE(s)
		data = append(data, e.Label, e.SourceID, e.TargetID, e.Weight, e.Start, e.End)
		return data, vattr
	} else if strings.HasPrefix(s, "g.addV") {

		v := parseAddV(s)
		vattr = v.Attributes
		data = append(data, v.ID, v.Label, v.Start, v.End)
		return data, vattr
	} else if strings.HasPrefix(s, "g.deleteV") {

		v := parseDeleteV(s)
		data = append(data, v.ID, v.End)
		return data, vattr
	} else if strings.HasPrefix(s, "g.deleteE") {

		e := parseDeleteE(s)
		data = append(data, e.SourceID, e.TargetID, e.End)
		return data, vattr
	}
	return data, vattr
}

func parseAddV(input string) Vertex {
	v := Vertex{}

	labelRegex := regexp.MustCompile(`g\.addV\('([^']+)'\)`)
	labelMatch := labelRegex.FindStringSubmatch(input)
	if len(labelMatch) > 0 {
		v.Label = labelMatch[1]
	}

	idRegex := regexp.MustCompile(`\.vid\('([^']+)'\)`)
	idMatch := idRegex.FindStringSubmatch(input)
	if len(idMatch) > 0 {
		v.ID = idMatch[1]
	}

	lifetimeRegex := regexp.MustCompile(`\.lifetime\('([^']+)', '([^']+)'\)`)
	lifetimeMatch := lifetimeRegex.FindStringSubmatch(input)
	if len(lifetimeMatch) > 0 {
		v.Start = lifetimeMatch[1]
		v.End = lifetimeMatch[2]
	}

	attributesRegex := regexp.MustCompile(`\.addA\('([^']+)', '([^']+)'\)`)
	attributesMatches := attributesRegex.FindAllStringSubmatch(input, -1)
	if len(attributesMatches) > 0 {
		v.Attributes = make(map[string]string)
		for _, match := range attributesMatches {
			v.Attributes[match[1]] = match[2]
		}
	}

	return v
}

func parseInsertE(input string) Edge {
	e := Edge{}

	labelRegex := regexp.MustCompile(`g\.insertE\('([^']+)', '([^']+)', '([^']+)'\)`)
	labelMatch := labelRegex.FindStringSubmatch(input)
	if len(labelMatch) > 0 {
		e.Label = labelMatch[1]
		e.SourceID = labelMatch[2]
		e.TargetID = labelMatch[3]
	}

	lifetimeRegex := regexp.MustCompile(`\.lifetime\('([^']+)', '([^']+)'\)`)
	lifetimeMatch := lifetimeRegex.FindStringSubmatch(input)
	if len(lifetimeMatch) > 0 {
		e.Start = lifetimeMatch[1]
		e.End = lifetimeMatch[2]
	}

	weightRegex := regexp.MustCompile(`\.weight\('([^']+)'\)`)
	weightMatch := weightRegex.FindStringSubmatch(input)
	if len(weightMatch) > 0 {
		e.Weight = weightMatch[1]
	}

	return e
}

func parseDeleteV(input string) Vertex {
	v := Vertex{}
	labelRegex := regexp.MustCompile(`g\.deleteV\('([^']+)', '([^']+)'\)`)
	labelMatch := labelRegex.FindStringSubmatch(input)
	if len(labelMatch) > 0 {
		v.ID = labelMatch[1]
		v.End = labelMatch[2]
	}

	return v
}

func parseDeleteE(input string) Edge {
	e := Edge{}
	labelRegex := regexp.MustCompile(`g\.deleteE\('([^']+)', '([^']+)', '([^']+)'\)`)
	labelMatch := labelRegex.FindStringSubmatch(input)
	if len(labelMatch) > 0 {
		e.SourceID = labelMatch[1]
		e.TargetID = labelMatch[2]
		e.End = labelMatch[3]
	}

	return e
}
