package utils

import (
	"encoding/json"
	"log"
	"strconv"
)

func AttributeToJSON(attrlabel, attr string, interval Interval) []byte {
	newAttribute := map[string]interface{}{
		attrlabel: attr,
		"start":   interval.Start,
	}

	JSONattribute, err := json.Marshal(newAttribute)
	if err != nil {
		log.Fatal(err)
	}

	return JSONattribute
}

func StrToInt(str string) int {
	num, err := strconv.Atoi(str)
	if err != nil {
		log.Fatal(err)
	}

	return num
}
