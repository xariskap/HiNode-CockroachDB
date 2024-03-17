package utils

import (
	"encoding/json"
	"log"
)

func AttributeToJSON(vID, label, attr string, interval Interval) []byte {
	newAttribute := map[string]interface{}{
		label:   attr,
		"start": interval.Start,
	}

	JSONattribute, err := json.Marshal(newAttribute)
	if err != nil {
		log.Fatal(err)
	}

	return JSONattribute
}
