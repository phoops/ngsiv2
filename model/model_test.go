package model_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/phoops/ngsiv2/model"
)

func TestEntityUnmarshal(t *testing.T) {
	roomEntityJson := `
	{
		"id": "Room1",
		"pressure": {
			"metadata": {},
			"type": "Integer",
			"value": 720
		},
		"temperature": {
			"metadata": {},
			"type": "Float",
			"value": 23
		},
		"location": {
			"metadata": {},
			"type": "geo:point",
			"value": "43.8030095, 11.2385831"
		},
		"lastUpdate": {
			"metadata": {},
			"type": "DateTime",
			"value": "2018-07-24T07:21:24.238Z"
		},
		"type": "Room"
	}
`
	roomEntity := &model.Entity{}
	if err := json.Unmarshal([]byte(roomEntityJson), roomEntity); err != nil {
		t.Fatalf("Error unmarshaling entity: %v", err)
	}
	if roomEntity.Id != "Room1" {
		t.Fatalf("Expected '%s' for Id; got '%s'", "Room1", roomEntity.Id)
	}
	if roomEntity.Type != "Room" {
		t.Fatalf("Expected '%s' for Type; got '%s'", "Room", roomEntity.Type)
	}

	if _, err := roomEntity.GetAttribute("humidity"); err == nil {
		t.Fatal("Expected a failure on missing attribute 'humidity'")
	}

	pressureAttr, err := roomEntity.GetAttribute("pressure")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if pressureAttr.Type != model.IntegerType {
		t.Fatalf("Expected '%s' for pressure attribute type; got '%s'", "Integer", pressureAttr.Type)
	}
	if _, err := pressureAttr.GetAsFloat(); err == nil {
		t.Fatal("Expected a failure on non float value 'pressure'")
	}
	pressureVal, err := pressureAttr.GetAsInteger()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if pressureVal != 720 {
		t.Fatalf("Expected '%d' for pressure value; got '%d'", 720, pressureVal)
	}

	temperatureAttr, err := roomEntity.GetAttribute("temperature")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if temperatureAttr.Type != model.FloatType {
		t.Fatalf("Expected '%s' for temperature attribute type, got '%s'", "Float", temperatureAttr.Type)
	}
	if _, err := temperatureAttr.GetAsInteger(); err == nil {
		t.Fatal("Expected a failure on non integer value 'temperature'")
	}
	temperatureVal, err := temperatureAttr.GetAsFloat()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if temperatureVal != 23.0 {
		t.Fatalf("Expected '%v' for temperature value, got '%v'", 23.0, temperatureVal)
	}

	if locationAttr, err := roomEntity.GetAttribute("location"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if locationAttr.Type != model.GeoPointType {
			t.Fatalf("Expected '%s' for location attribute type, got '%s'", "GeoPoint", locationAttr.Type)
		}
		if _, err := locationAttr.GetAsInteger(); err == nil {
			t.Fatal("Expected a failure on non integer value 'location'")
		}
		if locationVal, err := locationAttr.GetAsGeoPoint(); err != nil {
			t.Fatalf("Unexpected error: '%v'", err)
		} else {
			if locationVal.Latitude != 43.8030095 || locationVal.Longitude != 11.2385831 {
				t.Fatalf("Unexpected value reading lastUpdate")
			}
		}
	}

	if lastUpdateAttr, err := roomEntity.GetAttribute("lastUpdate"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if lastUpdateAttr.Type != model.DateTimeType {
			t.Fatalf("Expected '%s' for lastUpdate attribute type, got '%s'", "DateTime", lastUpdateAttr.Type)
		}
		if _, err := lastUpdateAttr.GetAsInteger(); err == nil {
			t.Fatal("Expected a failure on non integer value 'lastUpdate'")
		}
		if lastUpdateVal, err := lastUpdateAttr.GetAsDateTime(); err != nil {
			t.Fatalf("Unexpected error: '%v'", err)
		} else {
			if lastUpdateVal.Day() != 24 || lastUpdateVal.Minute() != 21 {
				t.Fatalf("Unexpected value reading lastUpdate")
			}
		}
	}

}

func TestEntityMarshal(t *testing.T) {
	office := model.NewEntity("openspace", "Office")
	office.SetAttributeAsString("name", "Phoops HQ")
	office.SetAttributeAsFloat("temperature", 34.2) // it's July and fan coils aren't very good
	timeNow := time.Now()
	office.SetAttributeAsDateTime("lastUpdate", timeNow)
	gp := model.NewGeoPoint(4.1, 2.3)
	office.SetAttributeAsGeoPoint("location", gp)

	bytes, err := json.Marshal(office)
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	unmarshaled := &model.Entity{}
	if err = json.Unmarshal(bytes, unmarshaled); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	if unmarshaled.Id != "openspace" {
		t.Fatalf("Expected '%s' for id, got '%s'", "openspace", unmarshaled.Id)
	}

	if unmarshaled.Type != "Office" {
		t.Fatalf("Expected '%s' for type, got '%s'", "Office", unmarshaled.Type)
	}

	nameAttr, err := unmarshaled.GetAttribute("name")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	nameVal, err := nameAttr.GetAsString()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if nameVal != "Phoops HQ" {
		t.Fatalf("Expected '%s' for name attribute, got '%s'", "Phoops HQ", nameVal)
	}

	temperatureAttr, err := unmarshaled.GetAttribute("temperature")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	temperatureVal, err := temperatureAttr.GetAsFloat()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if temperatureVal != 34.2 {
		t.Fatalf("Expected '%v' for temperature value, got '%v'", 34.2, temperatureVal)
	}

	if lastUpdateAttr, err := unmarshaled.GetAttribute("lastUpdate"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if lastUpdateVal, err := lastUpdateAttr.GetAsDateTime(); err != nil {
			t.Fatalf("Unexpected error: '%v'", err)
		} else {
			if lastUpdateVal.Day() != timeNow.Day() || lastUpdateVal.Minute() != timeNow.Minute() {
				t.Fatalf("Expected '%v' for lastUpdate value, got '%v'", timeNow, lastUpdateVal)
			}
		}
	}

	if locationAttr, err := unmarshaled.GetAttribute("location"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if locationVal, err := locationAttr.GetAsGeoPoint(); err != nil {
			t.Fatalf("Unexpected error: '%v'", err)
		} else {
			if locationVal.Latitude != gp.Latitude || locationVal.Longitude != gp.Longitude {
				t.Fatalf("Expected '%v' for location value, got '%v'", gp, locationVal)
			}
		}
	}
}
