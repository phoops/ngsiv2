package model_test

import (
	"encoding/json"
	"testing"

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
}

func TestEntityMarshal(t *testing.T) {
	office := model.NewEntity("openspace", "Office")
	office.SetAttributeAsString("name", "Phoops HQ")
	office.SetAttributeAsFloat("temperature", 34.2) // it's July and fan coils aren't very good

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
}
