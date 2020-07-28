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
		"dirty": {
			"metadata": {},
			"type": "Boolean",
			"value": false
		},
		"hot": {
			"metadata": {},
			"type": "Boolean",
			"value": true
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
		"roomDimensions": {
			"metadata": {},
			"type": "RoomDimensions",
			"value": {
				"width": 5,
				"height": 5,
				"depth": 5
			}
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
	if paVal, _ := roomEntity.GetAttributeAsInteger("pressure"); paVal != 720 {
		t.Fatalf("Expected '%d' for pressure attribute as integer; got '%d'", 720, paVal)
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
	if taVal, _ := roomEntity.GetAttributeAsFloat("temperature"); taVal != 23 {
		t.Fatalf("Expected '%v' for temperature attribute as float, got '%v'", 23.0, taVal)
	}

	dirtyAttr, err := roomEntity.GetAttribute("dirty")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if dirtyAttr.Type != model.BooleanType {
		t.Fatalf("Expected '%s' for dirty attribute type, got '%s'", "Boolean", dirtyAttr.Type)
	}
	if _, err := dirtyAttr.GetAsString(); err == nil {
		t.Fatal("Expected a failure on non boolean value 'dirty'")
	}
	dirtyVal, err := dirtyAttr.GetAsBoolean()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if dirtyVal {
		t.Fatalf("Expected '%t' for dirty value, got '%t'", false, dirtyVal)
	}
	if dtVal, _ := roomEntity.GetAttributeAsBoolean("dirty"); dtVal {
		t.Fatalf("Expected '%t' for dirty attribute as boolean, got '%t'", false, dtVal)
	}

	hotAttr, err := roomEntity.GetAttribute("hot")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if hotAttr.Type != model.BooleanType {
		t.Fatalf("Expected '%s' for hot attribute type, got '%s'", "Boolean", hotAttr.Type)
	}
	if _, err := hotAttr.GetAsString(); err == nil {
		t.Fatal("Expected a failure on non boolean value 'hot'")
	}
	hotVal, err := hotAttr.GetAsBoolean()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if !hotVal {
		t.Fatalf("Expected '%t' for hot value, got '%t'", true, hotVal)
	}
	if htVal, _ := roomEntity.GetAttributeAsBoolean("hot"); !htVal {
		t.Fatalf("Expected '%t' for hot attribute as boolean, got '%t'", true, htVal)
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
				t.Fatalf("Unexpected value reading location")
			}
		}
		if laVal, _ := roomEntity.GetAttributeAsGeoPoint("location"); laVal.Latitude != 43.8030095 || laVal.Longitude != 11.2385831 {
			t.Fatalf("Unexpected value reading location attribute as geopoint")
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

	if roomDimensions, err := roomEntity.GetAttribute("roomDimensions"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if roomDimensions.Type != model.AttributeType("RoomDimensions") {
			t.Fatalf("Expected '%s' for lastUpdate attribute type, got '%s'", "RoomDimensions", roomDimensions.Type)
		}
	}

}

func TestEntityMarshal(t *testing.T) {
	if _, err := model.NewEntity("invalid id", "Office"); err == nil {
		t.Fatal("Invalid id should have risen an error")
	}
	if _, err := model.NewEntity("openspace", "Office?"); err == nil {
		t.Fatal("Invalid entity type should have risen an error")
	}

	office, err := model.NewEntity("openspace", "Office")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	office.SetAttributeAsString("name", "Phoops HQ")
	office.SetAttributeAsFloat("temperature", 34.2) // it's July and fan coils aren't very good
	office.SetAttributeAsBoolean("dirty", false)
	office.SetAttributeAsBoolean("hot", true)
	timeNow := time.Now()
	office.SetAttributeAsDateTime("lastUpdate", timeNow)
	gp := model.NewGeoPoint(4.1, 2.3)
	office.SetAttributeAsGeoPoint("location", gp)
	if err := office.SetAttributeAsString("not valid", "invalid"); err == nil {
		t.Fatal("Expected an error for an invalid attribute")
	}

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

	dirtyAttr, err := unmarshaled.GetAttribute("dirty")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	dirtyVal, err := dirtyAttr.GetAsBoolean()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if dirtyVal {
		t.Fatalf("Expected '%t' for dirty value, got '%t'", false, dirtyVal)
	}

	hotAttr, err := unmarshaled.GetAttribute("hot")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	hotVal, err := hotAttr.GetAsBoolean()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if !hotVal {
		t.Fatalf("Expected '%t' for hot value, got '%t'", true, hotVal)
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

func TestDirectEntityAttributeAccess(t *testing.T) {
	office, _ := model.NewEntity("openspace", "Office")
	office.SetAttributeAsFloat("temperature", 34.2) // it's July and fan coils aren't very good
	office.SetAttributeAsBoolean("dirty", false)
	office.SetAttributeAsBoolean("hot", true)
	office.SetAttributeAsInteger("people", 11)
	timeNow := time.Now()
	office.SetAttributeAsDateTime("lastUpdate", timeNow)
	gp := model.NewGeoPoint(4.1, 2.3)
	office.SetAttributeAsGeoPoint("location", gp)

	if office.Id != "openspace" {
		t.Fatalf("Expected '%s' for id, got '%s'", "openspace", office.Id)
	}

	if office.Type != "Office" {
		t.Fatalf("Expected '%s' for type, got '%s'", "Office", office.Type)
	}

	temperatureAttr, err := office.GetAttribute("temperature")
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

	if tempAttrValue, err := office.GetAttributeAsFloat("temperature"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else if tempAttrValue != 34.2 {
		t.Fatalf("Expected '%v' for temperature attribute value, got '%v'", 34.2, tempAttrValue)
	}

	dirtyAttr, err := office.GetAttribute("dirty")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	dirtyVal, err := dirtyAttr.GetAsBoolean()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if dirtyVal {
		t.Fatalf("Expected '%t' for dirty value, got '%t'", false, dirtyVal)
	}

	if dirtyAttrValue, err := office.GetAttributeAsBoolean("dirty"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else if dirtyAttrValue {
		t.Fatalf("Expected '%t' for dirty attribute value, got '%t'", false, dirtyAttrValue)
	}

	hotAttr, err := office.GetAttribute("hot")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	hotVal, err := hotAttr.GetAsBoolean()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if !hotVal {
		t.Fatalf("Expected '%t' for hot value, got '%t'", true, hotVal)
	}

	if hotAttrValue, err := office.GetAttributeAsBoolean("hot"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else if !hotAttrValue {
		t.Fatalf("Expected '%t' for hot attribute value, got '%t'", true, hotAttrValue)
	}

	peopleAttr, err := office.GetAttribute("people")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	peopleVal, err := peopleAttr.GetAsInteger()
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if peopleVal != 11 {
		t.Fatalf("Expected '%v' for people value, got '%v'", 11, peopleVal)
	}

	if peopleAttrValue, err := office.GetAttributeAsInteger("people"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else if peopleAttrValue != 11 {
		t.Fatalf("Expected '%v' for people attribute value, got '%v'", 11, peopleAttrValue)
	}

	if lastUpdateAttr, err := office.GetAttribute("lastUpdate"); err != nil {
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

	if lastUpdateAttrValue, err := office.GetAttributeAsDateTime("lastUpdate"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else if lastUpdateAttrValue.Day() != timeNow.Day() || lastUpdateAttrValue.Minute() != lastUpdateAttrValue.Minute() {
		t.Fatalf("Expected '%v' for lastUpdate value, got '%v'", timeNow, lastUpdateAttrValue)
	}

	if locationAttr, err := office.GetAttribute("location"); err != nil {
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

	if locationAttrValue, err := office.GetAttributeAsGeoPoint("location"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else if locationAttrValue.Latitude != gp.Latitude || locationAttrValue.Longitude != gp.Longitude {
		t.Fatalf("Expected '%v' for location value, got '%v'", gp, locationAttrValue)
	}
}

func TestBuiltinAttributesUnmarshal(t *testing.T) {
	roomEntityJson := `
	{
		"id": "Room1",
		"pressure": {
			"metadata": {},
			"type": "Integer",
			"value": 720
		},
		"dateCreated": {
			"metadata": {},
			"type": "DateTime",
			"value": "2019-12-09T11:45:12.00Z"
		},
		"dateModified": {
			"metadata": {},
			"type": "DateTime",
			"value": "2019-12-09T11:57:12.00Z"
		},
		"dateExpires": {
			"metadata": {},
			"type": "DateTime",
			"value": "2019-12-31T12:05:00.00Z"
		},
		"type": "Room"
	}
`
	roomEntity := &model.Entity{}
	if err := json.Unmarshal([]byte(roomEntityJson), roomEntity); err != nil {
		t.Fatalf("Error unmarshaling entity: %v", err)
	}

	if dateCreated, err := roomEntity.GetDateCreated(); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if dateCreated.Day() != 9 || dateCreated.Minute() != 45 {
			t.Fatalf("Unexpected value reading dateCreated")
		}
	}

	if dateModified, err := roomEntity.GetDateModified(); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if dateModified.Day() != 9 || dateModified.Minute() != 57 {
			t.Fatalf("Unexpected value reading dateModified")
		}
	}
	if dateExpires, err := roomEntity.GetDateExpires(); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if dateExpires.Day() != 31 || dateExpires.Minute() != 5 {
			t.Fatalf("Unexpected value reading dateExpires")
		}
	}

}

func TestDateExpiresMarshal(t *testing.T) {
	office, err := model.NewEntity("openspace", "Office")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	timeGoneIn60Seconds := time.Now().Add(60 * time.Second)
	office.SetDateExpires(timeGoneIn60Seconds)

	bytes, err := json.Marshal(office)
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	unmarshaled := &model.Entity{}
	if err = json.Unmarshal(bytes, unmarshaled); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	if dateExpires, err := unmarshaled.GetDateExpires(); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if dateExpires.Day() != timeGoneIn60Seconds.Day() || dateExpires.Minute() != timeGoneIn60Seconds.Minute() || dateExpires.Second() != timeGoneIn60Seconds.Second() {
			t.Fatalf("Expected '%v' for dateExpires value, got '%v'", timeGoneIn60Seconds, dateExpires)
		}
	}
}

func TestIsValidString(t *testing.T) {
	if !model.IsValidString("hi there!") {
		t.Fatal("String shoud be valid")
	}
	if !model.IsValidString("") {
		t.Fatal("Empty string shoud be valid")
	}
	if model.IsValidString("Park (7)") {
		t.Fatal("String should be invalid")
	}
}

func TestSanitizeString(t *testing.T) {
	if model.SanitizeString("") != "" {
		t.Fatal("Invalid sanitization of empty string")
	}
	if model.SanitizeString("good string") != "good string" {
		t.Fatal("Invalid sanitization of a good string")
	}
	if model.SanitizeString("==> That's all, folks <3!") != " Thats all, folks 3!" {
		t.Fatal("Invalid sanitization of a nasty string")
	}
}

func TestIsValidFieldSyntax(t *testing.T) {
	if !model.IsValidFieldSyntax("hello") {
		t.Fatal("Field syntax shoud be valid")
	}
	if model.IsValidFieldSyntax("") {
		t.Fatal("Field syntax shoud not be valid for empty string")
	}
	b := make([]byte, 257)
	for i := range b {
		b[i] = 'x'
	}
	if model.IsValidFieldSyntax(string(b)) {
		t.Fatal("Field syntax shoud not be valid for a string that is too long")
	}
	if !model.IsValidFieldSyntax(string(b[:len(b)-1])) {
		t.Fatal("Field syntax shoud be valid for a string that is 256 characters long")
	}
	if !model.IsValidFieldSyntax("asdkoasdkoas.asd,asd!^asd") {
		t.Fatal("Field syntax shoud be valid")
	}
	if model.IsValidFieldSyntax("a b") {
		t.Fatal("Field syntax shoud not be valid for a string with whitespaces")
	}
	if model.IsValidFieldSyntax("a&b") ||
		model.IsValidFieldSyntax("a?b") ||
		model.IsValidFieldSyntax("a/b") ||
		model.IsValidFieldSyntax("a#b") {
		t.Fatal("Field syntax shoud not be valid for a string with restricted characters")
	}
	if model.IsValidFieldSyntax("a\fb") ||
		model.IsValidFieldSyntax("a\tb") ||
		model.IsValidFieldSyntax("a\bb") ||
		model.IsValidFieldSyntax("a\rb") ||
		model.IsValidFieldSyntax("a\nb") {
		t.Fatal("Field syntax shoud not be valid for a string with control characters")
	}
}

func TestIsValidAttributeName(t *testing.T) {
	if !model.IsValidAttributeName("temperature!") {
		t.Fatalf("Attribute name should be valid")
	}
	if model.IsValidAttributeName("not valid") ||
		model.IsValidAttributeName("temperature?") ||
		model.IsValidAttributeName("a/b") {
		t.Fatalf("Attribute name should not be valid")
	}
	if model.IsValidAttributeName("id") ||
		model.IsValidAttributeName("type") ||
		model.IsValidAttributeName("geo:distance") ||
		model.IsValidAttributeName("dateCreated") ||
		model.IsValidAttributeName("dateModified") {
		t.Fatalf("Attribute name should not be valid")
	}
}
