package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/mitchellh/mapstructure"
	geojson "github.com/paulmach/go.geojson"
)

var ErrInvalidCastingAttributeEntity = errors.New("could not cast the attribute of the entity")

// Entity is a context entity, i.e. a thing in the NGSI model.
type Entity struct {
	Id         string                `json:"id"`
	Type       string                `json:"type,omitempty"`
	Attributes map[string]*Attribute `json:"-"`
}

type typeValue struct {
	// Name  string      `json:"name"`
	Type  AttributeType `json:"type,omitempty"`
	Value interface{}   `json:"value"`
}

// Attribute is a Context attribute, i.e. a property of a context entity.
type Attribute struct {
	typeValue
	Metadata map[string]*Metadata `json:"metadata,omitempty"`
}

func NewAttribute(typ AttributeType, v interface{}) *Attribute {
	return &Attribute{
		typeValue: typeValue{
			Type:  typ,
			Value: v,
		},
	}
}

// Metadata is a Context metadata, i.e. an optional part of the attribute.
type Metadata struct {
	typeValue
}

type AttributeType string

// Constants representing NGSIv2 special data types
const (
	StringType          AttributeType = "String"
	TextType            AttributeType = "Text"
	NumberType          AttributeType = "Number"
	FloatType           AttributeType = "Float"
	IntegerType         AttributeType = "Integer"
	BooleanType         AttributeType = "Boolean"
	PercentageType      AttributeType = "Percentage"
	DateTimeType        AttributeType = "DateTime"
	GeoPointType        AttributeType = "geo:point"
	GeoLineType         AttributeType = "geo:line"
	GeoPolygonType      AttributeType = "geo:polygon"
	GeoBoxType          AttributeType = "geo:box"
	GeoJSONType         AttributeType = "geo:json"
	StructuredValueType AttributeType = "StructuredValue"
)

const (
	DateCreatedAttributeName  string = "dateCreated"
	DateModifiedAttributeName string = "dateModified"
	DateExpiresAttributeName  string = "dateExpires"
)

type ActionType string

const (
	AppendAction       ActionType = "append"
	AppendStrictAction ActionType = "appendStrict"
	UpdateAction       ActionType = "update"
	DeleteAction       ActionType = "delete"
	ReplaceAction      ActionType = "replace"
)

type GeoPoint struct {
	Latitude  float64
	Longitude float64
}

type APIResources struct {
	EntitiesUrl      string `json:"entities_url"`
	TypesUrl         string `json:"types_url"`
	SubscriptionsUrl string `json:"subscriptions_url"`
	RegistrationsUrl string `json:"registrations_url"`
}

type BatchUpdate struct {
	ActionType ActionType `json:"actionType"`
	Entities   []*Entity  `json:"entities"`
}

type BatchQuery struct {
	Entities   []*EntityMatcher `json:"entities,omitempty"`
	Attrs      []string         `json:"attrs,omitempty"`
	Expression *QueryExpression `json:"expression,omitempty"`
	Metadata   []string         `json:"metadata,omitempty"`
}

func (batchQuery *BatchQuery) Match(matchers ...*EntityMatcher) error {
	for _, matcher := range matchers {
		if matcher.Id == "" && matcher.IdPattern == "" {
			return fmt.Errorf("id or idPattern must be present")
		}
		if matcher.Id != "" && matcher.IdPattern != "" {
			return fmt.Errorf("id and idPattern cannot be used at the same time")
		}
		if matcher.Type != "" && matcher.TypePattern != "" {
			return fmt.Errorf("type and typePattern cannot be used at the same time")
		}
		batchQuery.Entities = append(batchQuery.Entities, matcher)
	}

	return nil
}

type EntityMatcher struct {
	Id          string `json:"id,omitempty"`
	IdPattern   string `json:"idPattern,omitempty"`
	Type        string `json:"type,omitempty"`
	TypePattern string `json:"typePattern,omitempty"`
}

func NewEntityMatcher() *EntityMatcher {
	return &EntityMatcher{}
}

func (entityMatcher *EntityMatcher) ById(id string) *EntityMatcher {
	entityMatcher.Id = id
	return entityMatcher
}

func (entityMatcher *EntityMatcher) ByIdPattern(idPattern string) *EntityMatcher {
	entityMatcher.IdPattern = idPattern
	return entityMatcher
}
func (entityMatcher *EntityMatcher) ByType(typeName string) *EntityMatcher {
	entityMatcher.Type = typeName
	return entityMatcher
}
func (entityMatcher *EntityMatcher) ByTypePattern(typePattern string) *EntityMatcher {
	entityMatcher.TypePattern = typePattern
	return entityMatcher
}

type QueryExpression struct {
	Q        string                       `json:"q,omitempty"`
	Mq       string                       `json:"mq,omitempty"`
	Georel   GeospatialRelationship       `json:"georel,omitempty"`
	Geometry SimpleLocationFormatGeometry `json:"geometry,omitempty"`
	Coords   string                       `json:"coords,omitempty"`
}

type SubscriptionSubjectEntity = EntityMatcher

type SubscriptionSubjectConditionExpression = QueryExpression

type SubscriptionSubjectCondition struct {
	Attrs      []string                                `json:"attrs,omitempty"`
	Expression *SubscriptionSubjectConditionExpression `json:"expression,omitempty"`
}

type SubscriptionSubject struct {
	Entities  []*SubscriptionSubjectEntity  `json:"entities,omitempty"`
	Condition *SubscriptionSubjectCondition `json:"condition,omitempty"`
}

type SubscriptionNotificationHttp struct {
	Url string `json:"url"`
}

type SubscriptionNotificationHttpCustom struct {
	Url     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Qs      map[string]string `json:"qs,omitempty"`
	Method  string            `json:"method,omitempty"`
	Payload string            `json:"payload,omitempty"`
}

type SubscriptionNotification struct {
	Attrs            []string                            `json:"attrs,omitempty"`
	ExceptAttrs      []string                            `json:"exceptAttrs,omitempty"`
	Http             *SubscriptionNotificationHttp       `json:"http,omitempty"`
	HttpCustom       *SubscriptionNotificationHttpCustom `json:"httpCustom,omitempty"`
	AttrsFormat      string                              `json:"attrsFormat,omitempty"`
	Metadata         []string                            `json:"metadata,omitempty"`
	TimesSent        uint                                `json:"timesSent,omitempty"`
	LastNotification *time.Time                          `json:"lastNotification,omitempty"`
	LastFailure      *time.Time                          `json:"lastFailure,omitempty"`
	LastSuccess      *time.Time                          `json:"lastSuccess,omitempty"`
	LastSuccessCode  *uint                               `json:"lastSuccessCode,omitempty"`
}

type Notification struct {
	Data           []*Entity `json:"data"`
	SubscriptionId string    `json:"subscriptionId"`
}

type OrionTime struct {
	time.Time
}

func (t OrionTime) MarshalJSON() ([]byte, error) {
	if y := t.Year(); y < 0 || y >= 10000 {
		// RFC 3339 is clear that years are 4 digits exactly.
		// See golang.org/issue/4556#c15 for more discussion.
		return nil, fmt.Errorf("OrionTime.MarshalJSON: year outside of range [0,9999] ('%d')", y)
	}
	return []byte(t.Format(`"2006-01-02T15:04:05.999Z07:00"`)), nil
}

type Subscription struct {
	Id           string                    `json:"id,omitempty"`
	Description  string                    `json:"description,omitempty"`
	Subject      *SubscriptionSubject      `json:"subject,omitempty"`
	Notification *SubscriptionNotification `json:"notification,omitempty"`
	Expires      *OrionTime                `json:"expires,omitempty"`
	Status       SubscriptionStatus        `json:"status,omitempty"`
	Throttling   uint                      `json:"throttling,omitempty"`
}

type SubscriptionStatus string

const (
	SubscriptionActive   SubscriptionStatus = "active"
	SubscriptionInactive SubscriptionStatus = "inactive"
	SubscriptionExpired  SubscriptionStatus = "expired"
	SubscriptionFailed   SubscriptionStatus = "failed"
)

const (
	InvalidChars      string = `<>"'=;()`
	InvalidFieldChars string = `&?/#` // plus control characters and whitespaces
)

var ReservedAttrNames = [...]string{"id", "type", "geo:distance", "dateCreated", "dateModified"}

// SimplifiedEntityRepresentation are representation modes to generate simplified
// representations of entitites.
// See: https://orioncontextbroker.docs.apiary.io/#introduction/specification/simplified-entity-representation
type SimplifiedEntityRepresentation string

const (
	KeyValuesRepresentation SimplifiedEntityRepresentation = "keyValues"
	ValuesRepresentation    SimplifiedEntityRepresentation = "values"
	UniqueRepresentation    SimplifiedEntityRepresentation = "unique"
	CountRepresentation     SimplifiedEntityRepresentation = "count"
)

type SimpleLocationFormatGeometry string

const (
	SLFPoint   SimpleLocationFormatGeometry = "point"
	SLFLine    SimpleLocationFormatGeometry = "line"
	SLFPolygon SimpleLocationFormatGeometry = "polygon"
	SLFBox     SimpleLocationFormatGeometry = "box"
)

type GeospatialRelationship string

const (
	GeorelNear       GeospatialRelationship = "near"
	GeorelCoveredBy  GeospatialRelationship = "coveredBy"
	GeorelIntersects GeospatialRelationship = "intersects"
	GeorelEquals     GeospatialRelationship = "equals"
	GeorelDisjoint   GeospatialRelationship = "disjoint"
)

type GeorelModifier string

func GeorelModifierMaxDistance(maxDistance float64) GeorelModifier {
	return GeorelModifier(fmt.Sprintf("maxDistance:%v", maxDistance))
}

func GeorelModifierMinDistance(minDistance float64) GeorelModifier {
	return GeorelModifier(fmt.Sprintf("minDistance:%v", minDistance))
}

type SimpleQueryOperator string

const (
	SQEqual              SimpleQueryOperator = "=="
	SQUnequal            SimpleQueryOperator = "!="
	SQGreaterThan        SimpleQueryOperator = ">"
	SQLessThan           SimpleQueryOperator = "<"
	SQGreaterOrEqualThan SimpleQueryOperator = ">="
	SQLessOrEqualThan    SimpleQueryOperator = "<="
	SQMatchPattern       SimpleQueryOperator = "~="
)

type SimpleQueryStatement string

func NewBinarySimpleQueryStatement(attr string, operator SimpleQueryOperator, value string) (SimpleQueryStatement, error) {
	if !IsValidAttributeName(attr) {
		return "", fmt.Errorf("'%s' is not a valid attribute name", attr)
	}
	quotedValue := value
	if operator == SQEqual || operator == SQUnequal {
		quotedValue = quoteIfComma(value)
	}
	return SimpleQueryStatement(fmt.Sprintf("%s%s%s", attr, operator, quotedValue)), nil
}

func NewBinarySimpleQueryStatementMultipleValues(attr string, operator SimpleQueryOperator, values ...string) (SimpleQueryStatement, error) {
	if !IsValidAttributeName(attr) {
		return "", fmt.Errorf("'%s' is not a valid attribute name", attr)
	}
	if len(values) == 0 {
		return "", fmt.Errorf("Cannot create simple query statement without values")
	}
	if operator != SQEqual && operator != SQUnequal {
		return "", fmt.Errorf("Multiple values are only permitted for equal or unequal operators")
	}
	var quotedValues = make([]string, len(values))
	for i, v := range values {
		quotedValues[i] = quoteIfComma(v)
	}
	return SimpleQueryStatement(fmt.Sprintf("%s%s%s", attr, operator, strings.Join(quotedValues, ","))), nil
}

func NewBinarySimpleQueryStatementRange(attr string, operator SimpleQueryOperator, minimum string, maximum string) (SimpleQueryStatement, error) {
	if !IsValidAttributeName(attr) {
		return "", fmt.Errorf("'%s' is not a valid attribute name", attr)
	}
	if operator != SQEqual && operator != SQUnequal {
		return "", fmt.Errorf("Range is only permitted for equal or unequal operators")
	}
	return SimpleQueryStatement(fmt.Sprintf("%s%s%s..%s", attr, operator, quoteIfComma(minimum), quoteIfComma(maximum))), nil
}

func quoteIfComma(str string) string {
	if strings.Contains(str, ",") {
		return "'" + str + "'"
	} else {
		return str
	}
}

// Creates a new context entity with id and type and no attributes.
func NewEntity(id string, entityType string) (*Entity, error) {
	if err := validateFieldSyntax(id); err != nil {
		return nil, err
	} else if err := validateFieldSyntax(entityType); err != nil {
		return nil, err
	}
	e := &Entity{}
	e.Id = id
	e.Type = entityType
	e.Attributes = make(map[string]*Attribute)
	return e, nil
}

type _entity Entity

func (e *Entity) UnmarshalJSON(b []byte) error {
	t_ := _entity{}
	if err := json.Unmarshal(b, &t_); err != nil {
		return err
	}

	var jsonValues map[string]json.RawMessage

	if err := json.Unmarshal(b, &jsonValues); err != nil {
		return err
	}
	/*if err := json.Unmarshal(b, &(t_.Attributes)); err != nil {
		return err
	}*/

	typ := reflect.TypeOf(t_)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		jsonTag := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonTag != "" && jsonTag != "-" {
			delete(jsonValues, jsonTag)
		}
	}

	t_.Attributes = make(map[string]*Attribute, len(jsonValues))
	for attr, aJson := range jsonValues {
		if !IsValidFieldSyntax(attr) {
			fmt.Printf("[Warning] Attribute %v has wrong field syntax\n", attr)
		}
		var a Attribute

		if err := json.Unmarshal(aJson, &a); err != nil {
			return err
		}
		switch a.Type {
		case DateTimeType:
			val, ok := a.Value.(string)
			if !ok {
				return fmt.Errorf("Invalid DateTimeType value: '%v'", a.Value)
			}
			if v, err := time.Parse(time.RFC3339, val); err == nil {
				a.Value = v
			}
		case GeoPointType:
			g := new(GeoPoint)
			val, ok := a.Value.(string)
			if !ok {
				return fmt.Errorf("Invalid geo:point value: '%v'", a.Value)
			}
			if err := g.UnmarshalJSON([]byte(val)); err == nil {
				a.Value = g
			}
		case GeoJSONType:
			var ma map[string]json.RawMessage
			if err := json.Unmarshal(aJson, &ma); err != nil {
				return err
			}
			gJSON, ok := ma["value"]
			if !ok {
				return fmt.Errorf("Invalid geo:json value: '%v'", a)
			}
			g := new(geojson.Geometry)
			if err := g.UnmarshalJSON(gJSON); err != nil {
				return err
			}
			a.Value = g
		}
		t_.Attributes[attr] = &a
	}

	*e = Entity(t_)

	return nil
}

func (e *Entity) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{})

	for k, v := range e.Attributes {
		data[k] = v
	}

	// Take all the struct values with a json tag
	val := reflect.ValueOf(*e)
	typ := reflect.TypeOf(*e)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldv := val.Field(i)
		jsonTag := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonTag != "" && jsonTag != "-" {
			data[jsonTag] = fieldv.Interface()
		}
	}

	return json.Marshal(data)
}

func (e *Entity) String() string {
	b, _ := e.MarshalJSON()
	return string(b)
}

func NewGeoPoint(latitude float64, longitude float64) *GeoPoint {
	return &GeoPoint{latitude, longitude}
}

func (p *GeoPoint) UnmarshalJSON(b []byte) error {
	tokens := strings.Split(string(b), ",")
	if len(tokens) != 2 {
		return fmt.Errorf("Invalid geo:point value: '%s'", string(b))
	}
	lat, err := strconv.ParseFloat(strings.TrimSpace(tokens[0]), 64)
	if err != nil {
		return fmt.Errorf("Invalid latitude value: '%s'", tokens[0])
	}
	lon, err := strconv.ParseFloat(strings.TrimSpace(tokens[1]), 64)
	if err != nil {
		return fmt.Errorf("Invalid longitude value: '%s'", tokens[1])
	}
	*p = GeoPoint{lat, lon}
	return nil
}

func (p *GeoPoint) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%v, %v"`, p.Latitude, p.Longitude)), nil
}

func (e *Entity) GetAttribute(name string) (*Attribute, error) {
	if attr, ok := e.Attributes[name]; ok {
		return attr, nil
	} else {
		return nil, fmt.Errorf("Entity has no attribute '%s'", name)
	}
}

// IsValidString checks whether the string is valid or contains any forbidden character.
// See: https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/user/forbidden_characters.md
func IsValidString(str string) bool {
	return !strings.ContainsAny(str, InvalidChars)
}

// SanitizeString removes any forbidden character from a string.
func SanitizeString(str string) string {
	return strings.Map(func(r rune) rune {
		if strings.IndexRune(InvalidChars, r) < 0 {
			return r
		}
		return -1
	}, str)
}

// IsValidFieldSyntax checks whether the field syntax is valid or violates restrictions.
// See: https://orioncontextbroker.docs.apiary.io/#introduction/specification/field-syntax-restrictions
func IsValidFieldSyntax(str string) bool {
	if len(str) < 1 || len(str) > 256 {
		return false
	}
	for _, r := range str {
		if unicode.IsControl(r) ||
			unicode.IsSpace(r) ||
			strings.ContainsRune(InvalidFieldChars, r) {
			return false
		}
	}
	return true
}

func validateFieldSyntax(str string) error {
	if !IsValidFieldSyntax(str) {
		return fmt.Errorf("'%s': syntax error for field", str)
	} else {
		return nil
	}
}

// IsValidAttributeName checks whether the attribute name is valid or is forbidden.
// See: https://orioncontextbroker.docs.apiary.io/#introduction/specification/attribute-names-restrictions
func IsValidAttributeName(name string) bool {
	if !IsValidFieldSyntax(name) {
		return false
	}
	for _, reserved := range ReservedAttrNames {
		if name == reserved {
			return false
		}
	}
	return true
}

func validateAttributeName(name string) error {
	if !IsValidAttributeName(name) {
		return fmt.Errorf("'%s' is not a valid attribute name", name)
	} else {
		return nil
	}
}

func (e *Entity) SetAttribute(name string, typ AttributeType, value interface{}) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  typ,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsString(name string, value string) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}

	if !IsValidString(value) {
		return fmt.Errorf("Invalid string value for attribute %s, contains invalid chars", name)
	}

	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  StringType,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsText(name string, value string) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}

	if !IsValidString(value) {
		return fmt.Errorf("Invalid string value for attribute %s, contains invalid chars", name)
	}

	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  TextType,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsNumber(name string, value float64) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  NumberType,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsInteger(name string, value int) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  IntegerType,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsFloat(name string, value float64) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  FloatType,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsBoolean(name string, value bool) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  BooleanType,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsDateTime(name string, value time.Time) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  DateTimeType,
			Value: OrionTime{value},
		},
	}
	return nil
}

func (e *Entity) SetDateExpires(value time.Time) {
	e.Attributes[DateExpiresAttributeName] = &Attribute{
		typeValue: typeValue{
			Type:  DateTimeType,
			Value: OrionTime{value},
		},
	}
}

func (e *Entity) SetAttributeAsGeoPoint(name string, value *GeoPoint) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  GeoPointType,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsGeoJSON(name string, value *geojson.Geometry) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  GeoJSONType,
			Value: value,
		},
	}
	return nil
}

func (e *Entity) SetAttributeAsStructuredValue(name string, value interface{}) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  StructuredValueType,
			Value: value,
		},
	}
	return nil
}

func (a *Attribute) GetAsString() (string, error) {
	if a.Type != StringType && a.Type != TextType {
		return "", fmt.Errorf("Attribute is nor String or Text, but %s", a.Type)
	}
	rawString, ok := a.Value.(string)
	if !ok {
		return "", ErrInvalidCastingAttributeEntity
	}
	return rawString, nil
}

func (a *Attribute) GetAsInteger() (int, error) {
	if a.Type != IntegerType {
		return 0, fmt.Errorf("Attribute is not Integer, but %s", a.Type)
	}
	// when we read from JSON, an int is a float64, when we fill with this library, an int is... an int!
	f, ok := a.Value.(float64)
	if !ok {
		return a.Value.(int), nil
	}

	if f > 0 && int(f) < 0 {
		return 0, errors.New("integer out of range")
	}

	return int(f), nil
}

func (a *Attribute) GetAsFloat() (float64, error) {
	if a.Type != FloatType && a.Type != NumberType {
		return 0, fmt.Errorf("Attribute is nor Float or Number, but %s", a.Type)
	}
	rawFloat, ok := a.Value.(float64)
	if !ok {
		return 0, ErrInvalidCastingAttributeEntity
	}
	return rawFloat, nil
}

func (a *Attribute) GetAsBoolean() (bool, error) {
	if a.Type != BooleanType {
		return false, fmt.Errorf("Attribute is not Boolean, but %s", a.Type)
	}
	rawBool, ok := a.Value.(bool)
	if !ok {
		return false, ErrInvalidCastingAttributeEntity
	}
	return rawBool, nil
}

func (a *Attribute) GetAsDateTime() (time.Time, error) {
	if a.Type != DateTimeType {
		return time.Time{}, fmt.Errorf("Attribute is not DateTime, but %s", a.Type)
	}
	if dt, ok := a.Value.(time.Time); !ok {
		if dt, ok := a.Value.(OrionTime); !ok {
			return time.Time{}, fmt.Errorf("Attribute with date time type does not contain time value")
		} else {
			return dt.Time, nil
		}
	} else {
		return dt, nil
	}
}

func (a *Attribute) GetAsGeoPoint() (*GeoPoint, error) {
	if a.Type != GeoPointType {
		return nil, fmt.Errorf("Attribute is not GeoPoint, but '%s'", a.Type)
	}
	if g, ok := a.Value.(*GeoPoint); !ok {
		return nil, fmt.Errorf("Attribute with geopoint type does not contain geopoint value")
	} else {
		return g, nil
	}
}

func (a *Attribute) GetAsGeoJSON() (*geojson.Geometry, error) {
	if a.Type != GeoJSONType {
		return nil, fmt.Errorf("Attribute is not geo:json, but '%s'", a.Type)
	}
	g, ok := a.Value.(*geojson.Geometry)
	if !ok {
		return nil, fmt.Errorf("Attribute with geo:json type does not contain geo:json value")
	}
	return g, nil
}

// DecodeStructuredValue decodes the attribute into output if attribute type is StructuredValue.
// output must be a pointer to a map or struct.
func (a *Attribute) DecodeStructuredValue(output interface{}) error {
	if a.Type != StructuredValueType {
		return fmt.Errorf("Attribute is not %s, but '%s'", StructuredValueType, a.Type)
	}

	return mapstructure.Decode(a.Value, output)
}

func (e *Entity) GetAttributeAsString(attributeName string) (string, error) {
	if a, err := e.GetAttribute(attributeName); err != nil {
		return "", err
	} else {
		return a.GetAsString()
	}
}

func (e *Entity) GetAttributeAsInteger(attributeName string) (int, error) {
	if a, err := e.GetAttribute(attributeName); err != nil {
		return 0, err
	} else {
		return a.GetAsInteger()
	}
}

func (e *Entity) GetAttributeAsFloat(attributeName string) (float64, error) {
	if a, err := e.GetAttribute(attributeName); err != nil {
		return 0, err
	} else {
		return a.GetAsFloat()
	}
}

func (e *Entity) GetAttributeAsBoolean(attributeName string) (bool, error) {
	if a, err := e.GetAttribute(attributeName); err != nil {
		return false, err
	} else {
		return a.GetAsBoolean()
	}
}

func (e *Entity) GetAttributeAsDateTime(attributeName string) (time.Time, error) {
	if a, err := e.GetAttribute(attributeName); err != nil {
		return time.Time{}, err
	} else {
		return a.GetAsDateTime()
	}
}

func (e *Entity) GetDateExpires() (time.Time, error) {
	if a, err := e.GetAttribute(DateExpiresAttributeName); err != nil {
		return time.Time{}, err
	} else {
		return a.GetAsDateTime()
	}
}

func (e *Entity) GetDateCreated() (time.Time, error) {
	if a, err := e.GetAttribute(DateCreatedAttributeName); err != nil {
		return time.Time{}, err
	} else {
		return a.GetAsDateTime()
	}
}

func (e *Entity) GetDateModified() (time.Time, error) {
	if a, err := e.GetAttribute(DateModifiedAttributeName); err != nil {
		return time.Time{}, err
	} else {
		return a.GetAsDateTime()
	}
}

func (e *Entity) GetAttributeAsGeoPoint(attributeName string) (*GeoPoint, error) {
	if a, err := e.GetAttribute(attributeName); err != nil {
		return new(GeoPoint), err
	} else {
		return a.GetAsGeoPoint()
	}
}

func (e *Entity) GetAttributeAsGeoJSON(attributeName string) (*geojson.Geometry, error) {
	a, err := e.GetAttribute(attributeName)
	if err != nil {
		return new(geojson.Geometry), err
	}
	return a.GetAsGeoJSON()
}

// DecodeStructuredValueAttribute decodes the attribute named attributeName into output if
// attribute type is StructuredValue. output must be a pointer to a map or struct.
func (e *Entity) DecodeStructuredValueAttribute(attributeName string, output interface{}) error {
	a, err := e.GetAttribute(attributeName)
	if err != nil {
		return err
	}
	return a.DecodeStructuredValue(output)
}

func NewBatchUpdate(action ActionType) *BatchUpdate {
	b := &BatchUpdate{ActionType: action}
	return b
}

func (u *BatchUpdate) AddEntity(entity *Entity) {
	u.Entities = append(u.Entities, entity)
}
