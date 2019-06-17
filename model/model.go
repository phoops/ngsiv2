package model

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Entity is a context entity, i.e. a thing in the NGSI model.
type Entity struct {
	Id         string                `json:"id"`
	Type       string                `json:"type,omitempty"`
	Attributes map[string]*Attribute `json:"-"`
}

type typeValue struct {
	//Name  string      `json:"name"`
	Type  AttributeType `json:"type,omitempty"`
	Value interface{}   `json:"value"`
}

// Attribute is a Context attribute, i.e. a property of a context entity.
type Attribute struct {
	typeValue
	Metadata map[string]*Metadata `json:"metadata,omitempty"`
}

// Metadata is a Context metadata, i.e. an optional part of the attribute.
type Metadata struct {
	typeValue
}

type AttributeType string

const (
	StringType     AttributeType = "String"
	FloatType      AttributeType = "Float"
	IntegerType    AttributeType = "Integer"
	PercentageType AttributeType = "Percentage"
	DateTimeType   AttributeType = "DateTime"
	GeoPointType   AttributeType = "geo:point"
	GeoLineType    AttributeType = "geo:line"
	GeoPolygonType AttributeType = "geo:polygon"
	GeoBoxType     AttributeType = "geo:box"
	GeoJSONType    AttributeType = "geo:json"
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

type SubscriptionSubjectEntity struct {
	Id          string `json:"id,omitempty"`
	IdPattern   string `json:"idPattern,omitempty"`
	Type        string `json:"type,omitempty"`
	TypePattern string `json:"typePattern,omitempty"`
}

type SubscriptionSubjectConditionExpression struct {
	Q        string `json:"q,omitempty"`
	Mq       string `json:"mq,omitempty"`
	Georel   string `json:"georel,omitempty"`
	Geometry string `json:"geometry,omitempty"`
	Coords   string `json:"coords,omitempty"`
}

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

	_ = json.Unmarshal(b, &(t_.Attributes))
	/*if err := json.Unmarshal(b, &(t_.Attributes)); err != nil {
		return err
	}*/

	typ := reflect.TypeOf(t_)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		jsonTag := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonTag != "" && jsonTag != "-" {
			delete(t_.Attributes, jsonTag)
		}
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
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  StringType,
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

func (e *Entity) SetAttributeAsDateTime(name string, value time.Time) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	e.Attributes[name] = &Attribute{
		typeValue: typeValue{
			Type:  DateTimeType,
			Value: value,
		},
	}
	return nil
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

func (a *Attribute) GetAsString() (string, error) {
	if a.Type != StringType {
		return "", fmt.Errorf("Attribute is not String, but %s", a.Type)
	}
	return a.Value.(string), nil
}

func (a *Attribute) GetAsInteger() (int, error) {
	if a.Type != IntegerType {
		return 0, fmt.Errorf("Attribute is not Integer, but %s", a.Type)
	}
	return int(a.Value.(float64)), nil
}

func (a *Attribute) GetAsFloat() (float64, error) {
	if a.Type != FloatType {
		return 0, fmt.Errorf("Attribute is not Float, but %s", a.Type)
	}
	return a.Value.(float64), nil
}

func (a *Attribute) GetAsDateTime() (time.Time, error) {
	if a.Type != DateTimeType {
		return time.Time{}, fmt.Errorf("Attribute is not DateTime, but %s", a.Type)
	}
	if t, err := time.Parse(time.RFC3339, a.Value.(string)); err != nil {
		return time.Time{}, err
	} else {
		return t, nil
	}
}

func (a *Attribute) GetAsGeoPoint() (*GeoPoint, error) {
	if a.Type != GeoPointType {
		return nil, fmt.Errorf("Attribute is not GeoPoint, but '%s'", a.Type)
	}
	g := new(GeoPoint)
	if err := g.UnmarshalJSON([]byte(a.Value.(string))); err != nil {
		return nil, err
	} else {
		return g, nil
	}
}

func NewBatchUpdate(action ActionType) *BatchUpdate {
	b := &BatchUpdate{ActionType: action}
	return b
}

func (u *BatchUpdate) AddEntity(entity *Entity) {
	u.Entities = append(u.Entities, entity)
}
