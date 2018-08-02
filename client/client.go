package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/phoops/ngsiv2/model"
)

type NgsiV2Client struct {
	c       *http.Client
	url     string
	timeout time.Duration
	apiRes  *model.APIResources
}

// ClientOptionFunc is a function that configures a NgsiV2Client.
type ClientOptionFunc func(*NgsiV2Client) error

// NewNgsiV2Client creates a new NGSIv2 client.
func NewNgsiV2Client(options ...ClientOptionFunc) (*NgsiV2Client, error) {
	c := &NgsiV2Client{
		timeout: time.Second * 15,
	}

	// apply the options
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	c.c = &http.Client{
		Timeout: c.timeout,
	}

	return c, nil
}

// SetClientTimeout is used to specify a value for http client timeout.
func SetClientTimeout(timeout time.Duration) ClientOptionFunc {
	return func(c *NgsiV2Client) error {
		c.timeout = timeout
		return nil
	}
}

// SetUrl is used to set client URL.
func SetUrl(url string) ClientOptionFunc {
	return func(c *NgsiV2Client) error {
		c.url = url
		return nil
	}
}

func newRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "ngsiv2-client")
	req.Header.Add("Accept", "application/json")
	return req, nil
}

func (c *NgsiV2Client) BatchUpdate(msg *model.BatchUpdate) error {
	jsonValue, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Could not serialize message: %+v", err)
	}
	req, err := newRequest("POST", fmt.Sprintf("%s/v2/op/update", c.url), bytes.NewBuffer(jsonValue))
	if err != nil {
		return fmt.Errorf("Could not create request for batch update: %+v", err)
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return fmt.Errorf("Error invoking batch update: %+v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// RetrieveAPIResources gives url link values for retrieving resources.
// See: https://orioncontextbroker.docs.apiary.io/#reference/api-entry-point/retrieve-api-resources/retrieve-api-resources
func (c *NgsiV2Client) RetrieveAPIResources() (*model.APIResources, error) {
	req, err := newRequest("GET", fmt.Sprintf("%s/v2", c.url), nil)
	if err != nil {
		return nil, fmt.Errorf("Could not create request for API resources: %+v", err)
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Could not retrieve API resources: %+v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	} else {
		ret := new(model.APIResources)
		if err := json.Unmarshal(bodyBytes, ret); err != nil {
			return nil, fmt.Errorf("Error reading API resources response: %+v", err)
		} else {
			return ret, nil
		}
	}
}

func (c *NgsiV2Client) getEntitiesUrl() (string, error) {
	if c.apiRes == nil {
		var err error
		if c.apiRes, err = c.RetrieveAPIResources(); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%s%s", c.url, c.apiRes.EntitiesUrl), nil
}

type retrieveEntityParams struct {
	id         string
	entityType string
	attrs      []string
	options    model.SimplifiedEntityRepresentation
}

type RetrieveEntityParamFunc func(*retrieveEntityParams) error

func setRetrieveEntityType(p *retrieveEntityParams, entityType string) error {
	if !model.IsValidFieldSyntax(entityType) {
		return fmt.Errorf("'%s' is not a valid entity type name", entityType)
	}
	p.entityType = entityType
	return nil
}

func RetrieveEntitySetType(entityType string) RetrieveEntityParamFunc {
	return func(p *retrieveEntityParams) error {
		return setRetrieveEntityType(p, entityType)
	}
}

func addRetrieveEntityAttribute(p *retrieveEntityParams, attr string) error {
	if !model.IsValidAttributeName(attr) {
		return fmt.Errorf("'%s' is not a valid attribute name", attr)
	}
	p.attrs = append(p.attrs, attr)
	return nil
}

func RetrieveEntityAddAttribute(attr string) RetrieveEntityParamFunc {
	return func(p *retrieveEntityParams) error {
		return addRetrieveEntityAttribute(p, attr)
	}
}

func setRetrieveEntityOptions(p *retrieveEntityParams, opts model.SimplifiedEntityRepresentation) error {
	if opts != "" {
		return fmt.Errorf("Simplified entity representation is not supported yet!")
	} else {
		return nil
	}
}

func RetrieveEntitySetOptions(opts model.SimplifiedEntityRepresentation) RetrieveEntityParamFunc {
	return func(p *retrieveEntityParams) error {
		return setRetrieveEntityOptions(p, opts)
	}
}

// RetrieveEntity retrieves an object representing the entity identified by the given id.
// See: https://orioncontextbroker.docs.apiary.io/#reference/entities/entity-by-id/retrieve-entity
func (c *NgsiV2Client) RetrieveEntity(id string, options ...RetrieveEntityParamFunc) (*model.Entity, error) {
	if id == "" {
		return nil, fmt.Errorf("Cannot retrieve entity with empty 'id'")
	}

	params := new(retrieveEntityParams)
	params.id = id

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return nil, err
		}
	}

	eUrl, err := c.getEntitiesUrl()
	if err != nil {
		return nil, err
	}

	req, err := newRequest("GET", fmt.Sprintf("%s/%s", eUrl, params.id), nil)
	if err != nil {
		return nil, fmt.Errorf("Could not create request for API resources: %+v", err)
	}
	q := req.URL.Query()
	if params.entityType != "" {
		q.Add("type", params.entityType)
	}
	attributes := strings.Join(params.attrs, ",")
	if attributes != "" {
		q.Add("attrs", attributes)
	}
	if params.options != "" {
		q.Add("options", string(params.options))
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Could not retrieve entity: %+v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusConflict {
		return nil, fmt.Errorf("Conflict (id non-unique?).\nResponse body: %s", string(bodyBytes))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	} else {
		ret := new(model.Entity)
		if err := json.Unmarshal(bodyBytes, ret); err != nil {
			return nil, fmt.Errorf("Error reading retrieve entity response: %+v", err)
		} else {
			return ret, nil
		}
	}
}

type listEntitiesParams struct {
	retrieveEntityParams
	idPattern string
	q         []string
	georel    string
	geometry  string
	coords    []string
	limit     int
	offset    int
	orderBy   []string
}

type ListEntitiesParamFunc func(*listEntitiesParams) error

func ListEntitiesSetId(id string) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		if !model.IsValidFieldSyntax(id) {
			return fmt.Errorf("'%s' is not a valid entity id", id)
		}
		p.id = id
		return nil

	}
}

func ListEntitiesSetIdPattern(idPattern string) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		if _, err := regexp.Compile(idPattern); err != nil {
			return err
		}
		p.idPattern = idPattern
		return nil
	}
}

func ListEntitiesSetType(entityType string) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		return setRetrieveEntityType(&p.retrieveEntityParams, entityType)
	}
}

func ListEntitiesAddAttribute(attr string) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		return addRetrieveEntityAttribute(&p.retrieveEntityParams, attr)
	}
}

func ListEntitiesSetOptions(opts model.SimplifiedEntityRepresentation) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		return setRetrieveEntityOptions(&p.retrieveEntityParams, opts)
	}
}

func ListEntitiesSetLimit(limit int) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		if limit <= 0 {
			return fmt.Errorf("limit cannot be less than or equal 0")
		}
		p.limit = limit
		return nil
	}
}

func ListEntitiesSetOffset(offset int) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		if offset <= 0 {
			return fmt.Errorf("offset cannot be less than 0")
		}
		p.offset = offset
		return nil
	}
}

func ListEntitiesAddOrderBy(attr string, ascending bool) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		if !model.IsValidAttributeName(attr) {
			return fmt.Errorf("'%s' is not a valid attribute name", attr)
		}
		if ascending {
			p.orderBy = append(p.orderBy, attr)
		} else {
			p.orderBy = append(p.orderBy, "!"+attr)
		}
		return nil
	}
}

func ListEntitiesAddCoord(latitude float64, longitude float64) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		p.coords = append(p.coords, fmt.Sprintf("%v,%v", latitude, longitude))
		return nil
	}
}

func ListEntitiesSetGeometry(slfGeometry model.SimpleLocationFormatGeometry) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		p.geometry = string(slfGeometry)
		return nil
	}
}

func ListEntitiesSetGeoRel(georel model.GeospatialRelationship, modifiers ...model.GeorelModifier) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		var s []string
		s = append(s, string(georel))
		for _, m := range modifiers {
			s = append(s, string(m))
		}
		p.georel = strings.Join(s, ";")
		return nil
	}
}

func ListEntitiesAddQueryStatement(statement model.SimpleQueryStatement) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		p.q = append(p.q, string(statement))
		return nil
	}
}

// ListEntities retrieves a list of entities that match all criteria.
// See: https://orioncontextbroker.docs.apiary.io/#reference/entities/list-entities
func (c *NgsiV2Client) ListEntities(options ...ListEntitiesParamFunc) ([]*model.Entity, error) {
	params := new(listEntitiesParams)

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return nil, err
		}
	}

	if params.id != "" && params.idPattern != "" {
		return nil, fmt.Errorf("Cannot use 'id' and 'idPattern' together")
	}

	eUrl, err := c.getEntitiesUrl()
	if err != nil {
		return nil, err
	}

	req, err := newRequest("GET", fmt.Sprintf("%s", eUrl), nil)
	if err != nil {
		return nil, fmt.Errorf("Could not create request for API resources: %+v", err)
	}
	q := req.URL.Query()
	if params.id != "" {
		q.Add("id", params.id)
	}
	if params.idPattern != "" {
		q.Add("idPattern", params.idPattern)
	}
	if params.entityType != "" {
		q.Add("type", params.entityType)
	}
	attributes := strings.Join(params.attrs, ",")
	if attributes != "" {
		q.Add("attrs", attributes)
	}
	qExpr := strings.Join(params.q, ";")
	if qExpr != "" {
		q.Add("q", qExpr)
	}
	if params.limit > 0 {
		q.Add("limit", strconv.Itoa(params.limit))
	}
	if params.offset > 0 {
		q.Add("offset", strconv.Itoa(params.offset))
	}
	if params.georel != "" {
		q.Add("georel", params.georel)
	}
	if params.geometry != "" {
		q.Add("geometry", params.geometry)
	}
	coordsStr := strings.Join(params.coords, ";")
	if coordsStr != "" {
		q.Add("coords", coordsStr)
	}
	orderByStr := strings.Join(params.orderBy, ",")
	if orderByStr != "" {
		q.Add("orderBy", orderByStr)
	}
	if params.options != "" {
		q.Add("options", string(params.options))
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Could not list entities: %+v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	} else {
		var ret []*model.Entity
		if err := json.Unmarshal(bodyBytes, &ret); err != nil {
			return nil, fmt.Errorf("Error reading list entities response: %+v", err)
		} else {
			return ret, nil
		}
	}
	return nil, nil
}
