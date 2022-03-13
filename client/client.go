package client

import (
	"bytes"
	"encoding/json"
	"errors"
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
	c                   *http.Client
	url                 string
	timeout             time.Duration
	apiRes              *model.APIResources
	customGlobalHeaders map[string]string
}

// ClientOptionFunc is a function that configures a NgsiV2Client.
type ClientOptionFunc func(*NgsiV2Client) error

// NewNgsiV2Client creates a new NGSIv2 client.
func NewNgsiV2Client(options ...ClientOptionFunc) (*NgsiV2Client, error) {
	c := &NgsiV2Client{
		timeout:             time.Second * 15,
		customGlobalHeaders: make(map[string]string),
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

// SetGlobalHeader is used a custom header applied to all the requests
// made to the context broker
func SetGlobalHeader(key string, value string) ClientOptionFunc {
	return func(c *NgsiV2Client) error {
		c.customGlobalHeaders[key] = value
		return nil
	}
}

type additionalHeader struct {
	key   string
	value string
}

func (c *NgsiV2Client) newRequest(method, url string, body io.Reader, additionalHeaders ...additionalHeader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "ngsiv2-client")
	req.Header.Add("Accept", "application/json")

	// set the global headers
	for header, value := range c.customGlobalHeaders {
		req.Header.Add(header, value)
	}

	for _, ah := range additionalHeaders {
		req.Header.Add(ah.key, ah.value)
	}
	return req, nil
}

func (c *NgsiV2Client) BatchUpdate(msg *model.BatchUpdate) error {
	jsonValue, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Could not serialize message: %+v", err)
	}
	req, err := c.newRequest("POST", fmt.Sprintf("%s/v2/op/update", c.url), bytes.NewBuffer(jsonValue))
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

func (c *NgsiV2Client) BatchQuery(msg *model.BatchQuery, options ...BatchQueryParamFunc) ([]*model.Entity, error) {
	params := new(batchQueryParams)

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return nil, err
		}
	}

	jsonValue, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("could not serialize message: %+v", err)
	}
	req, err := c.newRequest("POST", fmt.Sprintf("%s/v2/op/query", c.url), bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, fmt.Errorf("could not create request for batch query: %+v", err)
	}
	req.Header.Add("Content-Type", "application/json")
	q := req.URL.Query()

	if params.limit > 0 {
		q.Add("limit", strconv.Itoa(params.limit))
	}

	if params.offset > 0 {
		q.Add("offset", strconv.Itoa(params.offset))
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
		return nil, fmt.Errorf("Error invoking batch update: %+v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	}
	var ret []*model.Entity
	if err := json.Unmarshal(bodyBytes, &ret); err != nil {
		return nil, fmt.Errorf("Error reading batch query response: %+v", err)
	}
	return ret, nil
}

type batchQueryParams struct {
	limit   int
	offset  int
	orderBy []string
	options string
}

type BatchQueryParamFunc func(params *batchQueryParams) error

func BatchQuerySetLimit(limit int) BatchQueryParamFunc {
	return func(p *batchQueryParams) error {
		if limit <= 0 {
			return fmt.Errorf("limit cannot be less than or equal 0")
		}
		p.limit = limit
		return nil
	}
}

func BatchQuerySetOffset(offset int) BatchQueryParamFunc {
	return func(p *batchQueryParams) error {
		if offset < 0 {
			return fmt.Errorf("offset cannot be less than 0")
		}
		p.offset = offset
		return nil
	}
}

func BatchQueryAddOrderBy(attr string, ascending bool) BatchQueryParamFunc {
	return func(p *batchQueryParams) error {
		if !model.IsValidFieldSyntax(attr) {
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

func BatchQuerySetOptions(opts string) BatchQueryParamFunc {
	return func(p *batchQueryParams) error {
		return fmt.Errorf("not supported")
	}
}

// RetrieveAPIResources gives url link values for retrieving resources.
// See: https://orioncontextbroker.docs.apiary.io/#reference/api-entry-point/retrieve-api-resources/retrieve-api-resources
func (c *NgsiV2Client) RetrieveAPIResources() (*model.APIResources, error) {
	req, err := c.newRequest("GET", fmt.Sprintf("%s/v2", c.url), nil)
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

func (c *NgsiV2Client) getSubscriptionsUrl() (string, error) {
	if c.apiRes == nil {
		var err error
		if c.apiRes, err = c.RetrieveAPIResources(); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%s%s", c.url, c.apiRes.SubscriptionsUrl), nil
}

type fiwareHeaderParams struct {
	fiwareService     string
	fiwareServicePath string
}

func (f fiwareHeaderParams) headers() []additionalHeader {
	var ret []additionalHeader
	if f.fiwareService != "" {
		ret = append(ret, additionalHeader{"Fiware-Service", f.fiwareService})
	}
	if f.fiwareServicePath != "" {
		ret = append(ret, additionalHeader{"Fiware-ServicePath", f.fiwareServicePath})
	}
	return ret
}

type retrieveEntityParams struct {
	fiwareHeaderParams
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
	if !model.IsValidFieldSyntax(attr) {
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
	}

	return nil
}

func RetrieveEntitySetOptions(opts model.SimplifiedEntityRepresentation) RetrieveEntityParamFunc {
	return func(p *retrieveEntityParams) error {
		return setRetrieveEntityOptions(p, opts)
	}
}

func RetrieveEntitySetFiwareService(fiwareService string) RetrieveEntityParamFunc {
	return func(p *retrieveEntityParams) error {
		p.fiwareService = fiwareService
		return nil
	}
}

func RetrieveEntitySetFiwareServicePath(fiwareServicePath string) RetrieveEntityParamFunc {
	return func(p *retrieveEntityParams) error {
		p.fiwareServicePath = fiwareServicePath
		return nil
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

	req, err := c.newRequest("GET", fmt.Sprintf("%s/%s", eUrl, params.id), nil, params.headers()...)
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
		fmt.Println(string(bodyBytes))
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

func ListEntitiesSetIds(ids []string) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		for _, id := range ids {
			if !model.IsValidFieldSyntax(id) {
				return fmt.Errorf("'%s' is not a valid entity id", id)
			}
		}
		p.id = strings.Join(ids, ",")
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
		if offset < 0 {
			return fmt.Errorf("offset cannot be less than 0")
		}
		p.offset = offset
		return nil
	}
}

func ListEntitiesAddOrderBy(attr string, ascending bool) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		if !model.IsValidFieldSyntax(attr) {
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

func ListEntitiesSetFiwareService(fiwareService string) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		p.fiwareService = fiwareService
		return nil
	}
}

func ListEntitiesSetFiwareServicePath(fiwareServicePath string) ListEntitiesParamFunc {
	return func(p *listEntitiesParams) error {
		p.fiwareServicePath = fiwareServicePath
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

	req, err := c.newRequest("GET", fmt.Sprintf("%s", eUrl), nil, params.headers()...)
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
}

// CountEntities returns how many entities are compliant with parameters
func (c *NgsiV2Client) CountEntities(options ...ListEntitiesParamFunc) (int, error) {
	params := new(listEntitiesParams)

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return 0, err
		}
	}

	if params.id != "" && params.idPattern != "" {
		return 0, fmt.Errorf("Cannot use 'id' and 'idPattern' together")
	}

	eUrl, err := c.getEntitiesUrl()
	if err != nil {
		return 0, err
	}

	req, err := c.newRequest("GET", fmt.Sprintf("%s", eUrl), nil, params.headers()...)
	if err != nil {
		return 0, fmt.Errorf("Could not create request for API resources: %+v", err)
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

	q.Add("limit", strconv.Itoa(1))

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

	q.Add("options", string(model.CountRepresentation))

	req.URL.RawQuery = q.Encode()
	resp, err := c.c.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Could not list entities: %+v", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	}

	totalCount := resp.Header.Get("Fiware-Total-Count")
	if totalCount == "" {
		return 0, errors.New("Fiware-Total-Count not found in header")
	}
	cnt, err := strconv.Atoi(totalCount)
	if err != nil {
		return 0, err
	}
	return cnt, nil

}

type createEntityOption string

const (
	keyValuesCreateEntityOption createEntityOption = "keyValues"
	upsertCreateEntityOption    createEntityOption = "upsert"
)

type createEntityParams struct {
	fiwareHeaderParams
	options createEntityOption
}

type CreateEntityParamFunc func(*createEntityParams) error

func CreateEntitySetOptionsUpsert() CreateEntityParamFunc {
	return func(p *createEntityParams) error {
		p.options = upsertCreateEntityOption
		return nil
	}
}

func CreateEntitySetOptionsKeyValues() CreateEntityParamFunc {
	return func(p *createEntityParams) error {
		p.options = keyValuesCreateEntityOption
		return nil
	}
}

func CreateEntitySetFiwareService(fiwareService string) CreateEntityParamFunc {
	return func(p *createEntityParams) error {
		p.fiwareService = fiwareService
		return nil
	}
}

func CreateEntitySetFiwareServicePath(fiwareServicePath string) CreateEntityParamFunc {
	return func(p *createEntityParams) error {
		p.fiwareServicePath = fiwareServicePath
		return nil
	}
}

// CreateEntity creates a new entity passed as parameter.
// See: http://fiware.github.io/specifications/ngsiv2/stable -> Entities -> Create Entity
// It returns the resource location that has been created, if upsert is used or
// not and any error encountered.
func (c *NgsiV2Client) CreateEntity(entity *model.Entity, options ...CreateEntityParamFunc) (string, bool, error) {
	params := new(createEntityParams)

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return "", false, err
		}
	}

	eUrl, err := c.getEntitiesUrl()
	if err != nil {
		return "", false, err
	}

	jsonEntity, err := json.Marshal(entity)
	if err != nil {
		return "", false, fmt.Errorf("Could not serialize message: %v", err)
	}
	req, err := c.newRequest("POST", eUrl, bytes.NewBuffer(jsonEntity), params.headers()...)
	if err != nil {
		return "", false, fmt.Errorf("Could not create request for batch update: %v", err)
	}
	req.Header.Add("Content-Type", "application/json")
	if params.options != "" {
		q := req.URL.Query()
		q.Add("options", string(params.options))
		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("Error invoking entity creation: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		return resp.Header.Get("Location"), false, nil
	} else if resp.StatusCode == http.StatusNoContent {
		return resp.Header.Get("Location"), true, nil
	} else {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return "", false, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	}
	/*
		q := req.URL.Query()
		req.URL.RawQuery = q.Encode()

		return nil*/
}

type subscriptionParams struct {
	fiwareHeaderParams
	options createEntityOption
}

type SubscriptionParamFunc func(*subscriptionParams) error

func SubscriptionSetFiwareService(fiwareService string) SubscriptionParamFunc {
	return func(p *subscriptionParams) error {
		p.fiwareService = fiwareService
		return nil
	}
}

func SubscriptionSetFiwareServicePath(fiwareServicePath string) SubscriptionParamFunc {
	return func(p *subscriptionParams) error {
		p.fiwareServicePath = fiwareServicePath
		return nil
	}
}

// CreateSubscription creates a new subscription to the context broker.
// See: https://orioncontextbroker.docs.apiary.io/#reference/subscriptions/subscription-list/create-a-new-subscription
func (c *NgsiV2Client) CreateSubscription(subscription *model.Subscription, options ...SubscriptionParamFunc) (string, error) {
	params := new(subscriptionParams)

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return "", err
		}
	}

	jsonValue, err := json.Marshal(subscription)
	if err != nil {
		return "", fmt.Errorf("Could not serialize subscription: %+v", err)
	}

	sUrl, err := c.getSubscriptionsUrl()
	if err != nil {
		return "", err
	}
	req, err := c.newRequest("POST", sUrl, bytes.NewBuffer(jsonValue), params.headers()...)
	if err != nil {
		return "", fmt.Errorf("Could not create request for subscription creation: %+v", err)
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return "", fmt.Errorf("Error invoking create subscription: %+v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	}
	return strings.TrimPrefix(resp.Header.Get("Location"), c.apiRes.SubscriptionsUrl+"/"), nil
}

// RetrieveSubscription retrieves a subscription identified by the given id.
// See: https://orioncontextbroker.docs.apiary.io/#reference/subscriptions/subscription-by-id/retrieve-subscription
func (c *NgsiV2Client) RetrieveSubscription(id string) (*model.Subscription, error) {
	if id == "" {
		return nil, fmt.Errorf("Cannot retrieve subscription with empty 'id'")
	}

	sUrl, err := c.getSubscriptionsUrl()
	if err != nil {
		return nil, err
	}
	req, err := c.newRequest("GET", fmt.Sprintf("%s/%s", sUrl, id), nil)
	if err != nil {
		return nil, fmt.Errorf("Could not create request for subscription retrieval: %+v", err)
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Could not retrieve subscription: %+v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	} else {
		ret := new(model.Subscription)
		if err := json.Unmarshal(bodyBytes, ret); err != nil {
			return nil, fmt.Errorf("Error reading retrieve subscription response: %+v", err)
		} else {
			return ret, nil
		}
	}
}

type retrieveSubscriptionsParams struct {
	fiwareHeaderParams
	limit   int
	offset  int
	options string
}

type RetrieveSubscriptionsParamFunc func(*retrieveSubscriptionsParams) error

func RetrieveSubscriptionsSetLimit(limit int) RetrieveSubscriptionsParamFunc {
	return func(p *retrieveSubscriptionsParams) error {
		if limit <= 0 {
			return fmt.Errorf("limit cannot be less than or equal 0")
		}
		p.limit = limit
		return nil
	}
}

func RetrieveSubscriptionsSetOffset(offset int) RetrieveSubscriptionsParamFunc {
	return func(p *retrieveSubscriptionsParams) error {
		if offset < 0 {
			return fmt.Errorf("offset cannot be less than or equal 0")
		}
		p.offset = offset
		return nil
	}
}

func RetrieveSubscriptionsSetOptions(options string) RetrieveSubscriptionsParamFunc {
	return func(p *retrieveSubscriptionsParams) error {
		if options != "" && options != "count" {
			return fmt.Errorf("Invalid value for options param")
		}
		p.options = options
		return nil
	}
}

func RetrieveSubscriptionsSetFiwareService(fiwareService string) RetrieveSubscriptionsParamFunc {
	return func(p *retrieveSubscriptionsParams) error {
		p.fiwareService = fiwareService
		return nil
	}
}

func RetrieveSubscriptionsSetFiwareServicePath(fiwareServicePath string) RetrieveSubscriptionsParamFunc {
	return func(p *retrieveSubscriptionsParams) error {
		p.fiwareServicePath = fiwareServicePath
		return nil
	}
}

type SubscriptionsResponse struct {
	Count         int
	Subscriptions []*model.Subscription
}

// RetrieveSubscriptions returs the subscriptions present in the system.
// See: https://orioncontextbroker.docs.apiary.io/#reference/subscriptions/subscription-list/retrieve-subscriptions
func (c *NgsiV2Client) RetrieveSubscriptions(options ...RetrieveSubscriptionsParamFunc) (*SubscriptionsResponse, error) {
	params := new(retrieveSubscriptionsParams)

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return nil, err
		}
	}

	sUrl, err := c.getSubscriptionsUrl()
	if err != nil {
		return nil, err
	}
	req, err := c.newRequest("GET", sUrl, nil, params.headers()...)
	if err != nil {
		return nil, fmt.Errorf("Could not create request for subscriptions retrieval: %+v", err)
	}
	q := req.URL.Query()
	if params.limit > 0 {
		q.Add("limit", strconv.Itoa(params.limit))
	}
	if params.offset > 0 {
		q.Add("offset", strconv.Itoa(params.offset))
	}
	if params.options != "" {
		q.Add("options", string(params.options))
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Could not retrieve subscriptions: %+v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	} else {
		var subs []*model.Subscription
		if err := json.Unmarshal(bodyBytes, &subs); err != nil {
			return nil, fmt.Errorf("Error reading retrieve subscriptions response: %+v", err)
		} else {
			ret := new(SubscriptionsResponse)
			ret.Subscriptions = subs
			if c, err := strconv.Atoi(resp.Header.Get("Fiware-Total-Count")); err == nil {
				ret.Count = c
			}
			return ret, nil
		}
	}
}

// UpdateSubscription updates a subscription identified by the given id with the field specified in the request.
// See: https://orioncontextbroker.docs.apiary.io/#reference/subscriptions/subscription-by-id/update-subscription
func (c *NgsiV2Client) UpdateSubscription(id string, patchSubscription *model.Subscription, options ...SubscriptionParamFunc) error {
	if id == "" {
		return fmt.Errorf("Cannot update subscription with empty 'id'")
	}

	jsonValue, err := json.Marshal(patchSubscription)
	if err != nil {
		return fmt.Errorf("Could not serialize subscription: %+v", err)
	}

	sUrl, err := c.getSubscriptionsUrl()
	if err != nil {
		return err
	}

	params := new(subscriptionParams)

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return err
		}
	}

	req, err := c.newRequest("PATCH", fmt.Sprintf("%s/%s", sUrl, id), bytes.NewBuffer(jsonValue), params.headers()...)
	if err != nil {
		return fmt.Errorf("Could not create request for subscription updating: %+v", err)
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return fmt.Errorf("Error invoking update subscription: %+v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// DeleteSubscription cancels a subscription identified by the given id.
// See: https://orioncontextbroker.docs.apiary.io/#reference/subscriptions/subscription-by-id/delete-subscription
func (c *NgsiV2Client) DeleteSubscription(id string, options ...SubscriptionParamFunc) error {
	if id == "" {
		return fmt.Errorf("Cannot delete subscription with empty 'id'")
	}

	sUrl, err := c.getSubscriptionsUrl()
	if err != nil {
		return err
	}

	params := new(subscriptionParams)

	// apply the options
	for _, option := range options {
		if err := option(params); err != nil {
			return err
		}
	}

	req, err := c.newRequest("DELETE", fmt.Sprintf("%s/%s", sUrl, id), nil, params.headers()...)
	if err != nil {
		return fmt.Errorf("Could not create request for subscription deletion: %+v", err)
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return fmt.Errorf("Error invoking delete subscription: %+v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Unexpected status code: '%d'\nResponse body: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}
