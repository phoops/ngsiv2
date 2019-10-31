package client_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/phoops/ngsiv2/client"
	"github.com/phoops/ngsiv2/model"
)

var apiResourcesHandler = func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"entities_url":"/v2/entities","types_url":"/v2/types","subscriptions_url":"/v2/subscriptions","registrations_url":"/v2/registrations"}`)
}

func TestBatchUpdateBadRequest(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, `{"error":"ParseError","description":"Errors found in incoming JSON buffer"}`)
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if err := cli.BatchUpdate(&model.BatchUpdate{}); err == nil {
		t.Fatal("Expected an error")
	}
}

func TestBatchUpdateNoContent(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Accept") != "application/json" {
					t.Fatal("Missing application/json accept header")
				}
				if b, err := ioutil.ReadAll(r.Body); err != nil {
					t.Fatalf("Unexpected error: '%v'", err)
				} else if !strings.Contains(string(b), "entities") {
					t.Fatal("Request doesn't contain entities")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNoContent)
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if err := cli.BatchUpdate(&model.BatchUpdate{}); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
}

func TestRetrieveAPIResources(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(apiResourcesHandler))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	if res, err := cli.RetrieveAPIResources(); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if res.EntitiesUrl != "/v2/entities" ||
			res.TypesUrl != "/v2/types" ||
			res.SubscriptionsUrl != "/v2/subscriptions" ||
			res.RegistrationsUrl != "/v2/registrations" {
			t.Fatal("Failed reading API resources values")
		}
	}
}

func TestRetrieveEntity(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					if r.Header.Get("Accept") != "application/json" {
						t.Fatal("Missing application/json accept header")
					}
					if r.Header.Get("Content-Type") != "" {
						t.Fatal("No Content-Type allowed for GET request")
					}
					if !strings.HasSuffix(r.URL.Path, "/r1") {
						t.Fatal("Expected 'r1' as id")
					}
					if r.URL.Query().Get("type") != "Room" {
						t.Fatalf("Expected 'type' value: 'Room', got '%s'", r.URL.Query().Get("type"))
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, `{"id":"r1","type":"Room","pressure":{"type":"Integer","value":"720","metadata":{}},"temperature":{"type":"Float","value":23,"metadata":{}}}`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	if res, err := cli.RetrieveEntity("r1", client.RetrieveEntitySetType("Room")); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if res.Id != "r1" ||
			res.Type != "Room" ||
			res.Attributes["temperature"].Type != model.FloatType {
			t.Fatal("Invalid entity retrieved")
		}
	}
}

func TestRetrieveEntities(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					if r.Header.Get("Accept") != "application/json" {
						t.Fatal("Missing application/json accept header")
					}
					if r.Header.Get("Content-Type") != "" {
						t.Fatal("No Content-Type allowed for GET request")
					}
					if r.URL.Query().Get("type") != "Room" {
						t.Fatalf("Expected 'type' value: 'Room', got '%s'", r.URL.Query().Get("type"))
					}
					if r.URL.Query().Get("q") != "temperature>30" {
						t.Fatalf("Expected 'q' expression: 'temperature>30', got '%s'", r.URL.Query().Get("q"))
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, `[{"id":"r2","type":"Room","pressure":{"type":"Integer","value":"720","metadata":{}},"temperature":{"type":"Float","value":34,"metadata":{}}},{"id":"r5","type":"Room","pressure":{"type":"Integer","value":"700","metadata":{}},"temperature":{"type":"Float","value":31,"metadata":{}}}
]`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	qst, err := model.NewBinarySimpleQueryStatement("temperature", model.SQGreaterThan, "30")
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if res, err := cli.ListEntities(
		client.ListEntitiesSetType("Room"),
		client.ListEntitiesAddQueryStatement(qst)); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if len(res) != 2 {
			t.Fatalf("Expected 2 entities, got %d", len(res))
		}
		if res[0].Id != "r2" ||
			res[0].Type != "Room" ||
			res[0].Attributes["temperature"].Type != model.FloatType ||
			res[1].Id != "r5" ||
			res[1].Type != "Room" ||
			res[1].Attributes["temperature"].Type != model.FloatType {
			t.Fatal("Invalid entities retrieved")
		}
	}
}

func TestCreateEntityBadRequest(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprintf(w, `{"error":"BadRequest","description":"entity id length: 0, min length supported: 1"}`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if _, _, err := cli.CreateEntity(&model.Entity{}); err == nil {
		t.Fatal("Expected an error")
	}
}

func sampleEntity() *model.Entity {
	e, _ := model.NewEntity("Bcn-Welt", "Room")
	e.SetAttributeAsFloat("temperature", 21.7)
	e.SetAttributeAsInteger("humidity", 60)
	return e
}

func TestCreateEntityCreated(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					if r.Header.Get("Content-Type") != "application/json" {
						t.Fatal("Missing application/json Content-Type header")
					}
					if r.URL.Query().Get("options") != "upsert" {
						t.Fatalf("Expected upsert options value, got: '%v'", r.URL.Query().Get("options"))
					}
					if b, err := ioutil.ReadAll(r.Body); err != nil {
						t.Fatalf("Unexpected error: '%v'", err)
					} else if len(string(b)) < 1 {
						t.Fatal("Request doesn't contain data")
					}
					w.Header().Set("Location", "/v2/entities/Bcn-Welt?type=Room")
					w.WriteHeader(http.StatusCreated)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	if loc, upsert, err := cli.CreateEntity(sampleEntity(), client.CreateEntitySetOptionsUpsert()); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if loc != "/v2/entities/Bcn-Welt?type=Room" {
			t.Fatalf("Expected '%s' location, got '%s'", "/v2/entities/Bcn-Welt?type=Room", loc)
		}
		if upsert {
			t.Fatalf("Expected no upsert, but got an upsert")
		}
	}
}

func TestCreateEntityNoContent(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					if r.Header.Get("Content-Type") != "application/json" {
						t.Fatal("Missing application/json Content-Type header")
					}
					if r.URL.Query().Get("options") != "upsert" {
						t.Fatalf("Expected upsert options value, got: '%v'", r.URL.Query().Get("options"))
					}
					if b, err := ioutil.ReadAll(r.Body); err != nil {
						t.Fatalf("Unexpected error: '%v'", err)
					} else if len(string(b)) < 1 {
						t.Fatal("Request doesn't contain entities")
					}
					w.Header().Set("Location", "/v2/entities/Bcn-Welt?type=Room")
					w.WriteHeader(http.StatusNoContent)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	if loc, upsert, err := cli.CreateEntity(sampleEntity(), client.CreateEntitySetOptionsUpsert()); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if loc != "/v2/entities/Bcn-Welt?type=Room" {
			t.Fatalf("Expected '%s' location, got '%s'", "/v2/entities/Bcn-Welt?type=Room", loc)
		}
		if !upsert {
			t.Fatalf("Expected upsert, but got no upsert response")
		}
	}
}

func TestCreateSubscriptionBadRequest(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprintf(w, `{"error":"BadRequest","description":"no subject for subscription specified"}`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if subId, err := cli.CreateSubscription(&model.Subscription{Description: "quite empty"}); err == nil {
		t.Fatal("Expected an error")
	} else if subId != "" {
		t.Fatalf("Subscription id should be empty, got '%s' instead", subId)
	}
}

func TestCreateSubscriptionCreated(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.Header().Set("Location", "/v2/subscriptions/abcde12345")
					w.WriteHeader(http.StatusCreated)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if subId, err := cli.CreateSubscription(&model.Subscription{Description: "quite empty, but we pretend it's ok"}); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else if subId != "abcde12345" {
		t.Fatalf("Subscription id should be abcde12345, got '%s' instead", subId)
	}
}

func TestRetrieveSubscriptionNotFound(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprintf(w, `{"error":"NotFound","description":"The requested subscription has not been found. Check id"}`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if sub, err := cli.RetrieveSubscription("123456789012345678901234"); err == nil {
		t.Fatal("Expected an error")
	} else if sub != nil {
		t.Fatalf("Subscription should be nil, got '%+v' instead", sub)
	}
}

func TestRetrieveSubscriptionOk(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					fmt.Fprintf(w, `{
  "id": "abcdef",
  "description": "One subscription to rule them all",
  "subject": {
    "entities": [
      {
        "idPattern": ".*",
        "type": "Room"
      }
    ],
    "condition": {
      "attrs": [ "temperature" ],
      "expression": {
        "q": "temperature>40"
      }
    }
  },
  "notification": {
    "http": {
      "url": "http://localhost:1234"
    },
    "attrs": ["temperature", "humidity"],
    "timesSent": 12,
    "lastNotification": "2015-10-05T16:00:00.00Z"
  },
  "expires": "2016-04-05T14:00:00.00Z",
  "status": "active",
  "throttling": 5
}`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if sub, err := cli.RetrieveSubscription("abcdef"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else if sub.Subject.Condition.Attrs[0] != "temperature" {
		t.Fatalf("Unexpected retrieved subscription, got '%+v'", sub)
	}
}

func TestUpdateSubscriptionNotFound(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprintf(w, `{"error":"No context element found","description":"subscription id not found"}`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	et := time.Now()
	if err := cli.UpdateSubscription("abcde12345", &model.Subscription{Expires: &model.OrionTime{et}}); err == nil {
		t.Fatal("Expected an error")
	}
}

func TestUpdateSubscriptionNoContent(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	et := time.Now()
	if err := cli.UpdateSubscription("abcde12345", &model.Subscription{Expires: &model.OrionTime{et}}); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
}

func TestDeleteSubscriptionNotFound(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprintf(w, `{"error":"NotFound","description":"The requested subscription has not been found. Check id"}`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if err := cli.DeleteSubscription("abcde12345"); err == nil {
		t.Fatal("Expected an error")
	}
}

func TestDeleteSubscriptionNoContent(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
	if err := cli.DeleteSubscription("abcde12345"); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}
}

func TestRetrieveSubscriptions(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v2") {
					apiResourcesHandler(w, r)
				} else {
					if r.URL.Query().Get("limit") != "50" {
						t.Fatalf("Expected a limit value of '50', got '%s'", r.URL.Query().Get("limit"))
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					fmt.Fprint(w, `[{"id":"5c001e0b8ecef47022068b21","description":"One subscription to rule them all","expires":"2016-04-05T14:00:00.00Z","status":"expired","subject":{"entities":[{"idPattern":".*","type":"Room"}],"condition":{"attrs":["temperature"],"expression":{"q":"temperature>40"}}},"notification":{"attrs":["temperature","humidity"],"attrsFormat":"normalized","http":{"url":"http://localhost:1234"}},"throttling":5}]`)
				}
			}))
	defer ts.Close()

	cli, err := client.NewNgsiV2Client(client.SetUrl(ts.URL))
	if err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	}

	if res, err := cli.RetrieveSubscriptions(client.RetrieveSubscriptionsSetLimit(50)); err != nil {
		t.Fatalf("Unexpected error: '%v'", err)
	} else {
		if len(res.Subscriptions) != 1 {
			t.Fatalf("Expected 1 subscription, got %d", len(res.Subscriptions))
		} else {
			if res.Subscriptions[0].Status != "expired" ||
				res.Subscriptions[0].Subject.Entities[0].Type != "Room" {
				t.Fatal("Invalid subscription retrieved")
			}
		}
	}
}
