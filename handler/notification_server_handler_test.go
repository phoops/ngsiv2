package handler_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/phoops/ngsiv2/handler"
	"github.com/phoops/ngsiv2/model"
)

type testReceiver struct {
	notifications map[string][]*model.Entity
}

func newTestReceiver() *testReceiver {
	return &testReceiver{
		notifications: make(map[string][]*model.Entity),
	}
}

func (tr *testReceiver) Receive(subscritionId string, entities []*model.Entity) {
	tr.notifications[subscritionId] = append(tr.notifications[subscritionId], entities...)
}

func TestSubscriptionHandlerNotificationInvalidMethod(t *testing.T) {
	receiver := newTestReceiver()
	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()
	h := handler.NewNgsiV2SubscriptionHandler(receiver)

	h.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("wrong status code: expected %v, got %v", http.StatusMethodNotAllowed, status)
	}
}

func TestSubscriptionHandlerNotificationInvalidHeader(t *testing.T) {
	receiver := newTestReceiver()
	req, _ := http.NewRequest(http.MethodPost, "/test", strings.NewReader(`
{
    "data": [
        {
            "id": "Room1",
            "temperature": {
                "metadata": {},
                "type": "Float",
                "value": 28.5
            },
            "type": "Room"
        }
    ],
    "subscriptionId": "57458eb60962ef754e7c0998"
}`))
	req.Header.Add("Content-Type", "text/plain")
	rr := httptest.NewRecorder()
	h := handler.NewNgsiV2SubscriptionHandler(receiver)

	h.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("wrong status code: expected %v, got %v", http.StatusBadRequest, status)
	}
}

func TestSubscriptionHandlerNotificationOneData(t *testing.T) {
	receiver := newTestReceiver()
	req, _ := http.NewRequest(http.MethodPost, "/test", strings.NewReader(`
{
    "data": [
        {
            "id": "Room1",
            "temperature": {
                "metadata": {},
                "type": "Float",
                "value": 28.5
            },
            "type": "Room"
        }
    ],
    "subscriptionId": "57458eb60962ef754e7c0998"
}`))
	req.Header.Add("Content-Type", "application/json; charset=utf-8")
	rr := httptest.NewRecorder()
	h := handler.NewNgsiV2SubscriptionHandler(receiver)

	h.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("wrong status code: expected %v, got %v", http.StatusOK, status)
	}

	if nn := len(receiver.notifications); nn != 1 {
		t.Errorf("expected 1 block of notifications, got %d", nn)
	}

	if entities, ok := receiver.notifications["57458eb60962ef754e7c0998"]; !ok {
		t.Errorf("expected a subscriptionId as entity key, but it was not found")
	} else {
		if ne := len(entities); ne != 1 {
			t.Errorf("expected 1 notification, got %d", ne)
		} else {
			e := entities[0]
			if e.Id != "Room1" {
				t.Errorf("Expected '%s' as entity id, got '%s'", "Room1", e.Id)
			}
			if temp, err := e.GetAttribute("temperature"); err != nil {
				t.Errorf("Error getting temperature attribute: %v", err)
			} else {
				if tempFloat, err := temp.GetAsFloat(); err != nil {
					t.Errorf("Error getting temperature value as float: %v", err)
				} else if tempFloat != 28.5 {
					t.Errorf("Expecting temperature attribute with value %2.1f, got %2.1f", 28.5, tempFloat)
				}
			}
		}
	}
}
