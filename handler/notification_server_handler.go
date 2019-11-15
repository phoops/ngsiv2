// Handler for managing notification data received through subscriptions.
// Thanks to Matt Silverlock (https://twitter.com/@elithrar)
// for the idea on handlers with errors.
package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/phoops/ngsiv2/model"
)

// Error embeds the error interface and has a HTTP status code
type Error interface {
	error
	Status() int
}

// StatusError is an error with a HTTP status code
type StatusError struct {
	Code int
	Err  error
}

// StatusError satisfies the error interface
func (se StatusError) Error() string {
	return se.Err.Error()
}

// Returns the HTTP status code associated with the error
func (se StatusError) Status() int {
	return se.Code
}

// NotificationReceiver receives notifications from subscriptions
type NotificationReceiver interface {
	Receive(subscritionId string, entities []*model.Entity)
}

// Handler struct for managing errors and notification receivers
type Handler struct {
	Receivers []NotificationReceiver
	H         func(recs []NotificationReceiver, w http.ResponseWriter, r *http.Request) error
}

// Handler satisfies http.Handler
func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h.H(h.Receivers, w, r)
	if err != nil {
		var handlerError Error
		switch {
		case errors.As(err, &handlerError):
			http.Error(w, handlerError.Error(), handlerError.Status())
		default:
			// we don't know the status code desired, so we set the default
			// internal server error (HTTP 500)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
	}
}

func NewNgsiV2SubscriptionHandler(receivers ...NotificationReceiver) Handler {
	return Handler{receivers, NgsiV2SubscriptionHandler}
}

func NgsiV2SubscriptionHandler(receivers []NotificationReceiver, w http.ResponseWriter, r *http.Request) error {
	if r.Method != "POST" {
		return StatusError{http.StatusMethodNotAllowed, errors.New("Expected a POST")}
	}

	if ct := r.Header.Get("Content-Type"); ct != "" {
		if !strings.HasPrefix(ct, "application/json") {
			return StatusError{http.StatusBadRequest, errors.New("Invalid notification payload")}
		}
	}

	// maximum read of 8MB, the current max for Orion (https://fiware-orion.readthedocs.io/en/master/user/known_limitations/index.html)
	r.Body = http.MaxBytesReader(w, r.Body, 8*1024*1024)

	decoder := json.NewDecoder(r.Body)

	var n model.Notification
	err := decoder.Decode(&n)
	if err != nil {
		// unfortunately, it is not defined yet
		if err.Error() == "http: request body too large" {
			return StatusError{http.StatusRequestEntityTooLarge, err}
		} else {
			return StatusError{http.StatusBadRequest, err}
		}
	}

	for _, r := range receivers {
		r.Receive(n.SubscriptionId, n.Data)
	}
	return nil
}
