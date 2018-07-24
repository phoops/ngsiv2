package client_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/phoops/ngsiv2/client"
	"github.com/phoops/ngsiv2/model"
)

func TestBatchUpdateBadRequest(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				fmt.Println(w, `{"error":"ParseError","description":"Errors found in incoming JSON buffer"}`)
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
