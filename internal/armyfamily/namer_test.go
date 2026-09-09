package armyfamily

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestCloudflareNamerUsesGatewayAndModelPath(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != "https://api.example/accounts/account/ai/run/@cf/zai-org/glm-5.3-flash" {
			t.Fatalf("url = %s", req.URL)
		}
		if req.Header.Get("Authorization") != "Bearer secret" || req.Header.Get("cf-aig-gateway-id") != "clashking" {
			t.Fatalf("headers = %v", req.Header)
		}
		body, _ := io.ReadAll(req.Body)
		if !strings.Contains(string(body), `"max_tokens":24`) || !strings.Contains(string(body), "Root Rider") {
			t.Fatalf("body = %s", body)
		}
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"result":{"response":"Root Freeze"}}`)), Header: make(http.Header)}, nil
	})}
	name, err := (CloudflareNamer{HTTPClient: client, APIOrigin: "https://api.example", AccountID: "account", GatewayID: "clashking", APIToken: "secret"}).Name(t.Context(), NamingInput{TroopNames: []string{"Root Rider"}})
	if err != nil || name != "Root Freeze" {
		t.Fatalf("name=%q err=%v", name, err)
	}
}

func TestNameWithFallbackOnFailureAndDuplicate(t *testing.T) {
	input := NamingInput{Hash: Hash{0xab, 0xcd, 0xef}, TroopNames: []string{"Root Rider"}, ExistingNames: []string{"RootRider ABCDEF"}}
	namer := CloudflareNamer{HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"result":{"response":"Taken Name"}}`)), Header: make(http.Header)}, nil
	})}, APIOrigin: "https://api.example", AccountID: "a", GatewayID: "g", APIToken: "t"}
	input.ExistingNames = append(input.ExistingNames, "taken name")
	name, source := NameWithFallback(context.Background(), namer, input)
	if source != "fallback" || name != "RootRider ABCDEF2" {
		t.Fatalf("name=%q source=%q", name, source)
	}
}

func TestValidUniqueName(t *testing.T) {
	for _, name := range []string{"", "six word names are far too long", "bad/name", "Same"} {
		if ValidUniqueName(name, []string{"same"}) {
			t.Fatalf("accepted %q", name)
		}
	}
}
