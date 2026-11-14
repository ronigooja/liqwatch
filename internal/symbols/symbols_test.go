package symbols

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetSendsBrowserUserAgent(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("User-Agent")
		w.Write([]byte("[]"))
	}))
	defer srv.Close()
	if _, err := get(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}
	if got != "Mozilla/5.0" {
		t.Fatalf("user agent %q", got)
	}
}
