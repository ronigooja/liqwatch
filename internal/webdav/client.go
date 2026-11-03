package webdav

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type Client struct {
	root, user, pass string
	HTTP             *http.Client
	Retries          int
	MaxKBps          int
}

func New(root, user, pass string) *Client {
	return &Client{root: strings.TrimRight(root, "/"), user: user, pass: pass, HTTP: &http.Client{Timeout: 5 * time.Minute}, Retries: 4}
}
func (c *Client) endpoint(name string) (string, error) {
	base, err := url.Parse(c.root)
	if err != nil {
		return "", err
	}
	base.Path = path.Join(base.Path, "/"+strings.TrimLeft(name, "/"))
	return base.String(), nil
}
func (c *Client) do(ctx context.Context, method, name string, body io.Reader, size int64) (*http.Response, error) {
	u, err := c.endpoint(name)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.user, c.pass)
	if method == "PROPFIND" {
		req.Header.Set("Depth", "0")
	}
	if size >= 0 {
		req.ContentLength = size
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("webdav %s: http status %d", method, resp.StatusCode)
	}
	return resp, nil
}
func (c *Client) retry(ctx context.Context, fn func() error) error {
	n := c.Retries
	if n <= 0 {
		n = 1
	}
	var err error
	for i := 0; i < n; i++ {
		if err = fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(1<<i) * 100 * time.Millisecond):
		}
	}
	return err
}

// MkdirAll creates the root directory and every level of name, then confirms the full path
// exists. MKCOL 405/409 are tolerated per level because servers differ on "already exists",
// but some also answer 405 when the parent is missing, so existence is checked at the end.
func (c *Client) MkdirAll(ctx context.Context, name string) error {
	levels := []string{""}
	cur := ""
	for _, p := range strings.Split(strings.Trim(name, "/"), "/") {
		if p == "" {
			continue
		}
		cur = path.Join(cur, p)
		levels = append(levels, cur)
	}
	for _, n := range levels {
		n := n
		err := c.retry(ctx, func() error {
			r, e := c.do(ctx, "MKCOL", n, nil, -1)
			if e != nil && !strings.Contains(e.Error(), "status 405") && !strings.Contains(e.Error(), "status 409") {
				return e
			}
			if r != nil {
				r.Body.Close()
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return c.retry(ctx, func() error {
		r, e := c.do(ctx, "PROPFIND", cur, nil, -1)
		if e != nil {
			return fmt.Errorf("directory %q not present after MKCOL: %w", cur, e)
		}
		r.Body.Close()
		return nil
	})
}
func (c *Client) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if c.MaxKBps > 0 {
		d := time.Duration(float64(len(b)) / float64(c.MaxKBps*1024) * float64(time.Second))
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return c.retry(ctx, func() error {
		resp, e := c.do(ctx, "PUT", name, bytes.NewReader(b), int64(len(b)))
		if e == nil {
			_, e = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return e
	})
}
func (c *Client) Stat(ctx context.Context, name string) (int64, error) {
	var n int64
	err := c.retry(ctx, func() error {
		r, e := c.do(ctx, "HEAD", name, nil, -1)
		if e != nil {
			return e
		}
		r.Body.Close()
		_, e = fmt.Sscan(r.Header.Get("Content-Length"), &n)
		return e
	})
	return n, err
}
func (c *Client) Get(ctx context.Context, name string) (io.ReadCloser, int64, error) {
	r, e := c.do(ctx, "GET", name, nil, -1)
	if e != nil {
		return nil, 0, e
	}
	return r.Body, r.ContentLength, nil
}
func SHA256(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, e := io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil)), n, e
}
