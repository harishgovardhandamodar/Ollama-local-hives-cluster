package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const DefaultBaseURL = "http://localhost:8088"

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

type MemoryType string

const (
	MemoryTypeFact     MemoryType = "fact"
	MemoryTypeConcept  MemoryType = "concept"
	MemoryTypeEvent    MemoryType = "event"
	MemoryTypeRelation MemoryType = "relation"
	MemoryTypeEpisodic MemoryType = "episodic"
)

type RememberRequest struct {
	Content    string            `json:"content"`
	MemoryType MemoryType        `json:"memory_type,omitempty"`
	Dataset    string            `json:"dataset,omitempty"`
	UserID     string            `json:"user_id,omitempty"`
	SessionID  string            `json:"session_id,omitempty"`
	Metadata   map[string]any    `json:"metadata,omitempty"`
}

type MemoryResponse struct {
	ID         string         `json:"id"`
	Content    string         `json:"content"`
	MemoryType MemoryType     `json:"memory_type"`
	Dataset    string         `json:"dataset"`
	Confidence float64        `json:"confidence"`
	Metadata   map[string]any `json:"metadata"`
	CreatedAt  string         `json:"created_at"`
}

type RecallRequest struct {
	Query     string `json:"query"`
	Limit     int    `json:"limit,omitempty"`
	Dataset   string `json:"dataset,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
}

type RecallResult struct {
	ID         string  `json:"id"`
	Content    string  `json:"content"`
	Score      float64 `json:"score"`
	MemoryType string  `json:"memory_type"`
	Dataset    string  `json:"dataset"`
}

type RecallResponse struct {
	Results []RecallResult `json:"results"`
	Count   int            `json:"count"`
}

type ConnectRequest struct {
	SourceID     string  `json:"source_id"`
	TargetID     string  `json:"target_id"`
	RelationType string  `json:"relation_type"`
	Weight       float64 `json:"weight,omitempty"`
}

type ForgetRequest struct {
	Dataset  string `json:"dataset,omitempty"`
	MemoryID string `json:"memory_id,omitempty"`
}

type HealthResponse struct {
	Status string `json:"status"`
}

type StatusResponse struct {
	Status        string `json:"status"`
	Version       string `json:"version"`
	MemoriesCount int    `json:"memories_count"`
}

func (c *Client) Health() (*HealthResponse, error) {
	var resp HealthResponse
	if err := c.get("/health", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Status() (*StatusResponse, error) {
	var resp StatusResponse
	if err := c.get("/api", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Remember(req RememberRequest) (*MemoryResponse, error) {
	if req.MemoryType == "" {
		req.MemoryType = MemoryTypeFact
	}
	if req.Dataset == "" {
		req.Dataset = "default"
	}
	var resp MemoryResponse
	if err := c.post("/remember", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Recall(req RecallRequest) (*RecallResponse, error) {
	if req.Limit == 0 {
		req.Limit = 10
	}
	var resp RecallResponse
	if err := c.post("/recall", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Memories() (*RecallResponse, error) {
	var resp RecallResponse
	if err := c.get("/memories", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Connect(req ConnectRequest) (string, error) {
	if req.Weight == 0 {
		req.Weight = 1.0
	}
	var resp struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := c.post("/connect", req, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (c *Client) Forget(req ForgetRequest) (int, error) {
	var resp struct {
		Removed int `json:"removed"`
	}
	if err := c.post("/forget", req, &resp); err != nil {
		return 0, err
	}
	return resp.Removed, nil
}

func (c *Client) get(path string, out any) error {
	resp, err := c.HTTPClient.Get(c.BaseURL + path)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s: %d: %s", path, resp.StatusCode, body)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) post(path string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := c.HTTPClient.Post(
		c.BaseURL+path,
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s: %d: %s", path, resp.StatusCode, b)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
