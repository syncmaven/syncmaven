package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type RpcClient struct {
	url    string
	client http.Client
}

func NewRpcClient(url string) *RpcClient {
	return &RpcClient{url: url, client: http.Client{Timeout: time.Second * 5}}
}

func (r *RpcClient) Call(method string, body any) (any, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	url := r.url + "/" + method
	resp, err := r.client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("POST %s HTTP code = %d response: %s", url, resp.StatusCode, string(respBytes))
	}
	if resp.Header.Get("Content-Type") == "application/x-ndjson" {
		decoder := json.NewDecoder(resp.Body)
		arr := make([]any, 0)
		for {
			var object any
			err = decoder.Decode(&object)
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			arr = append(arr, object)
		}
		return arr, nil
	} else {
		var response any
		err := json.NewDecoder(resp.Body).Decode(&response)
		if err != nil {
			return nil, fmt.Errorf("POST %s Error unmarshalling response: %v", url, err)
		}
		return response, nil

	}
}

func (r *RpcClient) Get(key []string) (any, error) {
	body := make(map[string]any, 1)
	if len(key) == 1 {
		body["key"] = key[0]
	} else {
		body["key"] = key
	}
	return r.Call("state.get", body)
}

func (r *RpcClient) List(prefix []string) ([]any, error) {
	body := make(map[string]any, 1)
	if len(prefix) == 1 {
		body["prefix"] = prefix[0]
	} else {
		body["prefix"] = prefix
	}
	resp, err := r.Call("state.list", body)
	if err != nil {
		return nil, err
	}
	if arr, ok := resp.([]any); ok {
		return arr, nil
	} else {
		return nil, fmt.Errorf("unexpected response: %v", resp)
	}
}

func (r *RpcClient) Set(key []string, value any) error {
	body := make(map[string]any, 2)
	if len(key) == 1 {
		body["key"] = key[0]
	} else {
		body["key"] = key
	}
	body["value"] = value
	_, err := r.Call("state.set", body)
	return err
}

func (r *RpcClient) Del(key []string) error {
	body := make(map[string]any, 2)
	if len(key) == 1 {
		body["key"] = key[0]
	} else {
		body["key"] = key
	}
	_, err := r.Call("state.del", body)
	return err
}

func (r *RpcClient) DeleteByPrefix(prefix []string) error {
	body := make(map[string]any, 2)
	if len(prefix) == 1 {
		body["prefix"] = prefix[0]
	} else {
		body["prefix"] = prefix
	}
	_, err := r.Call("state.deleteByPrefix", body)
	return err
}

func (r *RpcClient) Size(key []string) (int, error) {
	body := make(map[string]any, 1)
	if len(key) == 1 {
		body["key"] = key[0]
	} else {
		body["key"] = key
	}
	resp, err := r.Call("state.get", body)
	if err != nil {
		return -1, err
	}
	if mp, ok := resp.(map[string]any); ok {
		return int(mp["size"].(float64)), nil
	} else {
		return -1, fmt.Errorf("unexpected response: %v", resp)
	}
}
