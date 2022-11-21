package esstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"k8s.io/klog/v2"
)

type Index struct {
	client *elasticsearch.Client
}

func NewIndex(client *elasticsearch.Client) *Index {
	return &Index{
		client: client,
	}
}
func (s *Index) SearchAll(ctx context.Context, query map[string]interface{}, indexNames []string) ([]*SearchResponse, error) {
	var result []*SearchResponse
	s.client.Search.WithScroll(1)
	resp, err := s.Search(ctx, query, indexNames, s.client.Search.WithScroll(1*time.Minute), s.client.Search.WithSize(5000))
	if err != nil {
		return nil, err
	}
	result = append(result, resp)
	scrollId := resp.ScrollId
	for {
		resp, err := s.ScrollSearch(ctx, scrollId)
		if err != nil {
			return nil, err
		}
		if len(resp.GetResources()) == 0 {
			s.ClearScroll(ctx, scrollId)
			break
		}
		result = append(result, resp)
	}
	return result, nil
}

func (s *Index) ClearScroll(ctx context.Context, scrollId string) error {
	res, err := s.client.ClearScroll(
		s.client.ClearScroll.WithContext(ctx),
		s.client.ClearScroll.WithScrollID(scrollId),
	)
	if err != nil {
		return err
	}
	if res.IsError() {
		return &ESError{

			StatusCode: res.StatusCode,
			Message:    res.String(),
		}
	}
	return nil
}
func (s *Index) ScrollSearch(ctx context.Context, scrollId string) (*SearchResponse, error) {
	res, err := s.client.Scroll(
		s.client.Scroll.WithContext(ctx),
		s.client.Scroll.WithScrollID(scrollId),
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, &ESError{
			StatusCode: res.StatusCode,
			Message:    res.String(),
		}
	}
	defer res.Body.Close()
	var r SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Index) Search(ctx context.Context, query map[string]interface{}, indexNames []string, searchOpts ...func(request *esapi.SearchRequest)) (*SearchResponse, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %s", err)
	}
	var searchFuns []func(searchRequest *esapi.SearchRequest)
	searchFuns = append(searchFuns, s.client.Search.WithContext(ctx))
	searchFuns = append(searchFuns, s.client.Search.WithIndex(indexNames...))
	searchFuns = append(searchFuns, s.client.Search.WithBody(&buf))
	if searchOpts != nil {
		searchFuns = append(searchOpts, searchOpts...)
	}
	res, err := s.client.Search(searchFuns...)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, &ESError{
			StatusCode: res.StatusCode,
			Message:    res.String(),
		}
	}
	defer res.Body.Close()
	var r SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Index) DeleteByQuery(ctx context.Context, query map[string]interface{}, indexName ...string) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}
	req := esapi.DeleteByQueryRequest{
		Index: indexName,
		Body:  &buf,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	if res.IsError() {
		return &ESError{
			StatusCode: res.StatusCode,
			Message:    res.String(),
		}
	}
	return nil
}

func (s *Index) DeleteById(ctx context.Context, docId string, indexName string) error {
	req := esapi.DeleteRequest{
		Index:      indexName,
		DocumentID: docId,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	if res.IsError() {
		return &ESError{
			StatusCode: res.StatusCode,
			Message:    res.String(),
		}
	}
	return nil
}

func (s *Index) Upsert(ctx context.Context, indexName string, uid string, doc map[string]interface{}) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal json error %v", err)
	}

	req := esapi.IndexRequest{
		DocumentID: uid,
		Body:       strings.NewReader(string(body)),
		Index:      indexName,
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	if res.IsError() {
		return &ESError{
			StatusCode: res.StatusCode,
			Message:    res.String(),
		}
	}
	return nil
}

func (s *Index) ListIndex() ([]string, error) {
	resp, err := s.client.Cat.Indices()
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, &ESError{
			StatusCode: resp.StatusCode,
			Message:    resp.String(),
		}
	}
	result := resp.String()
	klog.V(5).Info("cat index result: %s", result)
	result = strings.TrimPrefix(result, "[200 OK] ")
	reg := regexp.MustCompile("[^\\s]+")
	rows := strings.Split(result, "\n")
	var indexList []string
	for i := range rows {
		row := rows[i]
		if len(row) == 0 {
			continue
		}
		cols := reg.FindAllString(row, -1)
		if len(cols) > 3 {
			indexList = append(indexList, cols[2])
		}
	}
	return indexList, nil
}
