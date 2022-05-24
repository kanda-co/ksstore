package ksstore

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotFound    = errors.New("record not found")
	ErrInternal    = errors.New("internal datastore error")
	ErrInvalidData = errors.New("invalid data record")
)

// FromError converts grpc status error to ksstore error
func FromError(err, fallback error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.NotFound {
		return ErrNotFound
	}
	if status.Code(err) == codes.InvalidArgument {
		return ErrInvalidData
	}
	if fallback != nil {
		return fallback
	}
	return ErrInternal
}

// Term type definition
type Term struct {
	Field string
	Op    string
	Value interface{}
}

// Storer data manager interface
type Storer interface {
	Client() interface{}
	SetTable(table string)
	Get(ctx context.Context, uid string) (map[string]interface{}, error)
	Set(ctx context.Context, uid string, in interface{}) (map[string]interface{}, error)
	All(ctx context.Context) ([]map[string]interface{}, error)
	Query(ctx context.Context, terms ...Term) ([]map[string]interface{}, error)
	Delete(ctx context.Context, uid string) (map[string]interface{}, error)
}

// FStore implementation with Storer of Firestore
type FStore struct {
	table  string
	client *firestore.Client
}

// Client return firestore client
func (s *FStore) Client() interface{} { return s.client }

// SetTable set collection
func (s *FStore) SetTable(table string) { s.table = table }

// Get return uid matched record
func (s *FStore) Get(ctx context.Context, uid string) (map[string]interface{}, error) {
	result, err := s.client.Collection(s.table).Doc(uid).Get(ctx)
	if err != nil {
		return nil, FromError(err, ErrInvalidData)
	}
	return result.Data(), nil
}

// Set upserts and set uid to provided record
func (s *FStore) Set(ctx context.Context, uid string, in interface{}) (map[string]interface{}, error) {
	if uid == "" {
		uid = uuid.New().String()
	}
	b, err := json.Marshal(in)
	if err != nil {
		log.Printf("Set.Marshal.Error: %v", err)
		return nil, FromError(err, ErrInvalidData)
	}
	doc := make(map[string]interface{})
	if err = json.Unmarshal(b, &doc); err != nil {
		log.Printf("Set.Unmarshal.Error: %v", err)
		return nil, FromError(err, ErrInvalidData)
	}
	doc["id"] = uid
	if _, err = s.client.Collection(s.table).Doc(uid).Set(ctx, doc, firestore.MergeAll); err != nil {
		log.Printf("Set.Doc.Set.Error: %v", err)
		return nil, FromError(err, nil)
	}
	return s.Get(ctx, uid)
}

// All return all records
func (s *FStore) All(ctx context.Context) ([]map[string]interface{}, error) {
	var (
		iter    *firestore.DocumentIterator
		results = make([]map[string]interface{}, 0)
	)
	collection := s.client.Collection(s.table)
	iter = collection.Documents(ctx)

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("All.Next.Error: %v", err)
			return nil, FromError(err, ErrInvalidData)
		}
		results = append(results, doc.Data())
	}
	return results, nil
}

// Query return records from matched terms
func (s *FStore) Query(ctx context.Context, terms ...Term) ([]map[string]interface{}, error) {
	var (
		query   firestore.Query
		iter    *firestore.DocumentIterator
		results = make([]map[string]interface{}, 0)
	)
	ref := s.client.Collection(s.table)
	for _, term := range terms {
		query = ref.Where(term.Field, term.Op, term.Value)
	}
	iter = query.Documents(ctx)

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Query.Next.Error: %v", err)
			return nil, FromError(err, ErrInvalidData)
		}
		results = append(results, doc.Data())
	}
	return results, nil
}

// Delete existing record with matched uid
func (s *FStore) Delete(ctx context.Context, uid string) (map[string]interface{}, error) {
	result, err := s.Get(ctx, uid)
	if err != nil {
		return nil, FromError(err, ErrInvalidData)
	}
	if _, err := s.client.Collection(s.table).Doc(uid).Delete(ctx); err != nil {
		log.Printf("Delete.Error: %v", err)
		return nil, FromError(err, nil)
	}
	return result, nil
}

// GetDefaultClient return default implemented Storer
func GetDefaultClient(ctx context.Context, db string) (Storer, error) {
	client, err := firestore.NewClient(ctx, db)
	if err != nil {
		log.Printf("GetDefaultClient.Error: %v", err)
		return nil, err
	}
	return &FStore{client: client}, nil
}

// Bind will decode and bind data from in to dst
func Bind(in interface{}, dst interface{}) error {
	b, err := json.Marshal(in)
	if err != nil {
		log.Printf("Bind.Marshal.Error: %v", err)
		return ErrInvalidData
	}
	if err = json.Unmarshal(b, &dst); err != nil {
		log.Printf("Bind.Unmarshal.Error: %v", err)
		return ErrInvalidData
	}
	return nil
}
