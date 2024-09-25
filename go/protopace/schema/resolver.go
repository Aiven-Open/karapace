package schema

import (
	"fmt"
	"strings"

	"github.com/bufbuild/protocompile"
)

type SchemaResolver struct {
	schemas map[string]Schema
}

func NewSchemaResolver(schemas []Schema) protocompile.Resolver {
	schemasIndex := map[string]Schema{}
	for _, schema := range schemas {
		schemasIndex[schema.Name] = schema
	}
	resolver := SchemaResolver{schemas: schemasIndex}
	return WithGoogleTypeImports(protocompile.WithStandardImports(&resolver))
}

func (s *SchemaResolver) AddSchema(schema Schema) {
	s.schemas[schema.Name] = schema
}

// FindFileByPath implements protocompile.Resolver.
func (s *SchemaResolver) FindFileByPath(path string) (protocompile.SearchResult, error) {
	searchResult := protocompile.SearchResult{}
	schema, ok := s.schemas[path]
	if !ok {
		return searchResult, fmt.Errorf("schema not found: %s", path)
	}
	searchResult.Source = strings.NewReader(schema.Schema)
	searchResult.ParseResult = schema.ParserResult
	return searchResult, nil
}

var _ protocompile.Resolver = (*SchemaResolver)(nil)
