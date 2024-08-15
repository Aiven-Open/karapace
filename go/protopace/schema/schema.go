package schema

import (
	"context"
	"strings"

	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/protocompile/linker"
	"github.com/bufbuild/protocompile/parser"
	"github.com/bufbuild/protocompile/reporter"
	"github.com/gofrs/uuid/v5"
)

var (
	handler = reporter.NewHandler(nil)
)

type Schema struct {
	Schema       string
	Name         string
	ParserResult parser.Result
	Dependencies []Schema
}

func FromString(name string, proto string, dependencies []Schema) (*Schema, error) {
	fileNode, err := parser.Parse(name, strings.NewReader(proto), handler)
	if err != nil {
		return nil, err
	}
	result, err := parser.ResultFromAST(fileNode, true, handler)
	if err != nil {
		return nil, err
	}
	return &Schema{Schema: proto, Name: name, ParserResult: result, Dependencies: dependencies}, nil
}

func (s Schema) Compile() ([]linker.Result, error) {
	resolver := NewSchemaResolver(append(s.Dependencies, s))
	compiler := NewCompiler(resolver)
	ctx := context.Background()
	schemas := []string{s.Name}
	for _, dep := range s.Dependencies {
		schemas = append(schemas, dep.Name)
	}
	files, err := compiler.Compile(ctx, schemas...)
	if err != nil {
		return nil, err
	}
	res := make([]linker.Result, len(files))
	for i, f := range files {
		res[i] = f.(linker.Result)
	}
	return res, nil
}

func (s Schema) CompileBufImage() (bufimage.Image, error) {
	res, err := s.Compile()
	if err != nil {
		return nil, err
	}
	files := make([]bufimage.ImageFile, len(res))
	for i, r := range res {
		file, err := bufimage.NewImageFile(
			r.FileDescriptorProto(),
			nil,
			uuid.Nil,
			"",
			"",
			false,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
		files[i] = file
	}

	image, err := bufimage.NewImage(files)
	return image, err
}
