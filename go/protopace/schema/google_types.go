package schema

import (
	"embed"
	"io"

	"github.com/bufbuild/protocompile"
)

//go:embed google/type/*.proto
var files embed.FS

// WithGoogleTypeImports returns a new resolver that can provide the source code for google/types protos
func WithGoogleTypeImports(resolver protocompile.Resolver) protocompile.Resolver {
	return protocompile.CompositeResolver{
		resolver,
		&protocompile.SourceResolver{
			Accessor: func(path string) (io.ReadCloser, error) {
				return files.Open(path)
			},
		},
	}
}
