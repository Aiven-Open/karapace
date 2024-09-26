package schema

import (
	"github.com/bufbuild/protocompile"
)

func NewCompiler(resolver protocompile.Resolver) *protocompile.Compiler {
	compiler := protocompile.Compiler{Resolver: resolver, RetainASTs: true}
	return &compiler
}
