package main

import (
	"context"

	"github.com/Aiven-Open/karapace/go/protopace/schema"

	"github.com/bufbuild/buf/private/bufpkg/bufcheck/bufbreaking"
	"github.com/bufbuild/buf/private/bufpkg/bufconfig"
	"github.com/bufbuild/buf/private/pkg/tracing"
	"go.uber.org/zap"
)

func Check(schema schema.Schema, previousSchema schema.Schema) error {
	handler := bufbreaking.NewHandler(zap.NewNop(), tracing.NopTracer)
	ctx := context.Background()
	image, err := schema.CompileBufImage()
	if err != nil {
		return err
	}
	previousImage, err := previousSchema.CompileBufImage()
	if err != nil {
		return err
	}
	checkConfig, _ := bufconfig.NewEnabledCheckConfig(
		bufconfig.FileVersionV2,
		nil,
		[]string{
			"FIELD_NO_DELETE",
			"FILE_SAME_PACKAGE",
			"FIELD_SAME_NAME",
			"FIELD_SAME_JSON_NAME",
			"FILE_NO_DELETE",
			"ENUM_NO_DELETE",
		},
		nil,
		nil,
	)
	config := bufconfig.NewBreakingConfig(checkConfig, false)
	return handler.Check(ctx, config, previousImage, image)
}
