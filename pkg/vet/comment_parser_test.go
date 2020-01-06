package vet_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/houqp/sqlvet/pkg/vet"
)

func TestParseComment(t *testing.T) {
	tcase := []string{
		"sqlvet: ignore",
		"   sqlvet:ignore",
		" sqlvet: ignore",
		"  sqlvet: ignore ",
	}

	for _, c := range tcase {
		t.Run(c, func(t *testing.T) {
			anno, err := vet.ParseComment(c)
			assert.NoError(t, err)
			assert.Equal(t, vet.SqlVetAnnotation{Ignore: true}, anno)
		})
	}
}

func TestParseCommentWithoutAnnotation(t *testing.T) {
	tcase := []string{
		"ssqlvet: ignore",
		"   sqlvet:ok",
		"sqlvet ignore",
		"hello world!",
	}

	for _, c := range tcase {
		t.Run(c, func(t *testing.T) {
			_, err := vet.ParseComment(c)
			assert.Error(t, err)
		})
	}
}
