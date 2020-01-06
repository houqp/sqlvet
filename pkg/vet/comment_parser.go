package vet

import (
	"errors"
	"strings"
)

type SqlVetAnnotation struct {
	Ignore bool
}

func parseAnnotation(comment string) (SqlVetAnnotation, error) {
	anno := SqlVetAnnotation{}
	comment = strings.TrimSpace(comment)
	if strings.HasPrefix(comment, "ignore") {
		anno.Ignore = true
		return anno, nil
	}
	return anno, errors.New("Invalid annotation")
}

func ParseComment(comment string) (SqlVetAnnotation, error) {
	comment = strings.TrimSpace(comment)
	if !strings.HasPrefix(comment, "sqlvet:") {
		return SqlVetAnnotation{}, errors.New("Not found")
	}
	return parseAnnotation(comment[7:])
}
