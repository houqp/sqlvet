package vet

import (
	"testing"

	"go/constant"
	"go/token"

	"github.com/houqp/gtest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/tools/go/ssa"
)

type ExtractQueryStrTests struct{}

func (s *ExtractQueryStrTests) Setup(t *testing.T)      {}
func (s *ExtractQueryStrTests) Teardown(t *testing.T)   {}
func (s *ExtractQueryStrTests) BeforeEach(t *testing.T) {}
func (s *ExtractQueryStrTests) AfterEach(t *testing.T)  {}

// func (s *ExtractQueryStrTests) SubTestVarArg(t *testing.T) {
// 	// vararg is parsed as *ssa.Slice
// 	//     eg: (*xorm.io/xorm.Session).Exec
// 	argVal := ssa.Slice{}
// 	s, err := extractQueryStrFromArg(ssa)
// 	assert.NoError(t, err)
// 	assert.Equal(t, "SELECT name FROM foo WHERE id=1", s)
// }

func (s *ExtractQueryStrTests) SubTestQueryStringAsInterface(t *testing.T) { 
	// query string constant is passed in as interface to match query function
	// signature
	expectedQs := "SELECT name FROM foo WHERE id=2"
	argVal := &ssa.MakeInterface{
		X: &ssa.Const{
			Value: constant.MakeString(expectedQs),
		},
	}

	qs, err := extractQueryStrFromSsaValue(argVal)
	assert.NoError(t, err)
	assert.Equal(t, expectedQs, qs)
}

func (s *ExtractQueryStrTests) SubTestQueryStringAsConstant(t *testing.T) {
	expectedQs := "SELECT name FROM foo WHERE id=1"
	argVal := &ssa.Const{
		Value: constant.MakeString(expectedQs),
	}

	qs, err := extractQueryStrFromSsaValue(argVal)
	assert.NoError(t, err)
	assert.Equal(t, expectedQs, qs)
}

func (s *ExtractQueryStrTests) SubTestQueryStringThroughAddBinOp(t *testing.T) {
	expectedQs := "SELECT id FROM table"
	argVal := &ssa.BinOp{
		Op: token.ADD,
		X: &ssa.Const{
			Value: constant.MakeString("SELECT "),
		},
		Y: &ssa.Const{
			Value: constant.MakeString("id FROM table"),
		},
	}

	qs, err := extractQueryStrFromSsaValue(argVal)
	assert.NoError(t, err)
	assert.Equal(t, expectedQs, qs)
}

func (s *ExtractQueryStrTests) SubTestQueryStringThroughNestedAddBinOp(t *testing.T) {
	expectedQs := "SELECT id FROM table WHERE id = 1"
	argVal := &ssa.BinOp{
		Op: token.ADD,
		X: &ssa.BinOp{
			Op: token.ADD,
			X: &ssa.Const{
				Value: constant.MakeString("SELECT "),
			},
			Y: &ssa.Const{
				Value: constant.MakeString("id FROM table"),
			},
		},
		Y: &ssa.Const{
			Value: constant.MakeString(" WHERE id = 1"),
		},
	}

	qs, err := extractQueryStrFromSsaValue(argVal)
	assert.NoError(t, err)
	assert.Equal(t, expectedQs, qs)
}

func (s *ExtractQueryStrTests) SubTestQueryStringThroughUnsupportedBinOp(t *testing.T) {
	argVal := &ssa.BinOp{
		Op: token.AND,
		X: &ssa.Const{
			Value: constant.MakeString("SELECT "),
		},
		Y: &ssa.Const{
			Value: constant.MakeString("id FROM table"),
		},
	}
	qs, err := extractQueryStrFromSsaValue(argVal)
	assert.Error(t, err)
	assert.Equal(t, "", qs)
}

func TestGoSource(t *testing.T) {
	gtest.RunSubTests(t, &ExtractQueryStrTests{})
}
