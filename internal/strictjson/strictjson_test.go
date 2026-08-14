package strictjson

import (
	"errors"
	"strings"
	"testing"
)

func TestRejectDuplicateNamesAtEveryObjectDepth(t *testing.T) {
	t.Parallel()

	tests := []string{
		`{"schema":1,"schema":2}`,
		`{"outer":{"name":1,"name":2}}`,
		`[{"name":1,"n\u0061me":2}]`,
	}
	for _, document := range tests {
		if err := RejectDuplicateNames([]byte(document)); !errors.Is(err, ErrDuplicateName) {
			t.Errorf("RejectDuplicateNames(%s) error = %v", document, err)
		}
	}
}

func TestRejectDuplicateNamesAcceptsNamesRepeatedInDifferentObjects(t *testing.T) {
	t.Parallel()

	document := `{"left":{"name":1},"right":{"name":2}} trailing input`
	if err := RejectDuplicateNames([]byte(document)); err != nil {
		t.Fatalf("RejectDuplicateNames: %v", err)
	}
}

func TestRejectDuplicateNamesBoundsNesting(t *testing.T) {
	t.Parallel()

	document := strings.Repeat("[", maxNestingDepth+1) + "0" + strings.Repeat("]", maxNestingDepth+1)
	if err := RejectDuplicateNames([]byte(document)); !errors.Is(err, ErrNestingTooDeep) {
		t.Fatalf("nesting error = %v", err)
	}
}
