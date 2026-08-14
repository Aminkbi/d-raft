// Package strictjson provides lexical checks that encoding/json's typed
// decoder does not perform itself.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const maxNestingDepth = 10_000

var (
	// ErrDuplicateName reports two names in the same JSON object after escape
	// processing. For example, "name" and "n\u0061me" are duplicates.
	ErrDuplicateName = errors.New("strictjson: duplicate object name")
	// ErrNestingTooDeep prevents adversarial documents from driving unbounded
	// recursive descent independently of their byte-size limit.
	ErrNestingTooDeep = errors.New("strictjson: maximum nesting depth exceeded")
)

// RejectDuplicateNames walks the first JSON value and rejects a repeated name
// in any object. Callers retain responsibility for byte limits, typed decoding,
// unknown fields, and trailing-value rejection.
func RejectDuplicateNames(document []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	return scanValue(decoder, 0)
}

func scanValue(decoder *json.Decoder, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	if depth >= maxNestingDepth {
		return ErrNestingTooDeep
	}

	switch delimiter {
	case '{':
		names := make(map[string]struct{})
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := nameToken.(string)
			if !ok {
				return errors.New("strictjson: object name is not a string")
			}
			if _, duplicate := names[name]; duplicate {
				return fmt.Errorf("%w %q", ErrDuplicateName, name)
			}
			names[name] = struct{}{}
			if err := scanValue(decoder, depth+1); err != nil {
				return err
			}
		}
		return consumeClosing(decoder, '}')
	case '[':
		for decoder.More() {
			if err := scanValue(decoder, depth+1); err != nil {
				return err
			}
		}
		return consumeClosing(decoder, ']')
	default:
		return fmt.Errorf("strictjson: unexpected delimiter %q", delimiter)
	}
}

func consumeClosing(decoder *json.Decoder, expected json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != expected {
		return fmt.Errorf("strictjson: expected closing delimiter %q", expected)
	}
	return nil
}
