package transformmessages

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// maxSafeInteger is the largest integer a JavaScript number holds exactly.
const maxSafeInteger = 1 << 53

// exact returns decoded with its document re-read so that an integer beyond
// 2^53 reaches the scripts as a BigInt rather than a rounded double, and
// reports whether it held one.
//
// A search can afford a rounded id; a repair cannot. Passing a document through
// untouched must write the same numbers back, and a 64-bit order id rounded to
// the nearest double names a different order.
func exact(decoded serde.Decoded) (serde.Decoded, bool) {
	if decoded.Document == nil {
		return decoded, false
	}

	parser := json.NewDecoder(strings.NewReader(decoded.Text))
	parser.UseNumber()

	var document any
	if err := parser.Decode(&document); err != nil {
		return decoded, false
	}

	big := false
	decoded.Document = exactNumbers(document, &big)

	return decoded, big
}

func exactNumbers(value any, big *bool) any {
	switch typed := value.(type) {
	case json.Number:
		return exactNumber(typed, big)

	case map[string]any:
		for key, child := range typed {
			typed[key] = exactNumbers(child, big)
		}

	case []any:
		for index, child := range typed {
			typed[index] = exactNumbers(child, big)
		}
	}

	return value
}

func exactNumber(number json.Number, isBig *bool) any {
	text := number.String()

	if !strings.ContainsAny(text, ".eE") {
		if integer, err := number.Int64(); err == nil && integer >= -maxSafeInteger && integer <= maxSafeInteger {
			return integer
		}

		if integer, ok := new(big.Int).SetString(text, 10); ok {
			*isBig = true

			return integer
		}
	}

	float, _ := number.Float64()

	return float
}

// checkRounded refuses a returned document that holds an integer-valued double
// beyond 2^53, which is what a BigInt becomes after arithmetic with a number or
// a call to Number(). Writing it would replace an exact id with a rounded one.
func checkRounded(value any, path string) error {
	switch typed := value.(type) {
	case float64:
		if typed == math.Trunc(typed) && math.Abs(typed) > maxSafeInteger {
			return fmt.Errorf(
				"transform returned %s = %.0f, an integer too large for a JavaScript number to hold exactly, so it was rounded. The original holds it as a BigInt: pass it through untouched, compare it with a BigInt such as 123n, or convert it with String()",
				path, typed)
		}

	case map[string]any:
		for key, child := range typed {
			if err := checkRounded(child, path+"."+key); err != nil {
				return err
			}
		}

	case []any:
		for index, child := range typed {
			if err := checkRounded(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}

	return nil
}
