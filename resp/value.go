package resp

// Value represents a RESP value (Simple Strings, Errors, Integers, Bulk Strings, Arrays).
type Value struct {
	Type  string
	Str   string
	Num   int
	Bulk  string
	Array []Value
}
