package httpcodec

import "strings"

// Header is a single header field preserving the original name case.
type Header struct {
	Name  string
	Value string
}

// Headers is an ordered list of header fields. Lookup is case-insensitive;
// iteration order is the insertion (wire) order.
type Headers struct {
	fields []Header
}

// Add appends a header field, preserving duplicates and order.
func (h *Headers) Add(name, value string) {
	h.fields = append(h.fields, Header{Name: name, Value: value})
}

// Get returns the first value for name (case-insensitive) and whether it exists.
func (h *Headers) Get(name string) (string, bool) {
	for _, f := range h.fields {
		if strings.EqualFold(f.Name, name) {
			return f.Value, true
		}
	}
	return "", false
}

// Values returns all values for name in wire order.
func (h *Headers) Values(name string) []string {
	var out []string
	for _, f := range h.fields {
		if strings.EqualFold(f.Name, name) {
			out = append(out, f.Value)
		}
	}
	return out
}

// Len returns the number of header fields.
func (h *Headers) Len() int { return len(h.fields) }

// At returns the i-th header field.
func (h *Headers) At(i int) Header { return h.fields[i] }

// All returns a copy of all fields in order.
func (h *Headers) All() []Header {
	out := make([]Header, len(h.fields))
	copy(out, h.fields)
	return out
}

// Equal reports whether two header lists contain the same fields in the
// same order, comparing names case-insensitively.
func (h *Headers) Equal(o *Headers) bool {
	if h == nil || o == nil {
		return h == o
	}
	if len(h.fields) != len(o.fields) {
		return false
	}
	for i := range h.fields {
		if !strings.EqualFold(h.fields[i].Name, o.fields[i].Name) ||
			h.fields[i].Value != o.fields[i].Value {
			return false
		}
	}
	return true
}
