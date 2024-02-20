package pb

import (
	"encoding/json"
	"strings"
)

// RowMap returns a map representation of the row at the specified index in the
// RowSet.
//
// rowIndex: The index of the row to be encoded.
func (x *RowSet) RowMap(rowIndex int) map[string]interface{} {
	m := make(map[string]interface{}, len(x.Rows[rowIndex].Columns))
	for i, each := range x.Rows[rowIndex].Columns {
		key := x.ColumnSchemas[i].Name
		switch each.GetJsonValue().(type) {
		case *ColumnValue_StringValue:
			m[key] = each.GetStringValue()
		case *ColumnValue_NumberFloatValue:
			m[key] = each.GetNumberFloatValue()
		case *ColumnValue_NumberIntegerValue:
			m[key] = each.GetNumberIntegerValue()
		case *ColumnValue_ObjectValue:
			m[key] = each.GetObjectValue()
		case *ColumnValue_ArrayValue:
			m[key] = each.GetArrayValue()
		case *ColumnValue_BoolValue:
			m[key] = each.GetBoolValue()
		default:
			m[key] = nil
		}
	}
	return m
}

// JSONString returns a JSON-encoded string representation of the RowSet.
func (x *RowSet) JSONString() string {
	buf := new(strings.Builder)
	buf.WriteRune('[')
	enc := json.NewEncoder(buf)
	for r := range x.Rows {
		if r > 0 {
			buf.WriteRune(',')
		}
		x.encodeRowOn(r, enc, buf)
	}
	buf.WriteRune(']')
	return buf.String()
}

// RowJSONString returns a JSON-encoded string representation of a row at
// the specified index in the RowSet.
//
// index: The index of the row to be encoded.
//
// string: A string representation of the row in JSON format.
//
// options: The JSON output options.
func (x *RowSet) RowJSONString(index int) string {
	buf := new(strings.Builder)
	enc := json.NewEncoder(buf)
	x.encodeRowOn(index, enc, buf)
	return buf.String()
}

func (x *RowSet) encodeRowOn(rowIndex int, enc *json.Encoder, buf *strings.Builder) {
	row := x.Rows[rowIndex]
	buf.WriteRune('{')
	for c, other := range row.Columns {
		if c > 0 {
			buf.WriteRune(',')
		}
		buf.WriteRune('"')
		// assume no escaping needed for name
		buf.WriteString(x.ColumnSchemas[c].Name)
		buf.WriteString(`":`)
		switch other.GetJsonValue().(type) {
		case *ColumnValue_StringValue:
			enc.Encode(other.GetStringValue())
		case *ColumnValue_NumberFloatValue:
			enc.Encode(other.GetNumberFloatValue())
		case *ColumnValue_NumberIntegerValue:
			enc.Encode(other.GetNumberIntegerValue())
		case *ColumnValue_ObjectValue:
			buf.WriteString(other.GetObjectValue())
			buf.WriteRune('\n')
		case *ColumnValue_ArrayValue:
			buf.WriteString(other.GetArrayValue())
			buf.WriteRune('\n')
		case *ColumnValue_BoolValue:
			if other.GetBoolValue() {
				buf.WriteString("true\n")
			} else {
				buf.WriteString("false\n")
			}
		default:
			buf.WriteString("null\n")
		}
	}
	buf.WriteRune('}')
}
