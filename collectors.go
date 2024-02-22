package anyrow

import (
	"encoding/json"

	"github.com/emicklei/anyrow/pb"
)

type valueCollector interface {
	nextRow(length int)
	storeDefault(index int, value any)
	storeBool(index int, value bool)
	storeString(index int, value string)
	storeFloat32(index int, value float32)
	storeInt64(index int, value int64)
}

type objectCollector struct {
	list   []Object
	object Object
	set    *pb.RowSet
}

func (o *objectCollector) storeDefault(index int, value any) {
	o.object[o.set.ColumnSchemas[index].Name] = value
}
func (o *objectCollector) storeBool(index int, value bool) {
	o.object[o.set.ColumnSchemas[index].Name] = value
}
func (o *objectCollector) storeString(index int, value string) {
	o.object[o.set.ColumnSchemas[index].Name] = value
}
func (o *objectCollector) storeFloat32(index int, value float32) {
	o.object[o.set.ColumnSchemas[index].Name] = value
}
func (o *objectCollector) storeInt64(index int, value int64) {
	o.object[o.set.ColumnSchemas[index].Name] = value
}

func (o *objectCollector) nextRow(length int) {
	o.object = make(map[string]any, length)
	o.list = append(o.list, o.object)
}

type rowsetCollector struct {
	set *pb.RowSet
	row *pb.Row
}

func (r *rowsetCollector) storeDefault(index int, value any) {
	data, _ := json.Marshal(value)
	cell := new(pb.ColumnValue)
	cell.JsonValue = &pb.ColumnValue_ObjectValue{ObjectValue: string(data)}
	r.row.Columns[index] = cell
}
func (r *rowsetCollector) storeBool(index int, value bool) {
	cell := new(pb.ColumnValue)
	cell.JsonValue = &pb.ColumnValue_BoolValue{BoolValue: value}
	r.row.Columns[index] = cell
}
func (r *rowsetCollector) storeString(index int, value string) {
	cell := new(pb.ColumnValue)
	cell.JsonValue = &pb.ColumnValue_StringValue{StringValue: value}
	r.row.Columns[index] = cell
}
func (r *rowsetCollector) storeFloat32(index int, value float32) {
	cell := new(pb.ColumnValue)
	cell.JsonValue = &pb.ColumnValue_NumberFloatValue{NumberFloatValue: value}
	r.row.Columns[index] = cell
}
func (r *rowsetCollector) storeInt64(index int, value int64) {
	cell := new(pb.ColumnValue)
	cell.JsonValue = &pb.ColumnValue_NumberIntegerValue{NumberIntegerValue: value}
	r.row.Columns[index] = cell
}

func (r *rowsetCollector) nextRow(length int) {
	r.row = new(pb.Row)
	r.row.Columns = make([]*pb.ColumnValue, length)
	r.set.Rows = append(r.set.Rows, r.row)
}
