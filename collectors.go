package anyrow

import "github.com/emicklei/anyrow/pb"

type valueCollector interface {
	nextRow(length int)
	storeDefault(index int, value any)
	storeBool(index int, value bool)
	storeString(index int, value string)
	storeFloat32(index int, value float32)
	storeInt64(index int, value int64)
}

type objectCollector struct {
	target Object
}

func (o objectCollector) storeDefault(index int, value any)     {}
func (o objectCollector) storeBool(index int, value bool)       {}
func (o objectCollector) storeString(index int, value string)   {}
func (o objectCollector) storeFloat32(index int, value float32) {}
func (o objectCollector) storeInt64(index int, value int64)     {}

type rowsetCollector struct {
	target *pb.RowSet
}
