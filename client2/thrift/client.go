package thrift

import (
	"context"
	"errors"
	"net"
	"strings"

	hb "github.com/chennqqi/thrift-hbase/hbase-thrift2"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var defaultCtx = context.Background()

type HbaseClient struct {
	*hb.THBaseServiceClient
	trans *thrift.TSocket
}

func Open(host, port string) Factory {
	return func() (HbaseClient, error) {
		ret := HbaseClient{}
		trans, err := thrift.NewTSocket(net.JoinHostPort(host, port))
		if err != nil {
			return ret, err
		}
		client := hb.NewTHBaseServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		err = trans.Open()
		ret.THBaseServiceClient = client
		ret.trans = trans
		return ret, err
	}
}

func (h *HbaseClient) Put(tableName, rowKey string, data map[string][]byte) error {
	if len(tableName) <= 0 || len(rowKey) <= 0 {
		return errors.New("tableName or rowKey is nil")
	}
	if len(data) <= 0 {
		return errors.New("data is nil")
	}
	tp := hb.NewTPut()
	tp.Row = []byte(rowKey)
	cols := make([]*hb.TColumnValue, len(data))
	var idx int
	for k, v := range data {
		keys := strings.Split(k, ":")
		col := hb.NewTColumnValue()
		hb.NewTColumnValue()

		col.Family = []byte(keys[0])
		col.Qualifier = []byte(keys[1])
		col.Value = v
		cols[idx] = col
		idx++
	}
	tp.ColumnValues = tp.ColumnValues
	return h.THBaseServiceClient.Put(context.Background(), []byte(tableName), tp)
}
