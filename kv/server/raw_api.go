package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := kvrpcpb.RawGetResponse{}
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &response, err
	}
	values, err := storageReader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &response, err
	}

	notFound := false
	if values == nil {
		notFound = true
	}
	response.Value = values
	response.NotFound = notFound
	return &response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	key, val, cf := req.Key, req.Value, req.Cf
	put := storage.Put{
		Key:   key,
		Value: val,
		Cf:    cf,
	}
	modify := storage.Modify{
		Data: put,
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	response := kvrpcpb.RawPutResponse{}
	if err != nil {
		return &response, err
	}
	return &response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	key, cf := req.Key, req.Cf
	del := storage.Delete{
		Key: key,
		Cf:  cf,
	}
	modify := storage.Modify{
		Data: del,
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	response := kvrpcpb.RawDeleteResponse{}
	if err != nil {
		return &response, err
	}
	return &response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := kvrpcpb.RawScanResponse{}
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &response, err
	}
	iter := storageReader.IterCF(req.Cf)
	defer iter.Close()

	iter.Seek(req.StartKey)
	var pairs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit) && iter.Valid(); i++ {
		item := iter.Item()
		val, _ := item.Value()
		pair := kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		}
		pairs = append(pairs, &pair)
		iter.Next()
	}
	response.Kvs = pairs

	return &response, nil
}
