package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	keys = append(keys, req.Key)
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	response := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	// check is key locked after txn start
	lock, err := mvccTxn.GetLock(req.Key)
	if err != nil {
		return response, err
	}
	if lock != nil && lock.Ts <= req.Version {
		response.Error = &kvrpcpb.KeyError{Locked: lock.Info(req.Key)}
		return response, nil
	}

	value, err := mvccTxn.GetValue(req.Key)
	if err != nil {
		return response, err
	}
	if value == nil {
		response.NotFound = true
	} else {
		response.Value = value
	}
	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	response := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// check w-w conflict first
	for _, key := range keys {
		// read the most recent write to check w-w conflict
		write, commitTs, err := mvccTxn.MostRecentWrite(key)
		if err != nil {
			return response, err
		}
		// TODO commitTs == req.StartVersion is OK ?
		if write != nil && commitTs >= req.StartVersion {
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    write.StartTS,
				ConflictTs: commitTs,
				Key:        key,
				Primary:    req.PrimaryLock,
			}})
			return response, nil
		}
	}

	// check is key locked after txn start
	for _, key := range keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return response, err
		}
		if lock != nil {
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{Locked: lock.Info(key)})
			return response, nil
		}
		// TODO
		mvccTxn.PutLock(key, &mvcc.Lock{Primary: req.PrimaryLock, Ts: mvccTxn.StartTS, Ttl: req.LockTtl, Kind: mvcc.WriteKindFromProto(req.Op)})
	}

	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
