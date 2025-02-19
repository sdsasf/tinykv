package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()
		// save all ready states except apply committed entries
		applySnapRet, err := d.peer.peerStorage.SaveReadyState(&ready)
		if err != nil {
			panic(err)
		}
		// TODO clean up state because snapshot
		if applySnapRet != nil {
			if !reflect.DeepEqual(applySnapRet.PrevRegion, applySnapRet.Region) {
				d.peerStorage.SetRegion(applySnapRet.Region)
				d.ctx.storeMeta.Lock()
				d.ctx.storeMeta.regions[applySnapRet.Region.Id] = applySnapRet.Region
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapRet.PrevRegion})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapRet.Region})
				d.ctx.storeMeta.Unlock()
			}
		}

		// send messages to other peers
		if len(ready.Messages) > 0 {
			d.peer.Send(d.ctx.trans, ready.Messages)
		}
		// apply committed but not applied entries
		for _, entry := range ready.CommittedEntries {
			// update applied index before judge d.stopped, otherwise it may overwrite cleared metadata
			kvWb := new(engine_util.WriteBatch)
			d.applyEntry(&entry, kvWb)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			if d.stopped {
				// if stopped, it means the peer is destroyed when applyEntry and clear all data in engines
				// so we don't write other data to engines
				return
			}
			// update raftStorage.applyState
			if err := kvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
				panic(err)
			}
			// write to kvdb
			if err := kvWb.WriteToDB(d.ctx.engine.Kv); err != nil {
				panic(err)
			}
		}
		// advance the raft state
		d.RaftGroup.Advance(ready)
	}
}

// applyEntry applies committed entries to the kvdb
func (d *peerMsgHandler) applyEntry(entry *eraftpb.Entry, kvWb *engine_util.WriteBatch) {
	switch entry.EntryType {
	case eraftpb.EntryType_EntryNormal:
		// only data > 0 is valid, empty data is used for 2b test
		if len(entry.Data) > 0 {
			req := new(raft_cmdpb.RaftCmdRequest)
			if err := req.Unmarshal(entry.Data); err != nil {
				panic(err)
			}

			// check region epoch
			if !d.checkRegionEpoch(req) {
				response := ErrResp(&util.ErrEpochNotMatch{})
				d.processCallback(response, entry.Index, entry.Term)
				return
			}

			// handle admin request
			if req.AdminRequest != nil {
				// transferLeader will not propose to raft group
				switch req.AdminRequest.CmdType {
				case raft_cmdpb.AdminCmdType_CompactLog:
					d.execCompactLog(req.AdminRequest, kvWb, entry.Index, entry.Term)
				case raft_cmdpb.AdminCmdType_Split:
					d.execSplitRegion(req, kvWb, entry.Index, entry.Term)
				}
			}

			// handle first normal request
			if len(req.Requests) > 0 {
				cmd := req.Requests[0]
				switch cmd.CmdType {
				case raft_cmdpb.CmdType_Get:
					d.execGet(cmd, entry.Index, entry.Term)
				case raft_cmdpb.CmdType_Put:
					d.execPut(cmd, kvWb, entry.Index, entry.Term)
				case raft_cmdpb.CmdType_Delete:
					d.execDelete(cmd, kvWb, entry.Index, entry.Term)
				case raft_cmdpb.CmdType_Snap:
					d.execSnap(entry.Index, entry.Term)
				}
			}
		}
	case eraftpb.EntryType_EntryConfChange:
		// TODO handle conf change
		newConf := new(eraftpb.ConfChange)
		// entry.Data is pb.ConfChange
		if err := newConf.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
		context := new(raft_cmdpb.RaftCmdRequest)
		if err := context.Unmarshal(newConf.Context); err != nil {
			panic(err)
		}
		// is region epoch valid ?
		if !d.checkRegionEpoch(context) {
			response := ErrResp(&util.ErrEpochNotMatch{})
			d.processCallback(response, entry.Index, entry.Term)
			return
		}
		d.execConfChange(newConf, context, kvWb, entry.Index, entry.Term)
	}
}

func (d *peerMsgHandler) execGet(cmd *raft_cmdpb.Request, index uint64, term uint64) {
	val, _ := engine_util.GetCF(d.ctx.engine.Kv, cmd.Get.Cf, cmd.Get.Key)
	getResponse := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     &raft_cmdpb.GetResponse{Value: val},
			},
		},
	}
	d.processCallback(getResponse, index, term)
}

func (d *peerMsgHandler) execPut(cmd *raft_cmdpb.Request, kvWb *engine_util.WriteBatch, index uint64, term uint64) {
	kvWb.SetCF(cmd.Put.Cf, cmd.Put.Key, cmd.Put.Value)
	// update sizeDiffHint
	d.SizeDiffHint += uint64(len(cmd.Put.Key) + len(cmd.Put.Value))
	putResponse := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			},
		},
	}
	d.processCallback(putResponse, index, term)
}

func (d *peerMsgHandler) execDelete(cmd *raft_cmdpb.Request, kvWb *engine_util.WriteBatch, index uint64, term uint64) {
	kvWb.DeleteCF(cmd.Delete.Cf, cmd.Delete.Key)
	// TODO why only minus key size ?
	d.SizeDiffHint -= uint64(len(cmd.Delete.Key))
	deleteReponse := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			},
		},
	}
	d.processCallback(deleteReponse, index, term)
}

func (d *peerMsgHandler) execSnap(index uint64, term uint64) {
	// should clone region snapshot and send it to client in case of region changed later
	regionSnap := new(metapb.Region)
	if err := util.CloneMsg(d.Region(), regionSnap); err != nil {
		panic(err)
	}
	snapResponse := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{
			{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: regionSnap},
			},
		},
	}
	// only leader need process callback ?
	d.processCallback(snapResponse, index, term)
}

func (d *peerMsgHandler) execCompactLog(cmd *raft_cmdpb.AdminRequest, kvWb *engine_util.WriteBatch, index uint64, term uint64) {
	// compact log
	compactIndex := cmd.CompactLog.CompactIndex
	compactTerm := cmd.CompactLog.CompactTerm
	if compactIndex > d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index = compactIndex
		d.peerStorage.applyState.TruncatedState.Term = compactTerm
		// write apply state to kvdb
		if err := kvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			panic(err)
		}
		// schedule compactGC log task
		d.ScheduleCompactLog(compactIndex)
	}
	// response
	compactResponse := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		},
	}
	d.processCallback(compactResponse, index, term)
}

func (d *peerMsgHandler) execConfChange(newConf *eraftpb.ConfChange, context *raft_cmdpb.RaftCmdRequest,
	kvWb *engine_util.WriteBatch, index uint64, term uint64) {
	// modify raft conf
	d.RaftGroup.ApplyConfChange(*newConf)
	if newConf.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		if newConf.NodeId == d.PeerId() {
			// delete self
			// clear apply state
			//kvWb.DeleteMeta(meta.ApplyStateKey(d.regionId))
			d.destroyPeer()
		} else if d.isPeerExist(newConf.NodeId) {
			// removed peer is in other node, modify metadata in this node
			d.ctx.storeMeta.Lock()
			// modify region epoch, confVer++ for add/remove peer
			d.Region().RegionEpoch.ConfVer++
			// modify region.peer
			for i, peer := range d.Region().Peers {
				if peer.Id == newConf.NodeId {
					d.Region().Peers = append(d.Region().Peers[:i], d.Region().Peers[i+1:]...)
					break
				}
			}
			// write new regionLocalState to engines (update confVer)
			meta.WriteRegionState(kvWb, d.Region(), rspb.PeerState_Normal)
			// remove peer from cache
			d.removePeerCache(newConf.NodeId)
			d.ctx.storeMeta.Unlock()
		}
	} else {
		if !d.isPeerExist(newConf.NodeId) {
			// add peer
			d.ctx.storeMeta.Lock()
			// modify region epoch, confVer++ for add/remove peer
			d.Region().RegionEpoch.ConfVer++
			// modify region.peer
			peer := &metapb.Peer{Id: newConf.NodeId, StoreId: context.AdminRequest.ChangePeer.Peer.StoreId}
			d.Region().Peers = append(d.Region().Peers, peer)
			// write new regionLocalState to engines (update confVer)
			meta.WriteRegionState(kvWb, d.Region(), rspb.PeerState_Normal)
			// add peer to cache
			d.insertPeerCache(context.AdminRequest.ChangePeer.Peer)
			// update store meta
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.Unlock()
		}
	}
	response := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		},
	}
	d.processCallback(response, index, term)
	// TODO flush heartbeat scheduler
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
}

func (d *peerMsgHandler) execSplitRegion(req *raft_cmdpb.RaftCmdRequest, kvWb *engine_util.WriteBatch,
	index uint64, term uint64) {
	// region id not match
	if d.regionId != req.Header.RegionId {
		response := ErrResp(&util.ErrRegionNotFound{RegionId: req.Header.RegionId})
		d.processCallback(response, index, term)
		return
	}
	// check region epoch
	if !d.checkRegionEpoch(req) {
		response := ErrResp(&util.ErrEpochNotMatch{})
		d.processCallback(response, index, term)
		return
	}
	// check key in region
	if err := util.CheckKeyInRegion(req.AdminRequest.Split.SplitKey, d.Region()); err != nil {
		response := ErrResp(err)
		d.processCallback(response, index, term)
		return
	}
	// check peer number equal
	if len(req.AdminRequest.Split.NewPeerIds) != len(d.Region().Peers) {
		response := ErrResp(errors.Errorf("invalid new peer count %d, need %d",
			len(req.AdminRequest.Split.NewPeerIds), len(d.Region().Peers)))
		d.processCallback(response, index, term)
	}

	splitReq := req.AdminRequest.Split
	// copy region peer metas with new peer ids
	newPeerMetas := make([]*metapb.Peer, 0, len(d.Region().Peers))
	for i, peer := range d.Region().Peers {
		newPeerMetas = append(newPeerMetas, &metapb.Peer{
			Id:      splitReq.NewPeerIds[i],
			StoreId: peer.StoreId,
		})
	}
	// create new region
	newRegion := &metapb.Region{
		Id:       splitReq.NewRegionId,
		StartKey: splitReq.SplitKey,
		EndKey:   d.Region().EndKey,
		Peers:    newPeerMetas,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 0,
			Version: 0,
		},
	}
	// update region epoch
	d.Region().RegionEpoch.Version++
	newRegion.RegionEpoch.Version++
	d.Region().EndKey = splitReq.SplitKey
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regions[newRegion.Id] = newRegion
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	// write regionLocalState to engines
	meta.WriteRegionState(kvWb, d.Region(), rspb.PeerState_Normal)
	meta.WriteRegionState(kvWb, newRegion, rspb.PeerState_Normal)
	d.ctx.storeMeta.Unlock()
	// create new peer and register to router
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		panic(err)
	}
	newPeer.SetRegion(newRegion)
	d.ctx.router.register(newPeer)
	// start new peer
	startMsg := message.Msg{
		Type:     message.MsgTypeStart,
		RegionID: newRegion.Id,
	}
	if err := d.ctx.router.send(newRegion.Id, startMsg); err != nil {
		panic(err)
	}
	// update peer size and diffHint
	d.ApproximateSize = new(uint64)
	d.SizeDiffHint = 0
	newPeer.ApproximateSize = new(uint64)
	newPeer.SizeDiffHint = 0
	// process proposal
	splitResponse := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{d.Region(), newRegion},
			},
		},
	}
	d.processCallback(splitResponse, index, term)

	// flush cache of heartbeat scheduler
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
	d.notifyHeartbeatScheduler(newRegion, newPeer)
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) isPeerExist(peerID uint64) bool {
	for _, peer := range d.Region().Peers {
		if peer.Id == peerID {
			return true
		}
	}
	return false
}

// checkRegionEpoch checks whether the regionEpoch of the request is stale
func (d *peerMsgHandler) checkRegionEpoch(req *raft_cmdpb.RaftCmdRequest) bool {
	if req.Header != nil {
		fromEpoch := req.Header.RegionEpoch
		if fromEpoch != nil && util.IsEpochStale(fromEpoch, d.Region().RegionEpoch) {
			return false
		}
	}
	return true
}

func (d *peerMsgHandler) processCallback(resp *raft_cmdpb.RaftCmdResponse, index uint64, term uint64) {
	// clean up stale proposals
	d.cleanStaleIndexProposals(index)
	if len(d.proposals) == 0 {
		return
	}
	p := d.proposals[0]
	if p.index != index {
		//fmt.Printf("this shouldn't happen, because index should continue\n")
		return
	}
	// if index is equal but term is not equal
	if p.term != term {
		if p.cb != nil {
			// make error response header
			errResponse := ErrRespStaleCommand(term)
			// make error response
			errResponse.Responses = resp.Responses
			p.cb.Done(errResponse)
		}
		d.proposals = d.proposals[1:]
		return
	}
	// now index and term is match
	if len(resp.Responses) > 0 && resp.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
		// create a new transaction for snap command
		if p.cb != nil {
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
	}
	if p.cb != nil {
		p.cb.Done(resp)
	}
	d.proposals = d.proposals[1:]
}

// cleans up proposals that index is less than the given entry index
func (d *peerMsgHandler) cleanStaleIndexProposals(index uint64) {
	first, end := 0, len(d.proposals)
	for first < end && d.proposals[first].index < index {
		if d.proposals[first].cb != nil {
			d.proposals[first].cb.Done(ErrResp(&util.ErrStaleCommand{}))
		}
		first++
	}
	if first == end {
		// no proposal ?
		// TODO error handling
		d.proposals = make([]*proposal, 0)
		return
	}
	d.proposals = d.proposals[first:]
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// propose normal request
	for len(msg.Requests) > 0 {
		req := msg.Requests[0]
		if req.CmdType != raft_cmdpb.CmdType_Snap {
			// check key in region, should ignore snap command
			var key []byte
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				key = req.Get.Key
			case raft_cmdpb.CmdType_Put:
				key = req.Put.Key
			case raft_cmdpb.CmdType_Delete:
				key = req.Delete.Key
			}
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				msg.Requests = msg.Requests[1:]
				continue
			}
		}

		// should marshal msg which include admin request and normal request
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}

		nextproposalIndex := d.nextProposalIndex()
		// send raft command to raft group
		err = d.RaftGroup.Propose(data)
		if err != nil {
			if err != raft.ErrProposalDropped {
				panic(err)
			}
		} else {
			// add proposal
			d.proposals = append(d.proposals, &proposal{
				index: nextproposalIndex,
				term:  d.Term(),
				cb:    cb,
			})
		}
		msg.Requests = msg.Requests[1:]
	}
	// propose admin request
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}
			// add proposal
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				// compactLog request's callback is nil
				cb: cb,
			})
			if err := d.RaftGroup.Propose(data); err != nil {
				panic(err)
			}
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			transferLeaderResponse := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			}
			cb.Done(transferLeaderResponse)
		case raft_cmdpb.AdminCmdType_ChangePeer:
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}
			confChange := eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				// Context is raft_cmdpb.RaftCmdRequest
				Context: data,
			}

			if len(d.RaftGroup.Raft.Prs) == 2 && confChange.ChangeType == eraftpb.ConfChangeType_RemoveNode &&
				confChange.NodeId == d.RaftGroup.Raft.Lead && d.RaftGroup.Raft.State == raft.StateLeader {
				// if only two nodes in raft group and remove leader, transfer leader first
				// transfer leader to the other node
				for id := range d.RaftGroup.Raft.Prs {
					if id != d.RaftGroup.Raft.Lead {
						d.RaftGroup.TransferLeader(id)
						cb.Done(ErrResp(&util.ErrStaleCommand{}))
						return
					}
				}
			}

			if err := d.RaftGroup.ProposeConfChange(confChange); err != nil {
				// TODO error handling
				cb.Done(ErrResp(&util.ErrStaleCommand{}))
			}
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})
		case raft_cmdpb.AdminCmdType_Split:
			key := msg.AdminRequest.Split.SplitKey
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				return
			}
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}
			if err := d.RaftGroup.Propose(data); err != nil {
				panic(err)
			}
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})
		}
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
