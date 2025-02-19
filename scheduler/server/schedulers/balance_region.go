// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	maxDownTime := cluster.GetMaxStoreDownTime()
	suitableStores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < maxDownTime {
			suitableStores = append(suitableStores, store)
		}
	}
	if len(suitableStores) <= 1 {
		return nil
	}
	// sort suitable stores by region size in descending order
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})
	// find suitable region
	var movedRegion *core.RegionInfo
	var sourceStore, targetStore *core.StoreInfo
	cb := func(container core.RegionsContainer) {
		movedRegion = container.RandomRegion([]byte{}, []byte{})
	}
	for _, store := range suitableStores {
		// search pending region first
		cluster.GetPendingRegionsWithLock(store.GetID(), cb)
		if movedRegion != nil {
			if len(movedRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
				movedRegion = nil
			} else {
				sourceStore = store
				break
			}
		}
		// search follower region then
		cluster.GetFollowersWithLock(store.GetID(), cb)
		if movedRegion != nil {
			if len(movedRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
				movedRegion = nil
			} else {
				sourceStore = store
				break
			}
		}
		// search leader region at last
		cluster.GetLeadersWithLock(store.GetID(), cb)
		if movedRegion != nil {
			if len(movedRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
				movedRegion = nil
			} else {
				sourceStore = store
				break
			}
		}
	}
	if movedRegion == nil {
		return nil
	}
	// find target store
	for i := len(suitableStores) - 1; i >= 0; i-- {
		if movedRegion.GetStorePeer(suitableStores[i].GetID()) == nil &&
			sourceStore.GetRegionSize()-suitableStores[i].GetRegionSize() > 2*movedRegion.GetApproximateSize() {
			targetStore = suitableStores[i]
			break
		}
	}
	if targetStore == nil {
		return nil
	}

	// create new peer and operator
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		panic(err)
	}
	op, err := operator.CreateMovePeerOperator("balance-region", cluster, movedRegion, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), newPeer.GetId())
	if err != nil {
		panic(err)
	}
	return op
}
