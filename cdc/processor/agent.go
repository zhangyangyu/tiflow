// Copyright 2021 PingCAP, Inc.
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

package processor

import (
	stdContext "context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/scheduler"
	"github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/p2p"
	"github.com/pingcap/ticdc/pkg/version"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
)

// Agent is a data structure in the Processor that serves as a bridge with
// the Owner.
//
// Agent has a BaseAgent embedded in it, which handles the high-level logic of receiving
// commands from the Owner. It also implements ProcessorMessenger interface, which
// provides the BaseAgent with the necessary methods to send messages to the Owner.
//
// The reason for this design is to decouple the scheduling algorithm with the underlying
// RPC server/client.
//
// Note that Agent is not thread-safe, and it is not necessary for it to be thread-safe.
type processorAgent interface {
	scheduler.Agent
	scheduler.ProcessorMessenger
}

type agentImpl struct {
	*scheduler.BaseAgent

	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	changeFeed     model.ChangeFeedID
	ownerCaptureID model.CaptureID

	barrierSeqs map[p2p.Topic]p2p.Seq

	handlerErrChs []<-chan error
}

func newAgent(
	ctx context.Context,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	executor scheduler.TableExecutor,
	changeFeedID model.ChangeFeedID,
) (processorAgent, error) {
	ret := &agentImpl{
		messageServer: messageServer,
		messageRouter: messageRouter,

		changeFeed:  changeFeedID,
		barrierSeqs: map[p2p.Topic]p2p.Seq{},
	}
	ret.BaseAgent = scheduler.NewBaseAgent(
		changeFeedID,
		executor,
		ret,
		&scheduler.BaseAgentConfig{SendCheckpointTsInterval: time.Millisecond * 500})

	// Note that registerPeerMessageHandlers sets handlerErrChs.
	if err := ret.registerPeerMessageHandlers(ctx); err != nil {
		return nil, errors.Trace(err)
	}

	ownerCaptureID, err := ctx.GlobalVars().EtcdClient.GetOwnerID(ctx, etcd.CaptureOwnerKey)
	if err != nil {
		if err != concurrency.ErrElectionNoLeader {
			return nil, errors.Trace(err)
		}
		// We tolerate the situation where there is no owner.
		// If we are registered in Etcd, an elected Owner will have to
		// contact us before it can schedule any table.
	} else {
		ret.ownerCaptureID = ownerCaptureID
	}
	return ret, nil
}

func (a *agentImpl) Tick(ctx context.Context) error {
	for _, errCh := range a.handlerErrChs {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-errCh:
			log.Warn("Processor Agent received error from message handler",
				zap.Error(err))
			return errors.Trace(err)
		default:
		}
	}

	if err := a.BaseAgent.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (a *agentImpl) FinishTableOperation(
	ctx context.Context,
	tableID model.TableID,
) (bool, error) {
	done, err := a.trySendMessage(
		ctx, a.ownerCaptureID,
		model.DispatchTableResponseTopic(a.changeFeed),
		&model.DispatchTableResponseMessage{ID: tableID})
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

func (a *agentImpl) SyncTaskStatuses(
	ctx context.Context,
	running, adding, removing []model.TableID,
) (bool, error) {
	done, err := a.trySendMessage(
		ctx,
		a.ownerCaptureID,
		model.SyncTopic(a.changeFeed),
		&model.SyncMessage{
			ProcessorVersion: version.ReleaseSemver(),
			Running:          running,
			Adding:           adding,
			Removing:         removing,
		})
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

func (a *agentImpl) SendCheckpoint(
	ctx context.Context,
	checkpointTs model.Ts,
	resolvedTs model.Ts,
) (bool, error) {
	done, err := a.trySendMessage(
		ctx,
		a.ownerCaptureID,
		model.CheckpointTopic(a.changeFeed),
		&model.CheckpointMessage{
			CheckpointTs: checkpointTs,
			ResolvedTs:   resolvedTs,
		})
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

// Barrier returns whether there is a pending message not yet acknowledged by the owner.
// Please refer to the documentation on the ProcessorMessenger interface.
func (a *agentImpl) Barrier(ctx context.Context) (done bool) {
	if a.ownerCaptureID == "" {
		// We should wait for the first owner to contact us.
		// We need to wait for the sync request anyways, and
		// there would not be any table to replicate for now.
		log.Debug("waiting for owner to request sync",
			zap.String("changefeed-id", a.changeFeed))
		return false
	}

	client := a.messageRouter.GetClient(a.ownerCaptureID)
	if client == nil {
		// Client not found for owner.
		// Note that if the owner is eventually gone,
		// OnOwnerChanged will reset the barriers.
		return false
	}
	for topic, waitSeq := range a.barrierSeqs {
		actualSeq, ok := client.CurrentAck(topic)
		if !ok {
			return false
		}
		if actualSeq >= waitSeq {
			delete(a.barrierSeqs, topic)
		} else {
			return false
		}
	}
	return true
}

func (a *agentImpl) OnOwnerChanged(ctx context.Context, newOwnerCaptureID model.CaptureID) {
	a.ownerCaptureID = newOwnerCaptureID
	// Note that we clear the pending barriers.
	a.barrierSeqs = map[p2p.Topic]p2p.Seq{}
}

func (a *agentImpl) Close() error {
	log.Debug("processor messenger: closing", zap.Stack("stack"))
	ctx, cancel := stdContext.WithTimeout(stdContext.Background(), time.Second*1)
	defer cancel()

	// We create a fresh context for the clean-up.
	// There is no point is passing in a context because
	// when we have to close a processor, the context
	// is expected to have been canceled.
	cdcCtx := context.NewContext(ctx, nil)
	if err := a.deregisterPeerMessageHandlers(cdcCtx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (a *agentImpl) trySendMessage(
	ctx context.Context,
	target model.CaptureID,
	topic p2p.Topic,
	value interface{},
) (bool, error) {
	client := a.messageRouter.GetClient(target)
	if client == nil {
		log.Warn("processor: no message client found for owner, retry later",
			zap.String("owner", target))
		return false, nil
	}

	seq, err := client.TrySendMessage(ctx, topic, value)
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}

	a.barrierSeqs[topic] = seq
	return true, nil
}

//nolint:unused,deadcode
func (a *agentImpl) registerPeerMessageHandlers(ctx context.Context) (ret error) {
	defer func() {
		if ret != nil {
			if err := a.deregisterPeerMessageHandlers(ctx); err != nil {
				log.Error("failed to deregister handlers", zap.Error(err))
			}
		}
	}()

	errCh, err := a.messageServer.SyncAddHandler(
		ctx,
		model.DispatchTableTopic(a.changeFeed),
		&model.DispatchTableMessage{},
		func(sender string, value interface{}) error {
			ownerCapture := sender
			message := value.(*model.DispatchTableMessage)
			a.OnOwnerDispatchedTask(
				ownerCapture,
				message.OwnerRev,
				message.ID,
				message.IsDelete)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	a.handlerErrChs = append(a.handlerErrChs, errCh)

	errCh, err = a.messageServer.SyncAddHandler(
		ctx,
		model.AnnounceTopic(a.changeFeed),
		&model.AnnounceMessage{},
		func(sender string, value interface{}) error {
			ownerCapture := sender
			message := value.(*model.AnnounceMessage)
			a.OnOwnerAnnounce(
				ownerCapture,
				message.OwnerRev)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	a.handlerErrChs = append(a.handlerErrChs, errCh)
	return nil
}

func (a *agentImpl) deregisterPeerMessageHandlers(ctx context.Context) error {
	err := a.messageServer.SyncRemoveHandler(ctx, model.DispatchTableTopic(a.changeFeed))
	if err != nil {
		return errors.Trace(err)
	}

	err = a.messageServer.SyncRemoveHandler(ctx, model.AnnounceTopic(a.changeFeed))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
