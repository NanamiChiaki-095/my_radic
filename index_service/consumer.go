package index_service

import (
	"my_radic/internal/indexer"
	"my_radic/types"
	"my_radic/util"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

type ConsumerHandler struct {
	indexer *indexer.Indexer
}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const (
		batchSize    = 100
		batchTimeout = 500 * time.Millisecond
	)

	var (
		batchDocs []*types.Document
		batchMsgs []*sarama.ConsumerMessage
		timer     = time.NewTimer(batchTimeout)
	)
	defer timer.Stop()

	flush := func() {
		if len(batchDocs) == 0 {
			return
		}
		docIds, err := h.indexer.BatchAddDocument(batchDocs)
		if err != nil {
			util.LogError("ConsumeClaim BatchAddDocument err: %v", err)
		} else {
			util.LogInfo("ConsumeClaim BatchAddDocument success, docIds: %v", docIds)
		}
		for _, msg := range batchMsgs {
			session.MarkMessage(msg, "")
		}
		batchDocs = batchDocs[:0]
		batchMsgs = batchMsgs[:0]
	}
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				flush()
				return nil
			}
			var doc types.Document
			if err := proto.Unmarshal(msg.Value, &doc); err != nil {
				util.LogError("ConsumeClaim Unmarshal err: %v", err)
				session.MarkMessage(msg, "")
				continue
			}
			batchDocs = append(batchDocs, &doc)
			batchMsgs = append(batchMsgs, msg)
			if len(batchDocs) >= batchSize {
				flush()
				timer.Reset(batchTimeout)
			}
		case <-timer.C:
			flush()
			timer.Reset(batchTimeout)
		case <-session.Context().Done():
			flush()
			return nil
		}
	}
}
