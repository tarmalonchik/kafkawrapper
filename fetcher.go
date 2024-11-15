package kafka

import (
	"context"

	segmentio "github.com/segmentio/kafka-go"
)

type FetchMsg struct {
	Msg         []byte
	originalMsg segmentio.Message
}

type Fetcher struct {
	kaf     *segmentio.Reader
	channel chan FetchMsg
}

func newFetcher(ctx context.Context, kaf *segmentio.Reader) *Fetcher {
	rd := &Fetcher{
		kaf:     kaf,
		channel: make(chan FetchMsg),
	}
	go rd.runFetcher(ctx)
	return rd
}

func (w *Fetcher) runFetcher(ctx context.Context) {
	for {
		kafMsg, err := w.kaf.FetchMessage(ctx)
		if err != nil {
			continue
		}
		w.channel <- FetchMsg{
			Msg:         kafMsg.Value,
			originalMsg: kafMsg,
		}
	}
}

func (w *Fetcher) Close() error {
	close(w.channel)
	return w.kaf.Close()
}

func (w *Fetcher) CommitMessage(ctx context.Context, msg FetchMsg) error {
	return w.kaf.CommitMessages(ctx, msg.originalMsg)
}

func (w *Fetcher) GetFetcher() chan FetchMsg {
	return w.channel
}
