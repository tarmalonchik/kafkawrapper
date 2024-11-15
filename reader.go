package kafka

import (
	"context"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

type Msg struct {
	Err error
	Msg []byte
}

type Reader struct {
	kaf     *segmentio.Reader
	channel chan Msg
}

func newReader(ctx context.Context, kaf *segmentio.Reader) *Reader {
	rd := &Reader{
		kaf:     kaf,
		channel: make(chan Msg),
	}
	go rd.runReader(ctx)
	return rd
}

func (w *Reader) runReader(ctx context.Context) {
	for {
		kafMsg, err := w.kaf.ReadMessage(ctx)
		w.channel <- Msg{
			Msg: kafMsg.Value,
			Err: err,
		}
	}
}

func (w *Reader) Close() error {
	close(w.channel)
	return w.kaf.Close()
}

func (w *Reader) SetOffsetAt(ctx context.Context, t time.Time) error {
	return w.kaf.SetOffsetAt(ctx, t)
}

func (w *Reader) GetReader() chan Msg {
	return w.channel
}
