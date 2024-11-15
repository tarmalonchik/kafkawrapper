package kafka

import (
	"context"

	segmentio "github.com/segmentio/kafka-go"
)

type Writer struct {
	kaf *segmentio.Writer
}

func newWriter(kaf *segmentio.Writer) *Writer {
	return &Writer{
		kaf: kaf,
	}
}

func (w *Writer) Close() error {
	return w.kaf.Close()
}

func (w *Writer) Write(ctx context.Context, msg []byte) error {
	return w.kaf.WriteMessages(ctx, []segmentio.Message{{Value: msg}}...)
}
