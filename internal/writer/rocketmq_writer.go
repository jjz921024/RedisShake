package writer

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"context"
	"fmt"
	"strings"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type RocketMQWriterOptions struct {
	NamesrvAddress []string `mapstructure:"namesrv_address" default:""`
	Username       string   `mapstructure:"username" default:""`
	Password       string   `mapstructure:"password" default:""`
	Topic          string   `mapstructure:"topic" default:""`
}

type rocketMQWriter struct {
	producer rocketmq.Producer
	opt      *RocketMQWriterOptions

	stat struct {
		Name string `json:"name"`
	}
}

func NewRocketMQWriter(opt *RocketMQWriterOptions) Writer {
	rlog.SetLogger(log.LogAdapter)
	producer, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver(opt.NamesrvAddress)),
		producer.WithRetry(2),
		producer.WithQueueSelector(producer.NewHashQueueSelector()),
	)
	if err != nil {
		log.Panicf("create rocketmq writer err: %s", err.Error())
	}
	if err := producer.Start(); err != nil {
		log.Panicf("start rocketmq writer err: %s", err.Error())
	}

	writer := &rocketMQWriter{
		opt:      opt,
		producer: producer,
	}
	writer.stat.Name = "rocketmq_writer"
	return writer
}

func (w *rocketMQWriter) Write(e *entry.Entry) {
	msg := primitive.NewMessage(w.opt.Topic, []byte(strings.Join(e.Argv, " ")))
	// select queue by key
	msg.WithShardingKey(e.Argv[1])
	err := w.producer.SendAsync(context.Background(),
		func(ctx context.Context, result *primitive.SendResult, err error) {
			if err != nil {
				log.Warnf("receive message err: %s\n", err.Error())
			}
		}, msg)
	if err != nil {
		log.Warnf("send message err: %s", err.Error())
	}
}

func (w *rocketMQWriter) Close() {
	if err := w.producer.Shutdown(); err != nil {
		log.Warnf("close rocketmq producer err: %s", err.Error())
	}
}

func (w *rocketMQWriter) Status() interface{} {
	return w.stat
}

func (w *rocketMQWriter) StatusString() string {
	return fmt.Sprintf("[%s]: [placehold]", w.stat.Name)
}

func (w *rocketMQWriter) StatusConsistent() bool {
	return true
}
