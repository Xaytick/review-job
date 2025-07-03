package job

import (
	"context"
	"encoding/json"
	"errors"

	"review-job/internal/conf"

	"github.com/go-kratos/kratos/v2/log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/segmentio/kafka-go"
)

// 评论数据流处理

// 自定义执行job的结构体，实现transport.Server接口
type JobWorker struct {
	KafkaReader *kafka.Reader
	esClient    *ESClient
	log         *log.Helper
}

func NewJobWorker(kafkaReader *kafka.Reader, esClient *ESClient, logger log.Logger) *JobWorker {
	return &JobWorker{
		KafkaReader: kafkaReader,
		esClient:    esClient,
		log:         log.NewHelper(logger),
	}
}

func NewKafkaReader(cfg *conf.Kafka) *kafka.Reader {
	// 创建一个reader，指定GroupID，从 topic-A 消费消息
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		GroupID: cfg.GroupId, // 指定消费者组id
		Topic:   cfg.Topic,
	})
}

// ES 客户端
type ESClient struct {
	*elasticsearch.TypedClient
	index string
}

func NewESClient(cfg *conf.Elasticsearch) (*ESClient, error) {
	esCfg := elasticsearch.Config{
		Addresses: cfg.Addresses,
	}

	client, err := elasticsearch.NewTypedClient(esCfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
		return nil, err
	}

	return &ESClient{
		TypedClient: client,
		index:       cfg.Index,
	}, nil
}

// 定义kafka中接收到的数据
type Message struct {
	Type     string                   `json:"type"`
	Database string                   `json:"database"`
	Table    string                   `json:"table"`
	IsDdl    bool                     `json:"is_ddl"`
	Data     []map[string]interface{} `json:"data"`
}

// kratos 启动之后，会调用Start方法
// ctx 是kratos的context，带有取消信号
func (jw JobWorker) Start(ctx context.Context) error {
	// 1. 从kafka中获取MySQL的数据变更消息
	// 2. 将完整评价写入Elasticsearch
	// 接收消息
	for {
		m, err := jw.KafkaReader.ReadMessage(ctx)
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			jw.log.Errorf("failed to read message from kafka: %v", err)
			break
		}
		jw.log.Debugf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		// 将消息写入Elasticsearch
		msg := new(Message)
		if err := json.Unmarshal(m.Value, msg); err != nil {
			jw.log.Errorf("failed to unmarshal message from kafka: %v", err)
			continue
		}
		if msg.Type != "INSERT" {
			// 向ES中更新文档
			for idx := range msg.Data {
				jw.updateDocument(msg.Data[idx])
			}
			continue
		} else {
			// 向ES中插入文档
			for idx := range msg.Data {
				jw.indexDocument(msg.Data[idx])
			}
		}

		jw.log.Debugf("message: %v", msg)
	}

	return nil
}

// kratos 停止之前，会调用Stop方法
func (jw JobWorker) Stop(ctx context.Context) error {
	// 结束时关闭kafka连接
	if err := jw.KafkaReader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
	return nil
}

// indexDocument 索引文档
func (jw JobWorker) indexDocument(d map[string]interface{}) {
	reviewID := d["review_id"].(string)

	// 添加文档
	resp, err := jw.esClient.Index(jw.esClient.index).
		Id(reviewID).
		Document(d).
		Do(context.Background())
	if err != nil {
		jw.log.Errorf("indexing document failed, err:%v\n", err)
		return
	}
	jw.log.Debugf("result:%#v", resp.Result)
}

// updateDocument 更新文档
func (jw JobWorker) updateDocument(d map[string]interface{}) {
	reviewID := d["review_id"].(string)

	resp, err := jw.esClient.Update(jw.esClient.index, reviewID).
		Doc(d). // 使用结构体变量更新
		Do(context.Background())
	if err != nil {
		jw.log.Errorf("update document failed, err:%v\n", err)
		return
	}
	jw.log.Debugf("result:%v", resp.Result)
}
