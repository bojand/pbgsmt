package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	models "github.com/bojand/pbgsmt/gen/pkg/models"
	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {

	brokerAddr := "localhost:19092"
	topic := "test-pbgsmt"

	kafkaClient, kafkaAdminClient := CreateClients([]string{brokerAddr})

	CreateTopic(kafkaAdminClient, topic)

	ProduceRecords(kafkaClient, topic)
}

func CreateTopic(kafkaAdminClient *kadm.Client, topic string) {
	topicDetails, err := kafkaAdminClient.ListTopics(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	if !topicDetails.Has(topic) {
		resp, _ := kafkaAdminClient.CreateTopics(context.Background(), 1, 1, nil, topic)
		for _, ctr := range resp {
			if ctr.Err != nil {
				log.Fatal(ctr.Err)
			}
		}

		log.Printf("topic %+v created.", topic)
	} else {
		log.Printf("topic %+v already exists.", topic)
	}
}

func ProduceRecords(kafkaClient *kgo.Client, topic string) {
	id, err := nanoid.New()
	if err != nil {
		log.Fatal(err)
	}

	code := codes.Code(uint(rand.Intn(17)))

	msg := models.Package{
		Version:   1,
		Id:        id,
		CreatedAt: timestamppb.New(time.Now()),
		Status:    &spb.Status{Code: int32(code), Message: code.String()},
	}

	pbData, err := proto.Marshal(&msg)
	if err != nil {
		log.Fatal(err)
	}

	r := &kgo.Record{
		Key:       []byte(msg.Id),
		Value:     pbData,
		Topic:     topic,
		Timestamp: time.Now(),
	}

	produceCtx, produceCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer produceCancel()

	results := kafkaClient.ProduceSync(produceCtx, r)
	if results.FirstErr() != nil {
		log.Fatal(results.FirstErr())
	}

	log.Println("created message:", id)
}

func CreateClients(brokers []string) (*kgo.Client, *kadm.Client) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.MaxVersions(kversion.V2_6_0()),
		kgo.FetchMaxBytes(5 * 1000 * 1000), // 5MB
		kgo.MaxConcurrentFetches(12),
		kgo.KeepControlRecords(),
		kgo.MetadataMinAge(250 * time.Millisecond),
	}

	kClient, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}

	kafkaAdmCl := kadm.NewClient(kClient)

	return kClient, kafkaAdmCl
}
