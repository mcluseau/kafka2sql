package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/jmoiron/sqlx"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/mcluseau/sql2sync/pkg/db"

	"kafka2sql/pkg/kafka"
)

var (
	specPath string
)

func main() {
	cmd := &cobra.Command{
		Use:   "kafka2sql",
		Short: "Send a topic's data to a SQL database",
		Run:   run,
	}

	flags := cmd.Flags()
	flags.StringVar(&specPath, "spec", "spec.yaml", "Job specification")

	db.RegisterFlags("", "", flags)
	kafka.RegisterFlags("kafka-", flags)

	cmd.Execute()
}

func run(_ *cobra.Command, _ []string) {
	specBytes, err := ioutil.ReadFile(specPath)
	if err != nil {
		log.Fatal("failed to read spec: ", err)
	}

	spec := &Spec{}

	if err = yaml.UnmarshalStrict(specBytes, &spec); err != nil {
		log.Fatal("failed to parse spec: ", err)
	}

	db.Connect()
	defer db.Close()

	dbx := sqlx.NewDb(db.DB, db.Driver())

	config := kafka.DefaultConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := kafka.NewConsumerGroup(spec.ConsumerGroup, config)

	if err != nil {
		log.Fatal("failed to connect to Kafka: ", err)
	}

	topics := make([]string, 0, len(spec.Streams))
	for _, stream := range spec.Streams {
		if stream.Disabled {
			continue
		}

		found := false

		for _, prevTopic := range topics {
			if prevTopic == stream.Topic {
				found = true
				break
			}
		}

		if found {
			continue
		}

		topics = append(topics, stream.Topic)
	}

	consumer := &consumer{spec, dbx}

	go handleSignals(group)

	ctx := context.Background() // TODO cancellable
	err = group.Consume(ctx, topics, consumer)
	if err != nil {
		log.Fatal("group consume failed: ", err)
	}
}

func handleSignals(group sarama.ConsumerGroup) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	s := <-c

	log.Print("got signal ", s, ", closing...")

	if err := group.Close(); err != nil {
		log.Fatal("consumer group close failed: ", err)
	}
}
