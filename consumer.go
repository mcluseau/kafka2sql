package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/Shopify/sarama"
	"github.com/cespare/xxhash"
	"github.com/golang/groupcache/lru"
	"github.com/jmoiron/sqlx"
	"github.com/mcluseau/sql2sync/pkg/db"
)

type consumer struct {
	spec *Spec
	dbx  *sqlx.DB
}

func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("consumer group claim started: %s:%d at %d", claim.Topic(), claim.Partition(), claim.InitialOffset())
	logPrefix := fmt.Sprintf("claim %s:%d: ", claim.Topic(), claim.Partition())

	cache := lru.New(10000)

	// prepare for consuming
	myStreams := make([]*StreamSpec, 0, 1)
	columns := make([][]string, 0, 1)
	inserts := make([]*sql.Stmt, 0, 1)
	updates := make([]*sql.Stmt, 0, 1)
	for _, stream := range c.spec.Streams {
		if stream.Disabled {
			continue
		}
		if stream.Topic != claim.Topic() {
			continue
		}

		cols := make([]string, 0, len(stream.Mapping))
		for key := range stream.Mapping {
			cols = append(cols, key)
		}

		sort.Strings(cols)

		myStreams = append(myStreams, stream)
		columns = append(columns, cols)

		stmt, err := db.DB.Prepare(c.dbx.Rebind(insertStatement(stream.Table, cols)))
		if err != nil {
			log.Fatal(logPrefix, "failed to prepare insert: ", err)
		}
		inserts = append(inserts, stmt)

		stmt, err = db.DB.Prepare(c.dbx.Rebind(updateStatement(stream.Table, cols, stream.Key)))
		if err != nil {
			log.Fatal(logPrefix, "failed to prepare update: ", err)
		}
		updates = append(updates, stmt)
	}

	// consume messages
	for message := range claim.Messages() {
		log.Printf("%soffset=%d timestamp=%v value: %s", logPrefix, message.Offset, message.Timestamp,
			string(message.Value))

		value := make(map[string]interface{})
		if err := json.Unmarshal(message.Value, &value); err != nil {
			log.Printf("%sERROR: failed to parse message offset %d, ignoring: %v", logPrefix, message.Offset, err)
			session.MarkMessage(message, "")
			continue
		}

	streamsLoop:
		for idx, stream := range myStreams {
			keyArgs := make([]interface{}, 0, len(stream.Key))
			args := make([]interface{}, 0, len(columns[idx]))

			for _, key := range columns[idx] {
				mapping := stream.Mapping[key]
				v, err := mapping.ValueFrom(value)
				if err != nil {
					log.Printf("%sERROR: failed to get value for key %s: %v", logPrefix, key, err)
					continue streamsLoop
				}

				args = append(args, v)

				// also append to key as needed
				for _, keyCol := range stream.Key {
					if key == keyCol {
						keyArgs = append(keyArgs, v)
						break
					}
				}
			}

			// hash the key
			khw := xxhash.New()
			if err := json.NewEncoder(khw).Encode(keyArgs); err != nil {
				log.Fatal(logPrefix, "JSON encoding failed: ", err)
			}
			kh := khw.Sum64()

			updateArgs := append(args, keyArgs...)

			if _, ok := cache.Get(kh); ok {
				// that key exists in the DB
				log.Print(logPrefix, "SQL update")
				if _, err := updates[idx].Exec(updateArgs...); err != nil {
					log.Fatal(logPrefix, "SQL update failed: ", err)
				}

			} else {
				// the key may or may not exist, try to insert first
				log.Print(logPrefix, "SQL insert")
				if _, err := inserts[idx].Exec(args...); err != nil {
					log.Print(logPrefix, "SQL update after failed insert")
					// insert failed, try to update
					if _, err2 := updates[idx].Exec(updateArgs...); err2 != nil {
						log.Print(logPrefix, "SQL insert failed: ", err)
						log.Fatal(logPrefix, "SQL update failed: ", err2)
					}
				}
			}

			cache.Add(kh, true)
		}

		session.MarkMessage(message, "")
	}

	return nil
}
