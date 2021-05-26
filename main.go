package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
	"github.com/tkanos/gonfig"
	"github.com/urfave/cli/v2"
)

var config = Configuration{}

type Configuration struct {
	BrokerAddress string
	MaxMessages   int
}

func init() {
	gonfig.GetConf("settings.json", &config)
}

func main() {
	app := &cli.App{
		Name:  "KafkaCli",
		Usage: "A simple Kafka CLI program to manage your kafka",
		Commands: []*cli.Command{
			{
				Name:    "topic",
				Aliases: []string{"topic"},
				Usage:   "complete a task on the list",
				Action: func(c *cli.Context) error {
					if c.Args().First() == "list" || c.Args().First() == "l" {
						conn, err := kafka.Dial("tcp", config.BrokerAddress)
						if err != nil {
							panic(err.Error())
						}
						defer conn.Close()
						partitions, err := conn.ReadPartitions()
						if err != nil {
							panic(err.Error())
						}
						m := map[string]struct{}{}

						for _, p := range partitions {
							m[p.Topic] = struct{}{}
						}
						for k := range m {
							fmt.Println(k)
						}
					} else if c.Args().First() == "create" || c.Args().First() == "c" {
						topicName := c.Args().Get(1)

						if topicName == "" {
							fmt.Println("topic name cannot be empty or null!")
						}
						conn, err := kafka.DialLeader(context.Background(), "tcp", config.BrokerAddress, topicName, 0)

						if err != nil {
							panic(err.Error())
						} else {
							fmt.Println("topic created")
						}

						defer conn.Close()

					} else if c.Args().First() == "delete" || c.Args().First() == "d" {
						topicName := c.Args().Get(1)

						if topicName == "" {
							fmt.Println("topic name cannot be empty or null!")
						}
						conn, err := kafka.Dial("tcp", config.BrokerAddress)

						if err != nil {
							panic(err.Error())
						} else {
							err := conn.DeleteTopics(topicName)
							if err != nil {
								panic(err.Error())
							} else {
								fmt.Println("topic deleted")
							}
						}
						defer conn.Close()
					} else if c.Args().First() == "read" || c.Args().First() == "r" {
						topicName := c.Args().Get(1)

						if topicName == "" {
							fmt.Println("topic name cannot be empty or null!")
						}

						conn, _ := kafka.DialLeader(context.Background(), "tcp", config.BrokerAddress, topicName, 0)
						_, last, _ := conn.ReadOffsets()

						r := kafka.NewReader(kafka.ReaderConfig{
							Brokers:   []string{config.BrokerAddress},
							Topic:     topicName,
							Partition: 0,
							MinBytes:  10e3, // 10KB
							MaxBytes:  10e6, // 10MB
						})
						r.SetOffset(last - int64(config.MaxMessages))

						for {
							m, err := r.FetchMessage(context.Background())

							if err != nil {
								panic(err.Error())
							} else if string(m.Value) == "" {
								return nil
							} else {
								fmt.Println(string(m.Value))
							}
						}

					}
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
