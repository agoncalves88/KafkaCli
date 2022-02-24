package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/tkanos/gonfig"
	"github.com/urfave/cli/v2"
)

var config = Configuration{}

type Configuration struct {
	BrokerAddress  string
	BrokerUserName string
	BrokerPassword string
	MaxMessages    int64
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
				Aliases: []string{"list", "create", "delete", "read"},
				Usage:   "to topic commands",
				Action: func(c *cli.Context) error {
					if c.Args().First() == "list" || c.Args().First() == "l" {

						mechanism := plain.Mechanism{
							Username: config.BrokerUserName,
							Password: config.BrokerPassword,
						}

						dialer := &kafka.Dialer{
							Timeout:       10 * time.Second,
							DualStack:     true,
							SASLMechanism: mechanism,
						}
						conn, err := dialer.DialContext(context.Background(), "tcp", config.BrokerAddress)

						if err != nil {
							panic(err.Error())
						}
						defer conn.Close()
						partitions, err := conn.ReadPartitions()
						check(err)
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

						mechanism := plain.Mechanism{
							Username: config.BrokerUserName,
							Password: config.BrokerPassword,
						}

						dialer := &kafka.Dialer{
							Timeout:       10 * time.Second,
							DualStack:     true,
							SASLMechanism: mechanism,
						}
						conn, err := dialer.DialLeader(context.Background(), "tcp", config.BrokerAddress, topicName, 0)

						check(err)
						fmt.Println("topic created")
						defer conn.Close()

					} else if c.Args().First() == "delete" || c.Args().First() == "d" {
						topicName := c.Args().Get(1)

						if topicName == "" {
							fmt.Println("topic name cannot be empty or null!")
						}

						mechanism := plain.Mechanism{
							Username: config.BrokerUserName,
							Password: config.BrokerPassword,
						}

						dialer := &kafka.Dialer{
							Timeout:       10 * time.Second,
							DualStack:     true,
							SASLMechanism: mechanism,
						}

						conn, err := dialer.Dial("tcp", config.BrokerAddress)

						check(err)
						err = conn.DeleteTopics(topicName)
						check(err)
						fmt.Println("topic deleted")
						defer conn.Close()
					} else if c.Args().First() == "read" || c.Args().First() == "r" {
						topicName := c.Args().Get(1)

						if topicName == "" {
							fmt.Println("topic name cannot be empty or null!")
						}
						mechanism := plain.Mechanism{
							Username: config.BrokerUserName,
							Password: config.BrokerPassword,
						}

						dialer := &kafka.Dialer{
							Timeout:       10 * time.Second,
							DualStack:     true,
							SASLMechanism: mechanism,
						}

						conn, _ := dialer.DialLeader(context.Background(), "tcp", config.BrokerAddress, topicName, 0)
						_, last, _ := conn.ReadOffsets()

						r := kafka.NewReader(kafka.ReaderConfig{
							Brokers:   strings.Split(config.BrokerAddress, ","),
							Topic:     topicName,
							Partition: 0,
							MinBytes:  10e3, // 10KB
							MaxBytes:  10e6, // 10MB
							Dialer:    dialer,
						})
						r.SetOffset(last - int64(config.MaxMessages))

						for {
							m, err := r.ReadMessage(context.Background())
							check(err)
							if string(m.Value) == "" {
								return nil
							} else {
								fmt.Println(string(m.Value) + "\n")
							}
						}
					}
					return nil
				},
			}, {
				Name:    "connection",
				Aliases: []string{"get", "set"},
				Usage:   "to interact in settings.json",
				Action: func(c *cli.Context) error {
					if c.Args().First() == "set" {

						if c.Args().Get(1) == "BrokerAddress" {
							if c.Args().Get(2) != "" {
								config.BrokerAddress = c.Args().Get(2)
								newSettings, _ := json.Marshal(config)
								err := ioutil.WriteFile("settings.json", []byte(newSettings), 0644)
								check(err)

							}
						} else if c.Args().Get(1) == "MaxMessages" {
							if c.Args().Get(2) != "" {
								config.MaxMessages, _ = strconv.ParseInt(c.Args().Get(2), 10, 64)
								newSettings, _ := json.Marshal(config)
								err := ioutil.WriteFile("settings.json", []byte(newSettings), 0644)
								check(err)
							}
						}

					} else if c.Args().First() == "get" {
						dat, err := ioutil.ReadFile("settings.json")

						check(err)
						fmt.Println(string(dat))
					}
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	check(err)
}

func check(err error) {
	if err != nil {
		panic(err.Error())
	}
}
