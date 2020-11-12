package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	cfg := sarama.NewConfig()
	endpoints := []string{"127.0.0.1:9091", "127.0.0.1:9092", "127.0.0.1:9093"}
	client, err := sarama.NewClient(endpoints, cfg)
	if err != nil {
		log.Fatalf("new kafka client err: %s", err.Error())
	}
	defer client.Close()
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("kafka topics --list err: %s", err.Error())
	}

	for _, v := range topics {
		fmt.Printf("%s\n", v)
	}
	c, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("new consumer err: %s", err.Error())
	}
	topic := topics[0]
	wg := sync.WaitGroup{}
	messages := make(chan *sarama.ConsumerMessage, 10)
	closing := make(chan struct{})
	partitions, err := c.Partitions(topic)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
		<-signals
		log.Println("Initial shutdown of consumer ...")
		close(closing)
	}()

	for _, v := range partitions {
		pc, err := c.ConsumePartition(topic, v, 0)
		if err != nil {
			log.Fatalf("failed to start consumer for partition %d: %s", v, err.Error())
		}
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for msg := range messages {
			fmt.Printf("Partition:\t%d\n", msg.Partition)
			fmt.Printf("Offset:\t%d\n", msg.Offset)
			fmt.Printf("Key:\t%s\n", string(msg.Key))
			fmt.Printf("Value:\t%s\n", string(msg.Value))
			fmt.Println()
		}
	}()

	wg.Wait()
	log.Println("Done consume topic", topic)
	close(messages)
}
