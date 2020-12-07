package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

//var (
//	brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "separated list of brokers in the kafka cluster")
//	topic = flag.String("topic", "", "RE")
//)


func main() {
	topic := "test"
	brokerList := []string{"127.0.0.1:9091", "127.0.0.1:9092", "127.0.0.1:9093"}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionZSTD

	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalf("failed to open kafka producer: %s", err.Error())
	}
	defer producer.Close()

	var val string
	message := &sarama.ProducerMessage{Topic: topic}
	for{
		fmt.Print("> ")
		fmt.Scanln(&val)
		message.Value = sarama.StringEncoder(val)
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalf("Failed to produce send message err: %s", err.Error())
		}
		fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", topic, partition, offset)
	}
}
