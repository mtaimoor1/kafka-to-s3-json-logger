package writers

import "github.com/IBM/sarama"

type Writer interface {
	CreateWriter()
	Process(messages []sarama.ConsumerMessage)
	flush(data []byte)
}
