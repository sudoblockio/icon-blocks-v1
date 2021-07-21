package kafka

import (
	"gopkg.in/Shopify/sarama.v1"
)

// BroadcasterID - type for broadcaster channel IDs
type BroadcasterID int

var lastBroadcasterID BroadcasterID = 0

// TopicBroadcastFunc - Custom broadcaster function
type TopicBroadcastFunc func(channel chan *sarama.ConsumerMessage, message *sarama.ConsumerMessage)

// TopicBroadcaster - Broadcaster channels
type TopicBroadcaster struct {

	// Input"
	ConsumerChan chan *sarama.ConsumerMessage

	// Output
	BroadcastChans map[BroadcasterID]chan *sarama.ConsumerMessage

	BroadcastFunc TopicBroadcastFunc
}

// Broadcasters - topic name -> broadcaster
var Broadcasters = map[string]*TopicBroadcaster{}

func newBroadcaster(topicName string, BroadcastFunc TopicBroadcastFunc) {
	if BroadcastFunc == nil {
		BroadcastFunc = func(channel chan *sarama.ConsumerMessage, message *sarama.ConsumerMessage) {
			channel <- message
		}
	}

	Broadcasters[topicName] = &TopicBroadcaster{
		make(chan *sarama.ConsumerMessage),
		make(map[BroadcasterID]chan *sarama.ConsumerMessage),
		BroadcastFunc,
	}

	go Broadcasters[topicName].Start()
}

// AddBroadcastChannel - add channel to topic broadcaster
func (tb *TopicBroadcaster) AddBroadcastChannel(topicChan chan *sarama.ConsumerMessage) BroadcasterID {
	id := lastBroadcasterID
	lastBroadcasterID++

	tb.BroadcastChans[id] = topicChan

	return id
}

// RemoveBroadcastChannel - remove channel from topic broadcaster
func (tb *TopicBroadcaster) RemoveBroadcastChannel(id BroadcasterID) {
	_, ok := tb.BroadcastChans[id]
	if ok {
		delete(tb.BroadcastChans, id)
	}
}

// Start - Start broadcaster go routine
func (tb *TopicBroadcaster) Start() {
	for {
		msg := <-tb.ConsumerChan

		for _, channel := range tb.BroadcastChans {
			tb.BroadcastFunc(channel, msg)
		}
	}
}
