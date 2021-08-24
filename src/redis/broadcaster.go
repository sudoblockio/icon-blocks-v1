package redis

import "sync"

// BroadcasterID - type for broadcaster channel IDs
type BroadcasterID int

var lastBroadcasterID BroadcasterID = 0

// Broadcaster - Broadcaster channels
type Broadcaster struct {
	InputChannel chan []byte

	// Output
	OutputChannels map[BroadcasterID]chan []byte
}

var broadcaster *Broadcaster
var broadcasterOnce sync.Once

func GetBroadcaster() *Broadcaster {
	broadcasterOnce.Do(func() {
		broadcaster = &Broadcaster{
			make(chan []byte),
			make(map[BroadcasterID]chan []byte),
		}
	})

	return broadcaster
}

// AddBroadcastChannel - add channel to  broadcaster
func (tb *Broadcaster) AddBroadcastChannel(channel chan []byte) BroadcasterID {
	id := lastBroadcasterID
	lastBroadcasterID++

	tb.OutputChannels[id] = channel

	return id
}

// RemoveBroadcastChannelnel - remove channel from broadcaster
func (tb *Broadcaster) RemoveBroadcastChannel(id BroadcasterID) {
	_, ok := tb.OutputChannels[id]
	if ok {
		delete(tb.OutputChannels, id)
	}
}

// Start - Start broadcaster go routine
func (tb *Broadcaster) Start() {
	go func() {
		for {
			msg := <-tb.InputChannel

			for _, channel := range tb.OutputChannels {
				channel <- msg
			}
		}
	}()
}
