package main

import (
	"log"
	"sync"
	"time"
)

type ConsumerResult struct {
	ConsumerID int
	Value      int
	Squared    int
}

func ProducerConsumer() {
	numConsumers := 5
	numMsgs := 25
	msgChan := make(chan int, numConsumers)
	resultChan := make(chan ConsumerResult, numMsgs)
	wg := &sync.WaitGroup{}

	// producer
	wg.Add(1)
	go func() {
		defer close(msgChan)
		for i := 1; i <= numMsgs; i++ {
			msgChan <- i
			log.Printf("produced %d", i)
		}
		wg.Done()
	}()

	// result processor
	go func() {
		for result := range resultChan {
			log.Printf("consumer %d calculated %d^2=%d\n", result.ConsumerID, result.Value, result.Squared)
		}
	}()

	for id := 0; id < numConsumers; id++ {
		wg.Add(1)
		go consumerFunc(id, msgChan, resultChan, wg)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond) // give time to finish
	close(resultChan)
}

func consumerFunc(id int, msgChan <-chan int, resultChan chan<- ConsumerResult, wg *sync.WaitGroup) {
	for msg := range msgChan {
		r := ConsumerResult{
			ConsumerID: id,
			Value:      msg,
			Squared:    msg * msg,
		}
		resultChan <- r
	}
	wg.Done()
}
