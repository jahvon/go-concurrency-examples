package main

import (
	"log"
	"sync"
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
	resultChan := make(chan ConsumerResult, numMsgs/2)
	wg := &sync.WaitGroup{}

	// producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(msgChan)

		for i := 1; i <= numMsgs; i++ {
			msgChan <- i
			log.Printf("produced %d", i)
		}
	}()

	// consumers
	for id := 0; id < numConsumers; id++ {
		wg.Add(1)
		go consumerFunc(id, msgChan, resultChan, wg)
	}

	// result processor
	done := make(chan struct{})
	go func() {
		defer close(done)
		for result := range resultChan {
			log.Printf("consumer %d calculated %d^2=%d\n", result.ConsumerID, result.Value, result.Squared)
		}
	}()

	wg.Wait()
	close(resultChan)
	<-done
}

func consumerFunc(id int, msgChan <-chan int, resultChan chan<- ConsumerResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for msg := range msgChan {
		r := ConsumerResult{
			ConsumerID: id,
			Value:      msg,
			Squared:    msg * msg,
		}
		resultChan <- r
	}
}
