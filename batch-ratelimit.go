package main

import (
	"log"
	"sync"
	"time"
)

type BatchProcessor struct {
	batchSize   int
	rateLimit   bool
	batchChan   chan []int
	itemChan    chan int
	rateLimiter *time.Ticker
	wg          sync.WaitGroup
}

func BatchRateLimit() {
	processor := &BatchProcessor{
		batchSize:   4,
		rateLimit:   true,
		batchChan:   make(chan []int),
		itemChan:    make(chan int),
		rateLimiter: time.NewTicker(500 * time.Millisecond),
		wg:          sync.WaitGroup{},
	}
	processor.startBatchAggregator()
	processor.startBatchProcessor()
	processor.startItemProducer()
	processor.wg.Wait()
}

func (p *BatchProcessor) startItemProducer() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for i := 0; i <= 50; i++ {
			p.itemChan <- i
		}
		close(p.itemChan)
	}()
}

func (p *BatchProcessor) startBatchAggregator() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		batch := make([]int, 0, p.batchSize)
		flushTimer := time.NewTimer(2 * time.Second)

		sendBatch := func() {
			if p.rateLimit {
				<-p.rateLimiter.C // rate limit
			}

			batchCopy := make([]int, len(batch))
			copy(batchCopy, batch)
			p.batchChan <- batchCopy
			batch = make([]int, 0, p.batchSize)
		}
		for {
			select {
			case item, ok := <-p.itemChan:
				// if the item channel is closed, we need to check if there are any items left in the batch
				if !ok {
					if len(batch) > 0 {
						p.batchChan <- batch
					}
					close(p.batchChan)
					p.rateLimiter.Stop()
					return
				}
				batch = append(batch, item)
				if len(batch) >= p.batchSize {
					sendBatch()
				}
			case <-flushTimer.C:
				if len(batch) > 0 {
					sendBatch()
				}
			}
		}
	}()
}

func (p *BatchProcessor) startBatchProcessor() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for batch := range p.batchChan {
			result := 1
			for _, v := range batch {
				result = result * v
			}
			log.Printf("Processing batch=%v value=%d", batch, result)
		}
	}()
}
