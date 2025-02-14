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
	wg          *sync.WaitGroup
}

func BatchRateLimit() {
	processor := &BatchProcessor{
		batchSize:   4,
		rateLimit:   true,
		batchChan:   make(chan []int),
		itemChan:    make(chan int),
		rateLimiter: time.NewTicker(500 * time.Millisecond),
		wg:          &sync.WaitGroup{},
	}
	processor.Start()
	processor.wg.Add(1)
	go func() {
		for i := 0; i <= 50; i++ {
			processor.Process(i)
		}
		close(processor.itemChan)
		processor.wg.Done()
	}()
	processor.wg.Wait()
}

func (p *BatchProcessor) Start() {
	go func() {
		p.wg.Add(1)
		defer p.wg.Done()

		batch := make([]int, 0, p.batchSize)
		for {
			select {
			case item, ok := <-p.itemChan:
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
					if p.rateLimit {
						<-p.rateLimiter.C // rate limit
					}
					p.batchChan <- batch
					batch = make([]int, 0, p.batchSize)
				}
			}
		}
	}()

	go func() {
		p.wg.Add(1)
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

func (p *BatchProcessor) Process(value int) {
	p.itemChan <- value
}
