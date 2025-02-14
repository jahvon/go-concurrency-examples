package main

import (
	"log"
	"sync"
)

type PoolJob struct {
	ID   int
	Data int
}

type PoolResult struct {
	WorkerID int
	JobID    int
	Value    int
}

func WorkerPool() {
	numWorkers := 10
	numResults := 50
	wg := &sync.WaitGroup{}
	jobChan := make(chan PoolJob, 10)
	resultChan := make(chan PoolResult, numResults)

	// start workers
	for i := 0; i <= numWorkers; i++ {
		wg.Add(1)
		go startWorker(i, jobChan, resultChan, wg)
	}

	// schedule jobs
	wg.Add(1)
	go func() {
		defer close(jobChan)
		for i := 1; i <= numResults; i++ {
			jobChan <- PoolJob{ID: i, Data: i * 2}
		}
		wg.Done()
	}()

	// wait for workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for result := range resultChan {
		log.Printf("worker %d processed job %d value=%d", result.WorkerID, result.JobID, result.Value)
	}
}

func startWorker(id int, jobChan <-chan PoolJob, resultChan chan<- PoolResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		r := PoolResult{
			WorkerID: id,
			JobID:    job.ID,
			Value:    job.Data * job.Data,
		}
		resultChan <- r
	}
}
