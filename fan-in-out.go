package main

import (
	"log"
	"math/rand"
	"strings"
	"sync"
)

type FanTask struct {
	ID    int
	Value string
}

type FanResult struct {
	TaskID   int
	WorkerID int
	Value    string
}

func FanInOut() {
	numTasks := 25
	taskChan := make(chan FanTask, 5)
	resultChan := make(chan FanResult, numTasks)
	numWorkers := 3
	wg := &sync.WaitGroup{}

	// start workers
	for i := 0; i <= numWorkers; i++ {
		wg.Add(1)
		go fanWorker(i, taskChan, resultChan, wg)
	}

	// fan out to workers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i <= numTasks; i++ {
			taskChan <- FanTask{ID: i, Value: generateSentence()}
		}
		close(taskChan)
	}()

	// fan in results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for result := range resultChan {
		log.Printf("worker=%d task=%d value=%s", result.WorkerID, result.TaskID, result.Value)
	}
}

func fanWorker(id int, tasks <-chan FanTask, results chan<- FanResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range tasks {
		replacer := strings.NewReplacer(
			"o", "0",
			"i", "1",
			" ", "~",
			"a", "4",
			"t", "7",
		)
		r := FanResult{
			WorkerID: id,
			TaskID:   task.ID,
			Value:    replacer.Replace(task.Value),
		}
		results <- r
	}
}

func generateSentence() string {
	beginning := []string{
		"The quick brown fox",
		"Go concurrency",
		"Hello, world!",
		"Once upon a time,",
	}
	ending := []string{
		"jumps over the lazy dog.",
		"is an open-source programming language.",
		"is not parallelism.",
		"keep calm and code on.",
	}
	return beginning[rand.Intn(len(beginning))] + " " + ending[rand.Intn(len(ending))]
}
