package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

func MapReduce() {
	fmt.Println("--Testdata 1--")
	processData("testdata/test1.txt")

	fmt.Println("--Testdata 2--")
	processData("testdata/test2.txt")

	fmt.Println("--Testdata 3--")
	processData("testdata/test3.txt")

	fmt.Println("--Testdata 4--")
	processData("testdata/test4.txt")

	fmt.Println("--Testdata 5--")
	processData("testdata/test5.txt")
}

type KVPair struct {
	Key   string
	Value int
}

type KVPairs struct {
	Key    string
	Values []int
}

type FileChunk struct {
	Chunk string
	File  string
}

func processData(fn string) []KVPair {
	data, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer data.Close()

	scanner := bufio.NewScanner(data)
	scanner.Split(bufio.ScanWords)

	numMapWorkers := 3
	numReduceWorkers := 3
	textChan := make(chan FileChunk, numMapWorkers)
	mapResultChan := make(chan KVPair, numMapWorkers)
	reduceInChan := make(chan KVPairs, numReduceWorkers)
	finalOutChan := make(chan KVPair, 3)

	// start workers
	mapWg := &sync.WaitGroup{}
	for i := 0; i < numMapWorkers; i++ {
		mapWg.Add(1)
		go mapWorker(mapWg, textChan, mapResultChan)
	}

	// fan out chunks
	mapWg.Add(1)
	go func() {
		defer mapWg.Done()
		for scanner.Scan() {
			text := scanner.Text()
			textChan <- FileChunk{File: fn, Chunk: text}
		}
		close(textChan)
		if scanner.Err() != nil {
			panic(scanner.Err())
		}
	}()

	go func() {
		mapWg.Wait()
		close(mapResultChan)
	}()

	// fan in results
	results := make([]KVPair, 0)
	for result := range mapResultChan {
		results = append(results, result)
	}

	// shuffle and sort kvpairs
	shuffled := shuffle(results)
	slices.SortFunc(results, func(a, b KVPair) int {
		return strings.Compare(a.Key, b.Key)
	})

	// start workers
	reduceWg := &sync.WaitGroup{}
	for i := 0; i < numMapWorkers; i++ {
		reduceWg.Add(1)
		go reduceWorker(reduceWg, reduceInChan, finalOutChan)
	}

	// fan out kvpairs
	reduceWg.Add(1)
	go func() {
		defer reduceWg.Done()
		for key, values := range shuffled {
			reduceInChan <- KVPairs{Key: key, Values: values}
		}
		close(reduceInChan)
	}()

	go func() {
		reduceWg.Wait()
		close(finalOutChan)
	}()

	// fan in results
	final := make([]KVPair, 0)
	for out := range finalOutChan {
		final = append(final, out)
		fmt.Printf("key=%s value=%d\n", out.Key, out.Value)
	}
	return final
}

func shuffle(data []KVPair) map[string][]int {
	result := make(map[string][]int)
	for _, d := range data {
		if _, ok := result[d.Key]; !ok {
			result[d.Key] = make([]int, 0)
		}
		result[d.Key] = append(result[d.Key], d.Value)
	}
	return result
}

func normalize(chunk FileChunk) FileChunk {
	updatedChunk := chunk
	updatedChunk.Chunk = strings.ToLower(updatedChunk.Chunk)
	replacer := strings.NewReplacer(
		"-", "_",
		"(", " ",
		")", " ",
		"[", " ",
		"]", " ",
		"{", " ",
		"}", " ",
		"!", " ",
		"?", " ",
		",", " ",
		".", " ",
		";", " ",
		"'", " ",
		"#", " ",
		"$", " ",
		"%", " ",
		"@", " ",
	)
	updatedChunk.Chunk = replacer.Replace(updatedChunk.Chunk)
	updatedChunk.File = strings.TrimSuffix(filepath.Base(updatedChunk.File), ".txt")
	return updatedChunk
}

func mapWorker(wg *sync.WaitGroup, inputChan <-chan FileChunk, kvChan chan<- KVPair) {
	defer wg.Done()
	for in := range inputChan {
		norm := normalize(in)
		words := strings.Fields(norm.Chunk)
		for _, word := range words {
			kvChan <- KVPair{Key: norm.File + ":" + word, Value: 1}
		}
	}
}

func reduceWorker(wg *sync.WaitGroup, inputChan <-chan KVPairs, kvChan chan<- KVPair) {
	defer wg.Done()
	for kv := range inputChan {
		reducedVal := 1
		for _, v := range kv.Values {
			reducedVal += v
		}
		kvChan <- KVPair{Key: kv.Key, Value: reducedVal}
	}
}
