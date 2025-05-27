package main

import (
	"log"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("invalid args. Expect one of [producer-consumer,worker-pool,batch,rate-limit,fan,circuit-breaker,map-reduce]")
	}
	switch os.Args[1] {
	case "producer-consumer":
		ProducerConsumer()
	case "worker-pool":
		WorkerPool()
	case "batch", "rate-limit":
		BatchRateLimit()
	case "fan":
		FanInOut()
	case "circuit-breaker":
		CircuitBreaker()
	case "map-reduce":
		MapReduce()
	default:
		log.Fatal("invalid args. Expect one of [producer-consumer,worker-pool,batch,rate-limit,fan,circuit-breaker,map-reduce]")
	}
}
