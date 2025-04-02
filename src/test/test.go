package main

import (
    "fmt"
    "time"
)

func worker(done chan bool) {
    for {
        select {
        case <-done:
            fmt.Println("Worker goroutine is stopping...")
            return
        default:
            fmt.Println("Worker is working...")
            time.Sleep(10 * time.Second)
        }
    }
}

func main() {
    done := make(chan bool)
    go worker(done)

    // 模拟一段时间后停止工作
    time.Sleep(1 * time.Second)
    done <- true
    fmt.Println("Main goroutine is waiting for worker to stop...")
    time.Sleep(1 * time.Second)
    fmt.Println("Main goroutine is exiting...")
}