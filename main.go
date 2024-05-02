package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	nums := getNumbersFromFile("random_numbers.txt")
	wg := sync.WaitGroup{}
	ch := make(chan int)

	defer func() func() {
		startTime := time.Now()
		return func() {
			fmt.Printf("time since start: %1.3fs\n", time.Since(startTime).Seconds())
		}

	}()()

	for _, num := range nums {
		wg.Add(1)
		go func(num int, ch chan int) {
			defer wg.Done()
			worker(num, ch)
		}(num, ch)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	totalCount := 0
	for currentCount := range ch {
		totalCount += currentCount
	}
	fmt.Println(totalCount)
}

func getNumbersFromFile(fileName string) []int {
	fd, err := os.Open(fileName)
	checkErr(err)
	defer fd.Close()

	numbersFromFile := make([]int, 0, 50000)

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		numberAsText := scanner.Text()
		numberAsInt, err := strconv.Atoi(numberAsText)
		checkErr(err)
		numbersFromFile = append(numbersFromFile, numberAsInt)
	}

	return numbersFromFile
}

func worker(number int, outCh chan int) {
	currCount := 0
	numSqrt := int(math.Sqrt(float64(number)))
	for divider := 2; divider <= numSqrt; divider++ {
		for number%divider == 0 {
			number /= divider
			currCount += 1
		}
	}

	if number > 1 {
		currCount += 1
	}

	outCh <- currCount
}
