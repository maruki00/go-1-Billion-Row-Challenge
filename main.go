package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	WORKERS = 10
)

type Measure struct {
	minTemp, maxTemp, meanTemp, sum float64
	count                           int
}

func OpenFile(fPath string) *bufio.Scanner {
	file, err := os.Open(fPath)
	if err != nil {
		panic("file doesnt exists")
	}
	defer file.Close()
	return bufio.NewScanner(file)

}
func main() {
	scanner := OpenFile("./billion_rows_fake.txt")
	data := make(map[string]*Measure)
	stream := make(chan string)
	wg := &sync.WaitGroup{}
	for range WORKERS {
		go worker(stream, data, wg)
	}

	for scanner.Scan() {
		stream <- scanner.Text()
	}
	wg.Wait()
	fmt.Println(data)
}

func worker(linesChan <-chan string, MeasureChan map[string]*Measure, wg *sync.WaitGroup) {
	for line := range linesChan {
		str := strings.Split(line, ";")
		m, err := strconv.ParseFloat(str[1], 32)
		if err != nil {
			continue
		}
		measure, ok := MeasureChan[str[0]]
		if !ok {
			MeasureChan[str[0]] = &Measure{
				minTemp:  m,
				maxTemp:  m,
				meanTemp: m,
				sum:      m,
				count:    1,
			}
		}
		measure.maxTemp = max(measure.maxTemp, m)
		measure.minTemp = min(measure.minTemp, m)
		measure.minTemp += m
		measure.count++

	}
}
