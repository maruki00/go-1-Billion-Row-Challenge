package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/profile"
)

const (
	WORKERS = 10
)

type Measure struct {
	minTemp, maxTemp, meanTemp, sum float64
	count                           int
}

func main() {
	defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	file, err := os.Open("./measures.txt")
	if err != nil {
		panic("file doesn't exist")
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	measureMap := make(map[string]*Measure)
	var mapMutex sync.Mutex
	stream := make(chan string)
	wg := &sync.WaitGroup{}
	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go worker(stream, measureMap, &mapMutex, wg)
	}
	line := 0
	for scanner.Scan() {
		if line%100000000 == 0 {
			print(line)
		}
		stream <- scanner.Text()
		line++
	}
	close(stream)
	wg.Wait()
	// for key, measure := range measureMap {
	// 	measure.meanTemp = measure.sum / float64(measure.count)
	// 	fmt.Printf("Sensor: %s, Min: %.2f, Max: %.2f, Mean: %.2f\n", key, measure.minTemp, measure.maxTemp, measure.meanTemp)
	// }
}

func worker(linesChan <-chan string, measureMap map[string]*Measure, mapMutex *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	for line := range linesChan {
		// Parse the line
		parts := strings.Split(line, ";")
		if len(parts) < 2 {
			continue
		}
		sensor := parts[0]
		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		mapMutex.Lock()
		measure, exists := measureMap[sensor]
		if !exists {
			measure = &Measure{
				minTemp: value,
				maxTemp: value,
				sum:     value,
				count:   1,
			}
			measureMap[sensor] = measure
			mapMutex.Unlock()
			continue
		}
		mapMutex.Unlock()

		measure.maxTemp = max(measure.maxTemp, value)
		measure.minTemp = min(measure.minTemp, value)
		measure.sum += value
		measure.count++
	}
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
