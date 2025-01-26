package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/profile"
)

const (
	WORKERS = 100
	MAXROWS = 10000000
)

type Measure struct {
	minTemp, maxTemp, meanTemp, sum float64
	count                           int
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	file, err := os.Open("./measures.txt")
	if err != nil {
		panic("file doesn't exist")
	}

	defer file.Close()
	measureMap := new(sync.Map)

	wg := &sync.WaitGroup{}
	mk := 0
	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go worker(measureMap, mk, file, wg)
		mk += MAXROWS
	}
	wg.Wait()
	measureMap.Range(func(key any, value any) bool {
		fmt.Println(key, " : ", value)
		// measure := value.(Measure)
		// measure.meanTemp = measure.sum / float64(measure.count)
		// fmt.Printf("Sensor: %s, Min: %.2f, Max: %.2f, Mean: %.2f\n", key, measure.minTemp, measure.maxTemp, measure.meanTemp)
		return true
	})
}

func worker(measureMap *sync.Map, seekTo int, f *os.File, wg *sync.WaitGroup) {
	defer wg.Done()
	_, _ = f.Seek(int64(seekTo), 0)
	reader := bufio.NewReader(f)
	maxReach := MAXROWS
	for {
		if maxReach <= 0 {
			break
		}
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			continue
		}

		parts := strings.Split(string(line), ";")
		if len(parts) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		measure, exists := measureMap.Load(parts[0])
		if !exists {
			measureMap.Store(parts[0], &Measure{
				minTemp: value,
				maxTemp: value,
				sum:     value,
				count:   1,
			})
			continue
		}

		measure.(*Measure).maxTemp = max(measure.(*Measure).maxTemp, value)
		measure.(*Measure).minTemp = min(measure.(*Measure).minTemp, value)
		measure.(*Measure).sum += value
		measure.(*Measure).count++

		maxReach--
	}
}
