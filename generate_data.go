package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

const (
	numLines   = 1000000000
	numWorkers = 100
	filename   = "billion_rows_fake.txt"
)

func main() {
	cities := GetCities()
	rand.Seed(1)
	linesChan := make(chan int, numLines)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(linesChan, cities, &wg)
	}
	for i := 0; i < numLines; i++ {
		linesChan <- i
	}
	close(linesChan)
	wg.Wait()
	fmt.Println("Done generating lines.")
}

func worker(linesChan <-chan int, cities []string, wg *sync.WaitGroup) {
	defer wg.Done()
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	for line := range linesChan {
		rand.Seed(time.Now().UnixNano())
		if line%1000000 == 0 {
			print("\r\r " + strconv.Itoa(line))
		}
		tmpRand := rand.Float32()*60 - 20
		l := len(cities)
		line := fmt.Sprintf("%s;%.1f\n", cities[rand.Intn(l)], tmpRand)
		if _, err := file.WriteString(line); err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}
}

func GetCities() []string {
	var cities []string
	maxInt := 8000 + rand.Int()%10000
	for i := 0; i < maxInt; i++ {
		cities = append(cities, gofakeit.City())
	}
	return cities
}
