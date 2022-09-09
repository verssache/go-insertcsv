package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	csvFile     = "majestic_million.csv"
	totalWorker = 100
)

var dataHeader = make([]string, 0)

func main() {
	start := time.Now()

	db, err := openDbConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	reader, file, err := openCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}

	jobs := make(chan []interface{})
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	readCsvFilePerLineThenSendToWorker(db, reader, jobs, wg)

	wg.Wait()
	file.Close()

	log.Println("Done in", int(math.Ceil(time.Since(start).Seconds())), "seconds")

}

func openDbConnection() (*gorm.DB, error) {
	dsn := "root:@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	sql, err := db.DB()
	if err != nil {
		return nil, err
	}

	sql.SetMaxOpenConns(100)
	sql.SetMaxIdleConns(4)

	return db, err
}

func openCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("Opening CSV file...")

	file, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(file)
	return reader, file, err
}

func dispatchWorkers(db *gorm.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	log.Println("Dispatching workers...")

	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, db *gorm.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			log.Printf("Worker #%d started", workerIndex)

			counter := 0
			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func readCsvFilePerLineThenSendToWorker(db *gorm.DB, reader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	log.Println("Reading CSV file per line...")

	for {
		line, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(dataHeader) == 0 {
			dataHeader = line
			continue
		}

		lineOrdered := make([]interface{}, 0)
		for _, header := range dataHeader {
			lineOrdered = append(lineOrdered, line[getIndexByHeader(header)])
		}

		wg.Add(1)
		jobs <- lineOrdered
	}
	close(jobs)
}

func doTheJob(workerIndex int, counter int, db *gorm.DB, job []interface{}) {
	for {
		outerError := db.Transaction(func(tx *gorm.DB) error {
			defer func() {
				if r := recover(); r != nil {
					log.Println("Recovered in f", r)
				}
			}()

			query := fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s)", strings.Join(dataHeader, ","), strings.Join(generateQuestionsMark(len(dataHeader)), ","))
			innerError := tx.Exec(query, job...).Error
			if innerError != nil {
				return innerError
			}

			return nil
		})

		if outerError == nil {
			break
		}

		log.Printf("Worker #%d when inserting %d data, got error: %s", workerIndex, counter, outerError.Error())
		time.Sleep(1 * time.Second)
	}

	if counter%100 == 0 {
		log.Printf("Worker #%d inserted %d data", workerIndex, counter)
	}
}

func generateQuestionsMark(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, "?")
	}
	return s
}

func getIndexByHeader(header string) int {
	for index, value := range dataHeader {
		if value == header {
			return index
		}
	}
	return -1
}
