package main

import (
	"encoding/csv" //package for CSV file operations
	"fmt"          //package for formatting and printing
	"os"           //package for file system operations
	"strconv"      //package for string conversion
	"sync"         //package for synchronization primitives
	"time"         //package for measuring and displaying time
)

// concurrentQuickSort sorts an array of integers using the quicksort algorithm concurrently.
func concurrentQuickSort(arr []int, wg *sync.WaitGroup) {
	defer wg.Done() // Ensures that the WaitGroup counter is decremented when the function returns

	if len(arr) < 2 {
		return // Arrays with 0 or 1 element are already sorted
	}

	// Initializing left and right pointers for the partitioning process
	left, right := 0, len(arr)-1
	// Choosing the middle element as the pivot
	pivotIndex := len(arr) / 2
	// Swapping the pivot with the last element
	arr[pivotIndex], arr[right] = arr[right], arr[pivotIndex]

	// Partitioning process: elements less than pivot to the left, greater to the right
	for i := range arr {
		if arr[i] < arr[right] {
			arr[i], arr[left] = arr[left], arr[i]
			left++
		}
	}

	// Placing the pivot in its correct position
	arr[left], arr[right] = arr[right], arr[left]

	// Recursively sorting the left part of the array concurrently if it has more than one element
	if left > 1 {
		wg.Add(1)
		go concurrentQuickSort(arr[:left], wg)
	}
	// Recursively sorting the right part of the array concurrently if it has more than one element
	if right-left > 1 {
		wg.Add(1)
		go concurrentQuickSort(arr[left+1:], wg)
	}
}

// readCSV reads numbers from a CSV file into a slice of integers.
func readCSV(filename string) ([]int, error) {
	// Opening the CSV file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err // Returning an error if the file cannot be opened
	}
	defer file.Close() // Ensuring the file is closed when the function returns

	// Creating a new CSV reader
	reader := csv.NewReader(file)
	// Reading all records from the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err // Returning an error if the records cannot be read
	}

	var numbers []int // Slice to store the numbers
	// Iterating over the records
	for _, record := range records {
		if len(record) == 0 {
			continue // Skipping empty records
		}
		// Converting the first element of the record to an integer
		number, err := strconv.Atoi(record[0])
		if err != nil {
			return nil, err // Returning an error if the conversion fails
		}
		// Appending the number to the slice
		numbers = append(numbers, number)
	}

	return numbers, nil // Returning the slice of numbers
}

// writeCSV writes a slice of integers to a CSV file.
func writeCSV(filename string, numbers []int) error {
	// Creating a new CSV file
	file, err := os.Create(filename)
	if err != nil {
		return err // Returning an error if the file cannot be created
	}
	defer file.Close() // Ensuring the file is closed when the function returns

	// Creating a new CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush() // Ensuring all buffered data is written to the file when the function returns

	// Iterating over the numbers
	for _, number := range numbers {
		// Writing each number as a string to the CSV file
		if err := writer.Write([]string{strconv.Itoa(number)}); err != nil {
			return err // Returning an error if the write operation fails
		}
	}

	return nil // Returning nil if everything is successful
}

func main() {
	// Reading numbers from the CSV file
	numbers, err := readCSV("big-numbers.csv")
	if err != nil {
		fmt.Println("Error reading CSV:", err) // Printing an error message if the read operation fails
		return
	}

	wg := sync.WaitGroup{} // Creating a new WaitGroup
	wg.Add(1)              // Incrementing the WaitGroup counter

	start := time.Now() // Recording the start time
	// Starting the concurrent quicksort in a new goroutine
	go concurrentQuickSort(numbers, &wg)
	wg.Wait() // Waiting for all goroutines to finish

	// Calculating the duration of the sorting process
	duration := time.Since(start)
	// Printing the duration
	fmt.Printf("Sorting completed in %v\n", duration)

	// Writing the sorted numbers to a new CSV file
	err = writeCSV("sorted-numbers.csv", numbers)
	if err != nil {
		fmt.Println("Error writing CSV:", err) // Printing an error message if the write operation fails
		return
	}
}
