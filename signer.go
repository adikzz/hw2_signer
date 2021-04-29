package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

func JobWrapper(job_ job, in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	job_(in, out)
	close(out)
}

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}

	chanIn := make(chan interface{})
	for _, job_ := range jobs {
		wg.Add(1)
		chanOut := make(chan interface{})
		go JobWrapper(job_, chanIn, chanOut, wg)
		chanIn = chanOut
	}
	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	mutex := &sync.Mutex{}
	wgSH := &sync.WaitGroup{}

	for input := range in {

		data := fmt.Sprintf("%v", input)
		wgSH.Add(1)

		go func(data string, mutex *sync.Mutex, wgSH *sync.WaitGroup) {
			defer wgSH.Done()

			mutex.Lock()
			md5Result := DataSignerMd5(data)
			mutex.Unlock()

			crc1 := make(chan string)
			go func(data string, outChan chan string) {
				crc32FirstResult := DataSignerCrc32(data)
				outChan <- crc32FirstResult
			}(data, crc1)

			crc2 := make(chan string)
			go func(data string, outChan chan string) {
				crc32SecondResult := DataSignerCrc32(data)
				outChan <- crc32SecondResult
			}(md5Result, crc2)

			crc32FirstResult := <-crc1
			crc32SecondResult := <-crc2

			out <- crc32FirstResult+"~"+crc32SecondResult
		}(data, mutex, wgSH)
	}
	wgSH.Wait()
}

func OneMultiHash(data string, wgMH *sync.WaitGroup, out chan interface{}) {
	defer wgMH.Done()

	wgOMH := &sync.WaitGroup{}
	result := [6]string{}
	for th := 0; th < 6; th++ {
		wgOMH.Add(1)

		go func(data string,  th int){
			defer wgOMH.Done()

			res := DataSignerCrc32(fmt.Sprintf("%d%s", th, data))
			result[th] = res
		}(data, th)
	}
	wgOMH.Wait()
	out <- strings.Join(result[:], "")
}

func MultiHash(in, out chan interface{}) {
	wgMH := &sync.WaitGroup{}

	for data := range in {
		d := fmt.Sprintf("%v", data)
		wgMH.Add(1)
		go OneMultiHash(d, wgMH, out)
	}

	wgMH.Wait()
}

func CombineResults(in, out chan interface{}) {
	results := make([]string, 0)
	for input := range in {
		results = append(results, fmt.Sprintf("%s", input))
	}
	sort.Strings(results)
	out <- strings.Join(results, "_")
}

