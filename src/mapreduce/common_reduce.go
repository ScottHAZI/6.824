package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	var kv KeyValue
	var kvMap = make(map[string][]string)
	for m := 0; m < nMap; m++ {
		rn := reduceName(jobName, m, reduceTaskNumber)
		intrFile, err := os.Open(rn)
		if err != nil {
			fmt.Printf("fail to open intermediate file %s\n", rn)
			continue
		}
		dec := json.NewDecoder(intrFile)
		for err := dec.Decode(&kv); err == nil; err = dec.Decode(&kv) {
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		err = intrFile.Close()
		if err != nil {
			fmt.Printf("fail to close intermediate file %s\n", rn)
		}
	}
	outputFile, err := os.Create(outFile)
	// map and reduce function output appears atomically:
	// the output file will either not exist, or will contain the entire
	// output of a single execution of the map or reduce function
	outputBuf := bufio.NewWriter(outputFile)
	if err != nil {
		fmt.Printf("fail to create output file %s\n", outFile)
		return
	}
	var keys []string
	for key := range kvMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	enc := json.NewEncoder(outputBuf)
	for _, key := range keys {
		err := enc.Encode(KeyValue{key, reduceF(key, kvMap[key])})
		if err != nil {
			fmt.Printf("fail to encode key-value pair into output file %s\n", outFile)
		}
	}
	outputBuf.Flush()
	err = outputFile.Close()
	if err != nil {
		fmt.Printf("fail to close output file %s\n", outFile)
	}
}
