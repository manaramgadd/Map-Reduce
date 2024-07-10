package mapreduce

import (
	"encoding/json"
	//"fmt"
	"hash/fnv"
	"os"
	"slices"
	//"path/filepath"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file assigned to this task
	nReduce int, // The number of reduce tasks that will be run
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// ToDo: Write this function. See description in assignment.
	/////my steps
	//read the filename from the path and the contents of the files
	//split contents into an array of words and for each word, it produce a key value pair
	//key is the word
	//value =1
	//emit(word,1)
	//the final output from mapf function is a array of key value pairs
	//file_name := filepath.Base(inputFile)
	//fmt.Println("filename", file_name)
	c, _ := os.ReadFile(inputFile)
	//fmt.Println(string(c))
	kv := mapFn(inputFile, string(c))

	var encoder *json.Encoder
	var fileslist []*os.File
	for i := 0; i < nReduce; i++ {
		f := getIntermediateName(jobName, mapTaskIndex, i)
		fi, err := os.Create(f)
		fileslist = append(fileslist, fi)
		if err != nil {
			//  fmt.Println("error creating intermediate file")
		} else {
			//	fmt.Println("intermediate file created", f)
			encoder = json.NewEncoder(fi)

		}
		defer fi.Close()
	}
	for i, k := range kv {
		//fmt.Println("hi")

		hashv := (int)(hash32(k.Key)) % nReduce

		//fmt.Println(hashv)
		//fmt.Println("hi")

		//fmt.Println(f)
		//f := getIntermediateName(jobName, mapTaskIndex, hashv)
		//fileslist = append(fileslist, f)
		if hashv < len(fileslist) {
			encoder = json.NewEncoder(fileslist[hashv])
			//fmt.Println(fileslist[hashv])
			e := encoder.Encode(&k)
			if e != nil {
				panic(e)
			} else {
				//fmt.Println("done")
			}
			if i < len(fileslist) {
				defer fileslist[i].Close()
			}
		}

		//  fi, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)

	}

	//fmt.Println(fileslist)

}

//fmt.Println("hi")
//fmt.Println(a[hashv])

//var enc *json.Encoder

// which file a given key belongs into.
func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks that were run
	reduceFn func(key string, values []string) string,
) {
	//files := make(map[string]*os.File)
	//due to the error panic: assignment to entry in nil map I have to intialize it with zero
	mkv := make(map[string][]string, 0)
	outputfilename := getReduceOutName(jobName, reduceTaskIndex)
	//Ofile_name := filepath.Base(outputfilename)
	//fmt.Println("out:", Ofile_name)

	Outfile, err := os.Create(outputfilename)
	if err != nil {
		//fmt.Println("Could not create file")
	}
	//Ofile, _ := os.Open(Ofile_name)
	for i := 0; i < nMap; i++ {
		f := getIntermediateName(jobName, i, reduceTaskIndex)
		file, _ := os.Open(f)
		decoder := json.NewDecoder(file)

		//data, _ := os.ReadFile(f)
		//err := decoder.Decode(&data)
		// err != nil {
		//  panic(err)

		//}
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {

				break
			}
			mkv[kv.Key] = append(mkv[kv.Key], kv.Value)
		}

	}
	enc := json.NewEncoder(Outfile)
	var s []string
	for key := range mkv {
		s = append(s, key)

	}
	slices.Sort(s)

	for _, kv := range s {

		enc.Encode(KeyValue{(kv), reduceFn(kv, mkv[kv])})

		//fmt.Println("en:", en)
	}
	//outputfilename=append(, outputfilename)

	defer Outfile.Close()
	// ToDo: Write this function. See description in assignment.

}
