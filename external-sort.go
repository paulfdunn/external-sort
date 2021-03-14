// This project is hosted at https://github.com/paulfdunn/external-sort.
// Please see the README there for a description of the problem this code solves.
// Summary - this is an external sort; used to sort very large, \n delimited, text files.
package main

import (
	"bufio"
	"container/heap"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type mergedChunkedWorkItem struct {
	filePaths []string
	id        string
}

type sortHeap []sortHeapItem
type sortHeapItem struct {
	fileIndex int
	value     string
}

type exitErrorN int

const (
	exitResetError exitErrorN = iota + 1
	exitDirCreateError
	exitInputFileCreateError
	exitProcessedFilesGetError
	exitProcessedFilesSaveError
	exitProcessInputFilesError
	exitProcessChunkedFilesError
)

const (
	// inputLineBuffer is the channel buffer for feeding data from input files to sort/save threads.
	// May need optimized; highly dependent on persistent storage speed vs host capability.
	inputLineBuffer = 1000
	// Linux generally limits the number of open files for a process to 1024; leave plenty of margin.
	// Changing the linux default and increasing this value is likely to increase performance.
	maxOpenFiles = 800
	// writeBufferSize is the buffer size used when writing files. May need optimized; highly
	// dependent on persistent storage speed
	writeBufferSize = 4096
)

var (
	defaultWorkingDir = filepath.Join(os.TempDir(), "external-sort")
	defaultInputDir   = filepath.Join(defaultWorkingDir, "input")
	inputDir          = flag.String("inputdir", defaultInputDir,
		fmt.Sprintf("Directory containing input files, defaults to: %s", defaultInputDir))
	inputFile  = "testInput.txt"
	workingDir = flag.String("workingdir", defaultWorkingDir,
		fmt.Sprintf("Directory containing output files, defaults to: ./%s", defaultWorkingDir))
	reset               = flag.Bool("reset", false, "Reset, defaults false, use true to delete all persisted output data")
	threads             = flag.Int("threads", 8, "Threads to run while processing data")
	defaultThresoldSize = int64(10e6)
	testFileEntries     = flag.Int64("testfileentries", 0, "If non-zero, generates an file with this number of entries, then runs.")
	thresholdSize       = flag.Int64("thresholdsize", defaultThresoldSize,
		fmt.Sprintf("Threshold size determines the output file size, in bytes. "+
			"The actual output will be up to one entry longer than this value. "+
			"Default: %d", defaultThresoldSize))
	// Output directories
	chunkedSortedDir   = filepath.Join(*workingDir, "chunked-sorted")
	hierarchyDir       = filepath.Join(*workingDir, "hierarchy")
	mergedSortedDir    = filepath.Join(*workingDir, "merged-sorted")
	processedInputDir  = filepath.Join(*workingDir, "processed-input")
	processedFilesPath = filepath.Join(*workingDir, "processed-inputs.txt")
	allOutputDirs      = []string{*workingDir, chunkedSortedDir, mergedSortedDir,
		processedInputDir, hierarchyDir}

	// Format statements to generate file names.
	chunkedSortedFileNameFmt = filepath.Join(chunkedSortedDir, "chunked-sorted_%d.txt")
	mergedSortedFileNameFmt  = filepath.Join(mergedSortedDir, "merged-sorted_%s.txt")

	// Names of input files processed in prior runs.
	processedFiles []string
)

func main() {
	var err error
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Error: %+v\n%+v\n", err, string(debug.Stack()))
		}
	}()

	flag.Parse()
	if reset != nil && *reset {
		err := doReset()
		if err != nil {
			os.Exit(int(exitResetError))
		}
	}
	if *inputDir == defaultInputDir {
		err := makeDirs([]string{defaultInputDir})
		if err != nil {
			os.Exit(int(exitDirCreateError))
		}
	}
	err = makeDirs(allOutputDirs)
	if err != nil {
		os.Exit(int(exitDirCreateError))
	}
	if threads != nil && *threads <= 0 {
		one := 1
		threads = &one
		fmt.Println("Warning: threads was provided with a value <=0; it has been modified to 1.")
	}
	if testFileEntries != nil && *testFileEntries > 0 {
		err := makeTestFile(*testFileEntries, filepath.Join(*inputDir, inputFile))
		if err != nil {
			os.Exit(int(exitInputFileCreateError))
		}
	}

	processedFiles, err = processedFilesGet()
	if err != nil {
		os.Exit(int(exitProcessedFilesGetError))
	}

	err = chunkInputFiles()
	if err != nil {
		os.Exit(int(exitProcessInputFilesError))
	}

	err = mergeChunkedFiles()
	if err != nil {
		os.Exit(int(exitProcessChunkedFilesError))
	}

	processedFilesSave(processedFiles)
	if err != nil {
		os.Exit(int(exitProcessedFilesSaveError))
	}
}

func (h sortHeap) Len() int           { return len(h) }
func (h sortHeap) Less(i, j int) bool { return h[i].value < h[j].value }
func (h sortHeap) Swap(i, j int) {
	h[i].fileIndex, h[j].fileIndex, h[i].value, h[j].value =
		h[j].fileIndex, h[i].fileIndex, h[j].value, h[i].value
}

func (h *sortHeap) Push(x interface{}) {
	// Pointer receiver required to modify slice.
	*h = append(*h, x.(sortHeapItem))
}

func (h *sortHeap) Pop() interface{} {
	// Pointer receiver required to modify slice.
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func chunkInputFiles() error {
	outputFiles, err := outputFileNames(chunkedSortedFileNameFmt, *inputDir)
	if err != nil {
		return err
	}
	filePathChan := make(chan string, len(outputFiles))
	for i := range outputFiles {
		filePathChan <- outputFiles[i]
	}
	lineChan := make(chan string, inputLineBuffer)
	errorChan := make(chan error, *threads)
	var wg sync.WaitGroup
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func(lines <-chan string, filePath <-chan string, errc chan<- error, wg *sync.WaitGroup) {
			chunkInputFilesWorker(lines, filePath, errc, wg)
		}(lineChan, filePathChan, errorChan, &wg)
	}

	// Assumption - A single thread can feed all sorting threads. Needs verified. May need optimized.
	// ReadDir returns files sorted by name, use os.File.ReadDir if this is not desireable.
	files, err := ioutil.ReadDir(*inputDir)
	if err != nil {
		fmt.Printf("Error: getting input file list, error: %+v\n", err)
		return err
	}
	for _, file := range files {
		fpath := filepath.Join(*inputDir, file.Name())
		found := false
		for i := range processedFiles {
			if file.Name() == processedFiles[i] {
				found = true
				break
			}
		}
		if found {
			fmt.Printf("skipping previously processed input file: %s\n", fpath)
			continue
		}

		fmt.Printf("processing input file: %s\n", fpath)
		f, err := os.Open(fpath)
		if err != nil {
			fmt.Printf("Error: opening input file: %s, error: %+v\n", fpath, err)
			return err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			if len(scanner.Bytes()) >= int(*thresholdSize) || len(scanner.Bytes()) == 0 {
				continue
			}
			lineChan <- scanner.Text() + "\n"
		}
		if err := scanner.Err(); err != nil {
			fmt.Printf("Error: reading input file, error: %+v\n", err)
			return err
		}

		processedFiles = append(processedFiles, file.Name())
		os.Rename(fpath, filepath.Join(processedInputDir, file.Name()))
	}
	fmt.Println("All input files scanned.")
	close(lineChan)

	wg.Wait()
	close(errorChan)
	errorString := []string{}
	for err := range errorChan {
		errorString = append(errorString, fmt.Sprintf("%+v", err))
	}
	if len(errorString) > 0 {
		return fmt.Errorf("%s", strings.Join(errorString, "|"))
	}

	return nil
}

// chunkInputFilesWorker should run in a GO routine, and will take up to thresholdSize lines of input
// (in bytes), sort (in memory), and save to a file name pulled from filePathChan. Input lines that
// are blank are dropped.
func chunkInputFilesWorker(lines <-chan string, filePathChan <-chan string,
	errc chan<- error, wg *sync.WaitGroup) {
	buf := []string{}
	bytes := int64(0)

	for line := range lines {
		// Drop blanks; duplicates cannot yet be determined as the data is not yet sorted.
		if line == "" {
			continue
		}
		bytes += int64(len(line))
		buf = append(buf, line)
		if bytes >= *thresholdSize {
			fp := <-filePathChan
			err := dataBufSave(fp, buf, true)
			if err != nil {
				errc <- err
				break
			}
			buf = []string{}
			bytes = 0
		}
	}
	if len(buf) != 0 {
		fp := <-filePathChan
		err := dataBufSave(fp, buf, true)
		if err != nil {
			errc <- err
		}
	}
	wg.Done()
}

// dataBufSave will save a buffer to the specified filepath; doing an in-memory sort prior to
// saving, if requested. The input buffer may or may not be impacted, depending on how the
// functions; assume the input buffer is modified.
func dataBufSave(filepath string, buf []string, doSort bool) error {
	f, err := os.Create(filepath)
	if err != nil {
		fmt.Printf("Error: creating input file, error: %+v\n", err)
		return err
	}
	defer f.Close()
	if doSort {
		sort.Strings(buf)
	}
	_, err = f.WriteString(strings.Join(buf, ""))
	if err != nil {
		fmt.Printf("Error: writing input file, error: %+v\n", err)
		return err
	}

	f.Close()
	return nil
}

// doReset will delete all output directories in workingDir.
func doReset() error {
	err := os.RemoveAll(*workingDir)
	if err != nil {
		fmt.Printf("Error: deleting directory: %s, error: %+v\n", *workingDir, err)
		return err
	}
	return nil
}

// makeDirs will create all directories, including parents if required, in dirs.
func makeDirs(dirs []string) error {
	for i := range dirs {
		err := os.MkdirAll(dirs[i], 0777)
		if err != nil {
			fmt.Printf("Error: creating directory: %s, error: %+v\n", dirs[i], err)
			return err
		}
	}
	return nil
}

// makeHierarchy will compare two tokens (after removing leading/trailing whitespace), find
// the first different character, then create nested directories under baseDir for each leading
// character that is the same.
// The return value is a path of the created directories and filename of the currentToken
// starting at the first different character. (Think page headings in a dictionary, and the
// first word on the page.)
// I.E. currentToken=aab123, nextToken=aac456, the result is creating /baseDir/a/a and returning
// a string "/baseDir/a/a/b123"
func makeHierarchy(baseDir string, currentToken string, nextToken string) (string, error) {
	ct := strings.TrimSpace(currentToken)
	if len(ct) == 0 {
		err := fmt.Errorf("makeHierarchy currentToken was zero length")
		fmt.Printf("%+v\n", err)
		return "", err
	}
	nt := strings.TrimSpace(nextToken)
	indexNE := 0
	min := len(nt)
	if min > len(ct) {
		min = len(ct)
	}
	for indexNE = 0; indexNE < min; indexNE++ {
		if nt[indexNE] != ct[indexNE] {
			break
		}
	}
	difChars := string(ct[0])
	if indexNE > 0 {
		difChars = ct[0:indexNE]
	}
	pth := filepath.Join(baseDir, filepath.Join(strings.Split(difChars, "")...))
	pths := []string{pth}
	err := makeDirs(pths)
	return filepath.Join(pth, ct[indexNE:]), err
}

// makeTestFile will make an input file for testing, of length entries, that is filled with random
// 32 character strings.
func makeTestFile(entries int64, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("Error: creating input file, error: %+v\n", err)
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, writeBufferSize)
	for i := int64(0); i < entries; i++ {
		id, _ := uniqueID(false)
		_, err := w.WriteString(fmt.Sprintf("%+v\n", id))
		if err != nil {
			fmt.Printf("Error: writing input file, error: %+v\n", err)
			return err
		}
	}
	err = w.Flush()
	if err != nil {
		fmt.Printf("Error: flushing input file, error: %+v\n", err)
		return err
	}

	return err
}

// mergeChunkedFiles will process input files (chunked into thresholdSize, and pre-sorted), into less
// than threads number of output files. A heap is build from the first line of each file, the heap POP'd,
// that line written to the output file, a line added to the heap from the source of the last POP'd
// line, and this repeated, until all input files have been combined into sorted output.
func mergeChunkedFiles() error {
	var wg sync.WaitGroup
	dirs := []string{chunkedSortedDir, mergedSortedDir}
	id := 0
	for _, dir := range dirs {
		for {
			files, err := ioutil.ReadDir(dir)
			if err != nil {
				fmt.Printf("Error: getting input file list, error: %+v\n", err)
				return err
			}
			if (dir == chunkedSortedDir && len(files) == 0) || (dir == mergedSortedDir && len(files) <= 1) {
				break
			}
			fmt.Printf("mergeChunkedFiles processing %d files from directory: %s\n", len(files), dir)

			mergedChunkedWorkItemChan := make(chan mergedChunkedWorkItem, *threads)
			errorChan := make(chan error, *threads)
			mergeChunkedFilesWorkerStart(mergedChunkedWorkItemChan, errorChan, &wg)

			// fps is a slice of paths to all files needing processed.
			fps := make([]string, len(files))
			for i := range files {
				fps[i] = filepath.Join(dir, files[i].Name())
			}
			filesPerThread := 1 + len(files)/(*threads)
			// TODO: upate this so that threads number of output files remain, to enable new data
			// to be added more efficiently.
			if filesPerThread > maxOpenFiles {
				filesPerThread = maxOpenFiles
			} else if filesPerThread <= 1 {
				filesPerThread = 2
			}
			for i := 1; i <= *threads; i++ {
				if filesPerThread > len(fps) {
					filesPerThread = len(fps)
				}
				// fmt.Printf("mergeChunkedFiles threads: %d, filesPerThread: %d\n", *threads, filesPerThread)
				mcwi := mergedChunkedWorkItem{fps[0:filesPerThread], strconv.Itoa(id)}
				mergedChunkedWorkItemChan <- mcwi
				// id is a unique ID for each output file,
				id++
				fps = fps[filesPerThread:]
				if len(fps) == 0 {
					break
				}
			}

			close(mergedChunkedWorkItemChan)

			wg.Wait()
			close(errorChan)
			errorString := []string{}
			for err := range errorChan {
				errorString = append(errorString, fmt.Sprintf("%+v", err))
			}
			if len(errorString) > 0 {
				return fmt.Errorf("%s", strings.Join(errorString, "|"))
			}
		}
		fmt.Printf("mergeChunkedFiles processed dir: %s\n", dir)
	}

	return nil
}

// mergeChunkedFilesWorkerStart starts the workers in GO routines.
func mergeChunkedFilesWorkerStart(mergedChunkedWorkItemChan <-chan mergedChunkedWorkItem,
	errc chan<- error, wg *sync.WaitGroup) {
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func(instance int, mergedChunkedWorkItemChan <-chan mergedChunkedWorkItem,
			errc chan<- error, wg *sync.WaitGroup) {
			mergeChunkedFilesWorker(instance, mergedChunkedWorkItemChan, errc)
			wg.Done()
		}(i, mergedChunkedWorkItemChan, errc, wg)
	}
}

// mergeChunkedFilesWorker should be called in a GO routine and is used to do the work of
// mergeChunkedFiles.
func mergeChunkedFilesWorker(instance int, mergedChunkedWorkItemChan <-chan mergedChunkedWorkItem,
	errc chan<- error) {
	for mcwi := range mergedChunkedWorkItemChan {
		fo, err := os.Create(fmt.Sprintf(mergedSortedFileNameFmt, mcwi.id))
		if err != nil {
			nerr := fmt.Errorf("creating output file, error: %+v", err)
			errc <- nerr
			return
		}
		w := bufio.NewWriterSize(fo, writeBufferSize)

		//build a min-heap of the first item from each file, pop min from the heap into a buffer,
		//repeat and save output to file
		scanners := make([]*bufio.Scanner, len(mcwi.filePaths))
		fileHandles := make([]*os.File, len(mcwi.filePaths))
		hp := &sortHeap{}
		heap.Init(hp)
		for i := range mcwi.filePaths {
			// fmt.Printf("processing input file: %s\n", mcwi.filePaths[i])
			fileHandles[i], err = os.Open(mcwi.filePaths[i])
			if err != nil {
				fmt.Printf("Error: opening input file: %s, error: %+v\n", mcwi.filePaths[i], err)
				errc <- err
				return
			}
			scanners[i] = bufio.NewScanner(fileHandles[i])
			scanners[i].Scan()
			if err := scanners[i].Err(); err != nil {
				fmt.Printf("Error: reading input file, error: %+v\n", err)
				errc <- err
				return
			}
			shi := sortHeapItem{i, scanners[i].Text() + "\n"}
			heap.Push(hp, shi)
		}

		buf := []string{}
		bytes := int64(0)
		for {
			if hp.Len() == 0 {
				break
			}
			smallest := heap.Pop(hp).(sortHeapItem)
			buf = append(buf, smallest.value)
			bytes += int64(len(smallest.value))
			if bytes > *thresholdSize || hp.Len() == 0 {
				startToken := buf[0]
				nextToken := ""
				if len(buf) >= 2 {
					nextToken = buf[1]
				}
				fp, err := makeHierarchy(hierarchyDir, startToken, nextToken)
				if err != nil {
					errc <- err
					return
				}
				dataBufSave(fp, buf, false)
				buf = []string{}
				bytes = 0
			}

			_, err := w.WriteString(smallest.value)
			if err != nil {
				nerr := fmt.Errorf("writing input file, error: %+v", err)
				errc <- nerr
				return
			}

			scanners[smallest.fileIndex].Scan()
			if err := scanners[smallest.fileIndex].Err(); err != nil {
				fmt.Printf("Error: reading input file, error: %+v\n", err)
				errc <- err
				return
			} else if len(scanners[smallest.fileIndex].Bytes()) == 0 {
				continue
			}

			shi := sortHeapItem{smallest.fileIndex, scanners[smallest.fileIndex].Text() + "\n"}
			heap.Push(hp, shi)
		}

		err = w.Flush()
		if err != nil {
			nerr := fmt.Errorf("flushing output file, error: %+v", err)
			errc <- nerr
			return
		}
		fo.Close()
		for i := range mcwi.filePaths {
			os.Remove(mcwi.filePaths[i])
			fileHandles[i].Close()
		}
	}
}

// processedFilesGet gets a list of files names that were already processed. These files will
// not be processed again.
func processedFilesGet() ([]string, error) {
	if _, err := os.Stat(processedFilesPath); os.IsNotExist(err) {
		return []string{}, nil
	}

	b, err := ioutil.ReadFile(processedFilesPath)
	if err != nil {
		fmt.Printf("Error: reading file: %s, error: %+v\n", processedFilesPath, err)
		return nil, err
	}

	pf := []string{}
	err = json.Unmarshal(b, &pf)
	if err != nil {
		fmt.Printf("Error: unmarshalling json, error: %+v\n", err)
		return nil, err
	}

	return pf, nil
}

// processedFilesSave persists the list of processed input file names as a JSON object.
func processedFilesSave(processedFiles []string) error {
	b, err := json.Marshal(processedFiles)
	if err != nil {
		fmt.Printf("Error: marshalling processedFiles, error: %+v\n", err)
		return err
	}

	err = ioutil.WriteFile(processedFilesPath, b, 0666)
	if err != nil {
		fmt.Printf("Error: writing processedFiles, error: %+v\n", err)
		return err
	}

	return nil
}

// outputFileNames will generate a slice of files names used to store the output of the input
// chunking process. These need generated in advance, since the chunking is occuring in threads,
// which don't have visibility to what file(s) any other thread generated. A more complicated
// alternative would be to have threads communcate about generated file names, but that overcomplicates
// things.
func outputFileNames(format string, dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Printf("Error: getting input file list, error: %+v\n", err)
		return nil, err
	}
	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size()
	}
	// Each thread will require additional files due to rounding error and input not breaking
	// on increments of threshold-size.
	numOutputFiles := int64(*threads+1) + (1 + totalSize / *thresholdSize)
	fmt.Printf("input files size: %d, resulting files:~ %d\n", totalSize, numOutputFiles)
	names := make([]string, numOutputFiles+1)
	for i := int64(0); i < numOutputFiles; i++ {
		names[i] = fmt.Sprintf(format, i+1)
	}

	return names, nil
}

// uniqueID is used to generate 16 byte (32 character) ID's; as a UUID (includeHuphens) or
// hex string. Note these are hex strings; they do not include all alphanumeric characters.
func uniqueID(includeHyphens bool) (id string, err error) {
	idBin := make([]byte, 16)
	_, err = rand.Read(idBin)
	if err != nil {
		err := fmt.Errorf("creating unique binary ID, error: %+v", err)
		fmt.Printf("%+v\n", err)
		return "", err
	}

	if includeHyphens {
		return fmt.Sprintf("%x-%x-%x-%x-%x", idBin[0:4], idBin[4:6], idBin[6:8], idBin[8:10], idBin[10:]), err
	}

	return fmt.Sprintf("%x", idBin[:]), err
}
