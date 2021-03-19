# external-sort
This code was developed as part of an interview process. It implements an external sort of text files.

Summary of the utility of this application. A very large input text file(s) is broken down into small "chunks" that can be sorted in memory (concurrently), then saved to individual files. These chunks are then merged (concurrently) into larger files, using an in-memory heap to take the smallest elements from the top of the pre-sorted chunks. Thus a very large sort can be accomplished; where space is limited by persistent storage (disk), not RAM.

The context of this repo is as a "take home test" for an interview. My take of the expectations is this: better than white board code, but less than a full production app. In particular, tests will be skipped, or minimal at best.
## Status
I hit the (arbitrary) time limit I set for working on a problem that was provided as part of an interview. It generally appears to function and solve the problem. But I did not write tests, as that was going to be significant additional work, and I felt the code written is sufficient for interview purposes.  
## Problem Statement (as provided)
Please write an algorithm in Golang or Rust, utilizing concurrency, that sorts the contents of, /n delimited, txt files into nested alphanumeric files at a specified directory. If any file reaches a threshold size, create a folder with that index and sort by the subsequent character into subfiles. Internally sort all files alphanumerically. Finally, determine if an input file has already been sorted.
## Problem Restatement
There is some ambiguity in the statement as written, so here is my interpretation and additional statements of intent, as well as limitations.
* Assumptions and limitations
    * There is plenty of disk space; enough to hold at least 3x the combined size of input files. (3x because there are: the inputn (I'm moving/saving these as they are processed), intermedidate output files, and the final sort. By saving the final sort, more input files can be processed and added to the output.)
    * The threshold-size, combined with threads, is really a way of capping memory (RAM) use. Thus intermediate (sorted) output files will be of size (threshold_size/(threads + 1)), to limit in-use memory to ~threhsold_size.
    * Empty lines will be dropped, as will duplicates. This is a simplication to eliminate the corner cases where a single repeating entry is larger than threshold size, in which case it would not be clear what the directory or file name output should be. Plus I think this is fitting with the problem. The application of this code likely involves storing user IDs, keys for a hash map, etc., and thus you don't want duplicates. If duplicates were desired, this could be implemented with a sentinal character/string at EOL followed by a count, and some filename schema that indicated a repeat.
    * If any single line is longer than threshold-size, print an error and drop that line. Another elimination of a corner case, that should likely only exist for corrupt files or malformed input.
* Write a Golang CLI program that takes inputs of:
    * input-directory - Contains one of more input text files, \n delimited.
    * output-directory - Multiple directories are here for: saving processed input files, saving a list of processed files as JSON, chunking input files, merging chunks, and the hierarchy.
    * reset - Remove the output directory and all contents; re-created on start.
    * threads - number of threads (GO routines) to run.
    * threshold-size - Maximum file size (bytes) for files in the output-directory. Note this is ~maximum; some files may be smaller. It is approximately maximum, as the algorithm does not back-track; it processes input until the threshold is exceeded, writes that output, then starts a new file. So the maximum is really thresholld-size + (the maximum on any input token).
* Implementation notes:
    * Multiple calls using the same output-directory will add to existing output.
    * Provide additional input parameter(s) that will generate example input, for testing.
* Additional considerations for a production app that I am not going to consider further:
    * The application will keep a history of input filenames, and does not re-process the same **file name**. If input filenames are not guaranteed to be unique, other information needs persisted. Options: filename and created/modified time, filename and size, or filename and checksum (most robust, but also most expensive.)
    * Delete input files after successful processing? Maybe, do they exist somewhere else? Is history needed? I will move them when processed.
    * The list of processed files is kept in memory. I'm assuming the number of files is small. If that is not the case, then this needs additional work.
    * There are many notes in the code about potential optimizations; these need considered. Optimizing performance is going to be hardware specific, and a function of: threads, threshold-size, default linux file size and optimizing the maxOpenFiles contstant, channel buffer sizes, etc. Also, native GO sort and heap efficiency need investigated. See code notes.
## Strategy
* Using threads number of GO routines, read in (threshold_size/(threads + 1)) portions of unprocessed input file(s), sort (in memory using GO sort package), and write to chunked/sorted output files. 
    * Repeat until all input files processed.
    * Move input files into a processed folder (This is safe, but the use case may allow deleting them.)
* Open up to maxOpenFiles chunked/sorted files per GO routine, build a min-heap of the first item from each file, pop min from the heap and write to output file. Note these files are NOT limited to threshold size. Remove a token from the file from which the POP'ed item came and add to the heap.
    * Repeat this step until all chunked/sorted files are processed.
    * When done there there be threads number, or fewer, of merged/sorted files.
    * Keep merged/sorted files, so additional input can be added.
    * Once there are threads or fewer files, write output into a nested alphanumeric hierarchy.
* Persist input file names as JSON list.
* Processing additional files is just a matter of verifying the file is not in the persisted list, and if not, repeating this process. 
## Output
*...nested alphanumeric files at a specified directory... create a folder with that index and sort by the subsequent character into subfiles*

I'll use an example to demonstrate how I am reading this. For this sorted input, where threshold size is 8 bytes (disregarding line feed):
```
aaaa
aaab
aabb
aabc
defg
hijk
lmno
pqrs
```
The resulting file structure will look like:
```
./a
    file with contents "aaaa\naaab\n"
    ./a
    ./b
        file with contents "aabb\naabc\n" 
./d
    file with contents "defg\nhijk\n"
./l       
    file with contents "lmno\npqrs\n"
```
I *think* this is what is being asked for, but am not certain. I'm also generating a single output file.
## Concerns
This seems like a good approach, but I'm not confident it couldn't be more optimal. I spent enough time getting this far, and for the purpose of an interview coding exercise, I feel this is good enough. For something mission critical, I'd do more research, while using this as a starting point.
* Are the sort and heap packages efficient?
* Are there properties of the input data that make different sorting algorithms better for this application?
* Is the input data provided in the most efficient manner for this application? I.E. Is it random, when sequential would allow for faster sorting? (Random data is desireable in applications where the possible code space is much larger than the used code space, as corrupttion can be detected. But sequential data would allow faster processing.)
* I will either skip tests entirely, or have something very minimal, as the testing alone to verify production quality would be a significant amount of work.
## Requirements
* You need to have docker (engine and compose) installed. This was tested with client 20.10.1 and server 19.03.13.
* The application can be run in a docker container, or built and run on your host. The later will require having GOLANG installed and familiarity with building GO apps (tested with GO version 1.14.14).
## Install
Create a local directory in which to clone the application, cd to that directory, and clone the repo.
```mkdir SOME_DIR; 
cd SOME_DIR; 
git clone https://github.com/paulfdunn/external-sort.git
```
(Optional) If you get an error 'Got permission denied while trying to connect to the Docker daemon socket' running any docker related commands, look in this file for details regarding running as non-root user, and execute the script if necessary.
```
./dockersetup.sh
```
## Using the application
Here is output from an example terminal session using the Docker container. This example: builds a Docker container, runs external-sort, generating its own test data, and dumps the head/tail of the output.
```
paulfdunn@penguin:~/go/src/external-sort$ docker build -t external-sort:external-sort .
Sending build context to Docker daemon  3.276MB
Step 1/7 : FROM golang:1.14.14-buster
 ---> af936ae559f6
Step 2/7 : COPY ./ /go/src/external-sort/
 ---> 8416a7319e34
Step 3/7 : WORKDIR /go/src/external-sort/
 ---> Running in 57d7144fc2d0
Removing intermediate container 57d7144fc2d0
 ---> 1ab763e16b8a
Step 4/7 : RUN go test -v ./... >test.log 2>&1
 ---> Running in 45061592978b
Removing intermediate container 45061592978b
 ---> 04e4f5f716b7
Step 5/7 : RUN CGO_ENABLED=0 GOOS=linux go build
 ---> Running in 313bee130df7
Removing intermediate container 313bee130df7
 ---> e5e7c87af8e9
Step 6/7 : RUN apt-get update -y
.
.
.
Removing intermediate container 6f90ad9138c4
 ---> e324c9b29e80
Successfully built e324c9b29e80
Successfully tagged external-sort:external-sort
paulfdunn@penguin:~/go/src/external-sort$ docker run -it  --name external-sort external-sort:external-sort /bin/bash
root@5faa452d2d70:/go/src/external-sort# ./external-sort -testfileentries 10000 -thresholdsize 5000 -reset
input files size: 330000, resulting files:~ 75
processing input file: /tmp/external-sort/input/testInput.txt
All input files scanned.
mergeChunkedFiles processing 69 files from directory: /tmp/external-sort/chunked-sorted
mergeChunkedFiles processed dir: /tmp/external-sort/chunked-sorted
mergeChunkedFiles processing 8 files from directory: /tmp/external-sort/merged-sorted
mergeChunkedFiles processing 4 files from directory: /tmp/external-sort/merged-sorted
mergeChunkedFiles processing 2 files from directory: /tmp/external-sort/merged-sorted
mergeChunkedFiles processed dir: /tmp/external-sort/merged-sorted
root@5faa452d2d70:/go/src/external-sort# ls -al /tmp/external-sort/chunked-sorted/
total 0
drwxr-xr-x 1 root root   0 Mar 13 18:43 .
drwxr-xr-x 1 root root 152 Mar 13 18:43 ..
root@5faa452d2d70:/go/src/external-sort# ls -al /tmp/external-sort/merged-sorted/ 
total 324
drwxr-xr-x 1 root root     40 Mar 13 18:43 .
drwxr-xr-x 1 root root    152 Mar 13 18:43 ..
-rw-r--r-- 1 root root 330000 Mar 13 18:43 merged-sorted_14.txt
root@5faa452d2d70:/go/src/external-sort# head /tmp/external-sort/merged-sorted/merged-sorted_14.txt
0005361d3d7160d44414de5762d409d5
0009af510ace305ef78df4df7c12dc2e
000cba10ba612ffb6a4eb9b8e8bf8251
00166cf888f435172dfcc0bfb04c3169
001ed77f9de72f5f1e663f8b9c3d51fa
0022d6e77d4e9442b26fcf2138d2c927
002fdd5cf8c39c812f457506f774f5fd
0032dbabb9c10cd244073e782e0d909c
0045529139cad93f89cd65fe3197d2fd
004574e162336c02422497106a7d3a8e
root@5faa452d2d70:/go/src/external-sort# tail /tmp/external-sort/merged-sorted/merged-sorted_14.txt
ffba59bd583fc3a9c1bd1ade245992e5
ffbe3c3df9ffba398b832d20ace42269
ffc59a7593f536c9e435149b55d4e328
ffc9aa761de8b0e2d4de202ffed71f77
ffcbb166d58f0da961ad3c3c7e814ba2
ffd5bedaf9b861292b7c4868ece3fed7
ffda8eeb4efe05ccce3f266cba4729da
fff11495365d54c2d80d498f032ecde8
fffd920c218229c801aa7ebeaa3a6e27
ffffc671a80474639c959e717da665f8
root@5faa452d2d70:/go/src/external-sort# exit
exit
paulfdunn@penguin:~/go/src/external-sort$ docker container rm external-sort
external-sort
paulfdunn@penguin:~/go/src/external-sort$
```
The commands used:
```
docker build -t external-sort:external-sort .
docker run -it  --name external-sort external-sort:external-sort /bin/bash
./external-sort -testfileentries 10000 -thresholdsize 5000 -reset
ls -al /tmp/external-sort/chunked-sorted/
ls -al /tmp/external-sort/merged-sorted/ 
head /tmp/external-sort/merged-sorted/merged-sorted_14.txt
tail /tmp/external-sort/merged-sorted/merged-sorted_14.txt
docker container rm external-sort
```
