# docker build -t external-sort:external-sort .
# docker run -it  --name external-sort external-sort:external-sort /bin/bash
# docker container rm external-sort
FROM golang:1.14.14-buster
COPY ./ /go/src/external-sort/
WORKDIR /go/src/external-sort/
RUN go test -v ./... >test.log 2>&1 
RUN CGO_ENABLED=0 GOOS=linux go build
RUN apt-get update -y
RUN apt-get install vim -y