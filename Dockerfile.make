FROM golang:1.15.6 as build-env

RUN git clone https://github.com/hongmi/MongoShake.git

WORKDIR /go/MongoShake

RUN git checkout sync2es

RUN go get -u github.com/kardianos/govendor

ENV GOPATH=/go/MongoShake

WORKDIR /go/MongoShake/src/vendor/

RUN echo $GOPATH

RUN govendor sync -v

RUN echo govendor status | true

RUN go get -u golang.org/x/sync/semaphore

RUN go get -u golang.org/x/text/transform

RUN go get -u golang.org/x/text/unicode/norm

RUN go get -u github.com/elastic/go-elasticsearch

# change go-elasticsearch branch
WORKDIR /go/MongoShake/src/github.com/elastic/go-elasticsearch
# checkout go-elasticsearch to 7.x
RUN git checkout 7.x

RUN echo $GOPATH

WORKDIR /go/MongoShake

RUN DEBUG=1 ./build.sh

RUN ls

CMD ["/go/MongoShake/bin/collector.linux", "-conf=/go/MongoShake/conf/collector.conf", "--verbose"]