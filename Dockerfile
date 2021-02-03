FROM golang:1.15.6 as build-env

RUN git clone https://github.com/hongmi/MongoShake.git

WORKDIR /go/MongoShake

RUN git checkout sync2es

RUN go get -u github.com/kardianos/govendor

ENV GOPATH=/go/MongoShake

WORKDIR /go/MongoShake/src/vendor/

RUN govendor sync -v

RUN go get -u golang.org/x/sync/semaphore
RUN go get -u golang.org/x/text/transform
RUN go get -u golang.org/x/text/unicode/norm
RUN go get -u github.com/cenkalti/backoff
RUN go get -u github.com/dustin/go-humanize
RUN go get -u github.com/elastic/go-elasticsearch

# change go-elasticsearch branch
WORKDIR /go/MongoShake/src/github.com/elastic/go-elasticsearch
# checkout go-elasticsearch to 7.x
RUN git checkout 7.x

RUN echo $GOPATH

WORKDIR /go/MongoShake

RUN ./build.sh

# create a clean running evironment
FROM golang:1.15.6

COPY --from=build-env /go/MongoShake/bin /go/MongoShake/bin

# this port define in conf
#EXPOSE 9100
#EXPOSE 9101
#EXPOSE 9102

CMD ["/go/MongoShake/bin/collector.linux", "-conf=/go/MongoShake/conf/collector.conf", "--verbose"]