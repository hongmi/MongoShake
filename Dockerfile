FROM golang:alpine3.12

ENV CGO_ENABLED 0
ENV GO111MODULE on

RUN apk add --no-cache git
RUN apk add --no-cache zsh
RUN sh -c "$(wget -O- https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"

# Get Delve from a GOPATH not from a Go Modules project
WORKDIR /go/src/

RUN go get github.com/go-delve/delve/cmd/dlv@v1.4.0

COPY bin/collector.linux /go/bin/

EXPOSE 8080 30000

CMD ["/go/bin/dlv", "--listen=:30000", "--headless=true", "--api-version=2", "exec", "/go/bin/collector.linux", "--", "-conf=/go/conf/collector.conf", "--verbose"]
