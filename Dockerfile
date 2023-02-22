From golang:1.18-buster as build

WORKDIR /birdwatcher
COPY . .

WORKDIR /birdwatcher
RUN apt update && apt install gcc
RUN GOPROXY="https://goproxy.cn,direct" go build -o /birdwatcher/bin/birdwatcher -tags WKAFKA main.go

FROM debian:buster 
COPY --from=build /birdwatcher/bin/birdwatcher /birdwatcher/birdwatcher

CMD ["sleep", "infinity"]
