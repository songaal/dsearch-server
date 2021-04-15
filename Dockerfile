FROM openjdk:8

ENV TZ=Asia/Seoul

RUN apt-get update -y
RUN apt-get install rsync -y
WORKDIR /data
WORKDIR /data/indexFile

WORKDIR /app



