FROM openjdk:8

ENV TZ=Asia/Seoul

RUN apt update -y
RUN apt install rsync -y
RUN mkdir /data

WORKDIR /app



