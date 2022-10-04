FROM openjdk:8

ARG VERSION

ENV TZ=Asia/Seoul
ENV LOG4J_FORMAT_MSG_NO_LOOKUPS=true

# 필수 패키지 설치
RUN apt update -y
RUN apt install sudo vim curl net-tools rsync -y

# 유저 추가 후 변경 (root 권한 포함)
RUN useradd -r -u 1000 -g users danawa
RUN echo 'danawa ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

WORKDIR /data
WORKDIR /data/indexFile
WORKDIR /home/danawa
WORKDIR /root

WORKDIR /app

COPY branch-desc.txt/ .

COPY target/dsearch-server-${VERSION}.jar dsearch-server.jar

USER danawa

RUN sudo chown -R danawa:users /app
RUN sudo chown -R danawa:users /root
RUN sudo chown -R danawa:users /data
RUN sudo chown -R danawa:users /home/danawa

CMD ["java", "-jar", "dsearch-server.jar"]

# 도커 실행 커맨드
# docker build -t server .
# docker run -d -p 8080:8080 --name server server
# docker stop server ; docker rm server
# docker logs -f server

