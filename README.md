## 소개
elasticsearch기반 관리도구입니다. 


## jar 실행방법

1 소스를 다운로드 받습니다.
```
$ git clone https://github.com/danawalab/dsearch.git
```

2 메이븐 빌드를 진행합니다.
```
$ mvn clean package
```

3 dsearch 실행합니다.

```
$ cd target/bin
$ sh dsearch start
```

제공하는 명령어
- sh dsearch start: dsearch 프로세스를 시작합니다.
- sh dsearch stop: dsearch 프로세스를 종료합니다.
- sh dsearch restart:  dsearch 프로세스를 재시작합니다.

## docker 실행방법

### docker run 방식

```
$ docker run -p 8080:8080 dnwlab/dsearch-server:1.0.0
```

### docker-compose 방식

1 폴더를 생성 후 docker-compose 를 다운받습니다.
```
$ mkdir dsearch
$ cd dsearch
$ wget https://raw.githubusercontent.com/danawalab/dsearch/master/docker-compose.yml
```

2 docker-compose를 실행합니다.
```
$ docker-compose up -d
```
