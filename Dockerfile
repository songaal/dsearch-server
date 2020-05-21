FROM openjdk:8

WORKDIR /app

COPY target/fastcatx-server.jar .

CMD ["java", "-jar", "fastcatx-server.jar"]