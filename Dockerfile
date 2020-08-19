FROM openjdk:8

WORKDIR /app

COPY target/dsearch-server-1.0.0.jar .
CMD ["java", "-jar", "dsearch-server-1.0.0.jar"]