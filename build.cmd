

CALL mvnw clean

CALL mvnw package -DskipTests

CALL docker build -t dsearch-server:latest .

