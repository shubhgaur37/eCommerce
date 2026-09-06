FROM maven:3.9.6-eclipse-temurin-21

WORKDIR /app

COPY .mvn .mvn

COPY mvnw pom.xml ./

RUN ./mvnw dependency:go-offline

COPY src ./src

CMD ["./mvnw", "spring-boot:run"]