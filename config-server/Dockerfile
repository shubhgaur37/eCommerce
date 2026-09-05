# Base image provides Maven 3.9.6 and JDK 21.
FROM maven:3.9.6-eclipse-temurin-21

# Set the working directory inside the container.
WORKDIR /app

# Copy Maven Wrapper configuration.
COPY .mvn .mvn

# Copy Maven Wrapper and project configuration.
COPY mvnw pom.xml ./

# Download dependencies during image build so this layer can be cached.
RUN ./mvnw dependency:go-offline

# Copy application source code.
COPY src ./src

# Runs when the container starts.
# Spring automatically picks up SPRING_PROFILES_ACTIVE from Docker Compose.
CMD ["./mvnw", "spring-boot:run"]