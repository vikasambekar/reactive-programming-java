# Use a JDK image for runtime
FROM eclipse-temurin:21-jdk

WORKDIR /app

# Copy the JAR (make sure you have built it locally using Maven)
COPY target/reactive-programming-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8089
LABEL authors="vikasambekar"

ENTRYPOINT ["java", "-jar", "app.jar"]