# Copyright 2024 Mehmet ERGULCU
# SPDX-License-Identifier: Apache-2.0

# One recipe for both applications: docker build --build-arg MODULE=producer .
FROM eclipse-temurin:25-jdk AS build
ARG MODULE
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY contract contract
COPY producer producer
COPY consumer consumer
RUN ./mvnw -B -q -DskipTests -pl ${MODULE} -am package

FROM eclipse-temurin:25-jre
ARG MODULE
RUN useradd --system --uid 10001 app
USER app
WORKDIR /app
COPY --from=build /src/${MODULE}/target/kafka-demo-${MODULE}-*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
