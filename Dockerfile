FROM gradle:jdk15 as build

WORKDIR /src
COPY --chown=gradle:gradle . .
RUN gradle shadowJar --no-daemon

FROM openjdk:14-slim

WORKDIR /app

COPY --from=build /src/build/libs/actor-stuff-1.0-SNAPSHOT-all.jar .

ENTRYPOINT ["java", "-ea", "-jar", "/app/actor-stuff-1.0-SNAPSHOT-all.jar"]
