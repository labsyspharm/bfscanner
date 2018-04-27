# Build Environment
FROM maven:3.5.3-jdk-10
COPY src /tmp/src/
COPY pom.xml /tmp/
WORKDIR /tmp
RUN mvn package

# Released Image
FROM alpine:3.5
RUN apk -Uuv add openjdk8-jre \
	&& mkdir /opt \
	&& rm /var/cache/apk/*
COPY --from=0 /tmp/target/bfscanner*.jar /opt/
WORKDIR /tmp/aws
ENTRYPOINT ["sh", "-c", "java -jar /opt/bfscanner*.jar"]
