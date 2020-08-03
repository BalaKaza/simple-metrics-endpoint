FROM openjdk:8

RUN apt-get update \
 && apt-get install -y git
RUN mkdir /home/workspace \
 && cd /home/workspace \
 && git clone https://github.com/BalaKaza/simple-metrics-endpoint.git

WORKDIR /home/workspace/simple-metrics-endpoint
#RUN git fetch --all && git co wip-collect-more-stats
RUN git pull && git log
RUN ./gradlew clean build --refresh-dependencies
