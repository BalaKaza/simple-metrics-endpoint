FROM openjdk:8

RUN apt-get update \
 && apt-get install -y git

ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/ \
 && echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa \
 && echo -e "Host github.com\n\tStrictHostKeyChecking no\n" >> /root/.ssh/config

RUN mkdir /home/workspace \
 && cd /home/workspace \
 && git clone git@github.com:gemfire/tanzu-integrations.git

WORKDIR /home/workspace/tanzu-integrations/enhanced-metrics
RUN git pull
RUN ./gradlew clean build --refresh-dependencies
RUN rm /root/.ssh/id_rsaot/.ssh/id_rsa