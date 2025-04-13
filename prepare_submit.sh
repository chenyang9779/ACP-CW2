#!/bin/bash

#docker build -t acp_cw1 .

docker buildx build --platform=linux/amd64,linux/arm64 --load -t acp_cw2 .

docker image save acp_cw2 -o acp_submission_image.tar

rm -rf submit/*
rm /home/chenyang/Desktop/s2693586.zip
#
cp -r src/ acp_submission_image.tar Dockerfile mvnw mvnw.cmd pom.xml acp_cw2.http compose.yml AcpCw2Template.iml submit/
#
zip -r /home/chenyang/Desktop/s2693586.zip submit/*

#docker run -e REDIS_HOST=host.docker.internal -e REDIS_PORT=6379 -e RABBITMQ_HOST=host.docker.internal -e RABBITMQ_PORT=5672 -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 -p 8080:8080 --network acp_network acp_cw2