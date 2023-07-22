#!/bin/bash

eval $(minikube docker-env)
docker buildx build --tag localhost:5000/scaler:latest .
docker push localhost:5000/scaler