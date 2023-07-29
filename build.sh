#!/bin/bash

docker buildx build --no-cache --tag registry.cn-shanghai.aliyuncs.com/hai-hsin/scaler:arch .
docker push registry.cn-shanghai.aliyuncs.com/hai-hsin/scaler:arch