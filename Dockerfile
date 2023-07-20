# Use a multi-stage build to keep the final image size small.
# In the first stage, we'll use the official Go image to build the binary.
# FROM golang:1.17.1 as builder
FROM registry.cn-shanghai.aliyuncs.com/hai-hsin/rust:0.1.3

# Set the working directory to /build.
WORKDIR /app

# Copy the source code excluding the 'data' directory
COPY . ./
RUN rm -rf ./data
RUN cargo build --release

# Copy the binary from the builder stage.
RUN apt-get install -y curl
RUN cp ./target/release/scaler ./
RUN cargo clean
RUN chmod +x ./scaler

# Copy the startup script.
COPY run.sh run.sh
RUN chmod +x run.sh
