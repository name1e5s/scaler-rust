# Use a multi-stage build to keep the final image size small.
# In the first stage, we'll use the official Go image to build the binary.
# FROM golang:1.17.1 as builder
FROM registry.cn-shanghai.aliyuncs.com/hai-hsin/rust:0.1.4 as builder

# Set the working directory to /build.
WORKDIR /build

# Copy the source code excluding the 'data' directory
COPY . ./
RUN rm -rf ./data
RUN cargo build --release

FROM ubuntu:latest

# Update the package list and install required packages.
RUN apt-get update && apt-get install -y netcat curl

# Set the working directory to /app.
WORKDIR /app

# Copy the binary from the builder stage.
COPY --from=builder /build/target/release/scaler /app/scaler
RUN chmod +x /app/scaler

# Copy the source code into the container, excluding the 'data' directory
COPY --from=builder /build /app/source

# Copy the startup script.
COPY run.sh run.sh
RUN chmod +x run.sh
