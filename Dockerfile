
# Stage to build Sync Gateway binary
FROM golang:1.11.5-stretch as builder

# Customize this with the commit hash or branch name you want to build
ARG COMMIT=master

# Refresh apt repository, install git
RUN apt-get update && apt-get install -y \
  git

# Without these settings, the repo tool will fail (TODO: cleaner way to do this?)
RUN git config --global user.email "you@example.com" && \
    git config --global user.name "Your Name"

# Disable the annoying "color prompt" when running repo that can make this build get stuck
RUN git config --global color.ui false

# Download and run the bootstrap.sh script which will download and invoke the repo
# tool to grap all required repositories
RUN wget https://raw.githubusercontent.com/couchbase/sync_gateway/master/bootstrap.sh && \
    cat bootstrap.sh && \
    chmod +x bootstrap.sh && \
    ./bootstrap.sh -c $COMMIT -p sg

ARG SG_EDITION=CE

# Build the Sync Gateway binary
RUN ./build.sh -v


# Stage to run the SG binary from the previous stage
FROM ubuntu:latest as runner

ARG SG_FILENAME=sync_gateway_ce

COPY --from=builder /go/godeps/bin/$SG_FILENAME /sync_gateway

ENTRYPOINT ["/sync_gateway"]
