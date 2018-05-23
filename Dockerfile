
FROM golang:1.8-stretch

# Customize this with the commit hash or branch name you want to build
ENV COMMIT feature/issue_3558_int_cache_post_dcp

RUN apt-get update && apt-get install -y \
  git

RUN echo "source branch: $SOURCE_BRANCH"
RUN echo "DOCKER_REPO: $DOCKER_REPO"
RUN echo "DOCKER_TAG: $DOCKER_TAG"
RUN echo "SOURCE_URL: $SOURCE_URL"
RUN echo "DOCKERFILE_PATH: $DOCKERFILE_PATH"
RUN echo "PATH: $PATH"

# Without these settings, the repo tool will fail (TODO: cleaner way to do this?)
RUN git config --global user.email "you@example.com" && \
    git config --global user.name "Your Name"

# Disable the annoying "color prompt" when running repo that can make this build get stuck
RUN git config --global color.ui false 

# Download and run the bootstrap.sh script which will download and invoke the repo
# tool to grap all required repositories
RUN wget https://raw.githubusercontent.com/couchbase/sync_gateway/$COMMIT/bootstrap.sh && \
    cat bootstrap.sh && \
    chmod +x bootstrap.sh && \
    ./bootstrap.sh -c $COMMIT -p sg

# Build the Sync Gateway binary
RUN ./build.sh -v

# Deploy to a directory in the path
RUN mv godeps/bin/sync_gateway $GOPATH/bin/
