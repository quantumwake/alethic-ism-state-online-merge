#!/bin/bash

# Function to print usage
print_usage() {
  echo "Usage: $0 [-t tag] [-a architecture]"
  echo "  -t tag             Docker image tag"
  echo "  -p platform        Target platform architecture (default: linux/amd64)"
}

# Default values
TAG=""
ARCH="linux/amd64"


# Parse command line arguments
while getopts 't:a:' flag; do
  case "${flag}" in
    t) TAG="${OPTARG}" ;;
    a) ARCH="${OPTARG}" ;;
    *) print_usage
       exit 1 ;;
  esac
done

# Check if ARCH is set, if not default to linux/amd64
if [ -z "$ARCH" ]; then
  ARCH="linux/amd64"
  # TODO: Check operating system and set ARCH accordingly, e.g., ARCH="linux/arm64"
fi

# Check if GIT_USERNAME and GIT_TOKEN environment variables are set
if [ -z "$GIT_USERNAME" ] || [ -z "$GIT_TOKEN" ]; then
  echo "Error: GIT_USERNAME and GIT_TOKEN environment variables must be set."
  exit 1
fi

## Display arguments
echo "Platform: $ARCH"
echo "Platform Docker Image Tag: $TAG"
echo "Git Username: $GIT_USERNAME"
echo "Git Token: $GIT_TOKEN"

docker build  myapp .

# Build the Docker image which creates the package
docker build --progress=plain \
  --platform "$ARCH" -t "$TAG" \
  --build-arg GIT_USERNAME=$GIT_USERNAME \
  --build-arg GIT_TOKEN=$GIT_TOKEN \
  --no-cache .
