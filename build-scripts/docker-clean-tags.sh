#!/bin/bash

# Removes all tags that ends with arm64 or amd64. Those tags
# are used to build the manifest, but we don't want them to be permanent

# env vars DOCKERHUB_USERNAME and DOCKERHUB_TOKEN must be present to authenticate


# Check if repository parameter is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <repository>"
  exit 1
fi

REPOSITORY=$1

if [ -z "$DOCKERHUB_USERNAME" ] || [ -z "$DOCKERHUB_TOKEN" ]; then
  echo "DOCKERHUB_USERNAME and DOCKERHUB_TOKEN must be set"
  exit 1
fi

JWT_TOKEN=$(curl -s -H "Content-Type: application/json" -X POST -d '{"username": "'$DOCKERHUB_USERNAME'", "password": "'$DOCKERHUB_TOKEN'"}' https://hub.docker.com/v2/users/login/ | jq -r .token)

# Check if JWT token was obtained
if [ -z "$JWT_TOKEN" ]; then
  echo "Failed to obtain JWT token. Please check your credentials from ."
  exit 1
fi

# Get list of tags for the repository
TAGS=$(curl -s -H "Authorization: JWT $JWT_TOKEN" "https://hub.docker.com/v2/repositories/$REPOSITORY/tags/?page_size=100" | jq -r '.results[].name')

# Loop through the tags and delete those ending in -arm64 or -amd64
for TAG in $TAGS; do
  if [[ $TAG == *-arm64 ]] || [[ $TAG == *-amd64 ]]; then
    echo "Deleting tag: $TAG"
    curl -s -X DELETE -H "Authorization: JWT $JWT_TOKEN" "https://hub.docker.com/v2/repositories/$REPOSITORY/tags/$TAG/"
  fi
done

echo "Completed tag cleanup for repository: $REPOSITORY"