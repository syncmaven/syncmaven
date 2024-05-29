#!/bin/bash

# This scripts accept array of tags prefixs, and assumes that every $tag has
# $tag-amd64 and $tag-arm64 images. It will create a manifest for each $tag and
# push it to Docker Hub.
# It's used in github actions

function publish() {
  local tag=$1
  docker manifest create syncmaven/syncmaven:$tag syncmaven/syncmaven:$tag-arm64 syncmaven/syncmaven:$tag-amd64 --amend
  docker manifest annotate syncmaven/syncmaven:$tag syncmaven/syncmaven:$tag-arm64 --os linux --arch arm64
  docker manifest annotate syncmaven/syncmaven:$tag syncmaven/syncmaven:$tag-amd64 --os linux --arch amd64
  docker manifest push syncmaven/syncmaven:$tag
}

for tag in "$@"; do
  echo "Publishing $tag..."
  publish $tag
done