#!/bin/bash

#Builds a canary release of the syncmaven/syncmaven image
#Shouldn't be used directly, GitHub Actions executes the same thing
#It's here mainly for reference / debugging purposes

function build() {
  local platform=$1
  local tag=$2
  local version=$3
  docker buildx build --platform linux/$platform -t syncmaven/syncmaven:$tag-$platform -t syncmaven/syncmaven:$version-$platform --push .
}



function main() {
  local version="$1"
  local rev="$(git rev-list --count HEAD).$(git rev-parse --short HEAD)"
  local script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
  local tags
  if [ -z "$version" ]; then
    build arm64 canary canary-v$rev
    build amd64 canary canary-v$rev
    "$script_dir"/docker-publish-tags.sh canary-$rev canary
  else
    build arm64 latest v$version
    build amd64 latest v$version
    "$script_dir"/docker-publish-tags.sh latest v$version
  fi
  if [ -z "$DOCKERHUB_USERNAME" ] || [ -z "$DOCKERHUB_TOKEN" ]; then
    echo "Skipping tags cleanup, DOCKERHUB_USERNAME and DOCKERHUB_TOKEN must be set for that"
  else
    "$script_dir"/docker-clean-tags.sh syncmaven/syncmaven
  fi
}

main "$@"

