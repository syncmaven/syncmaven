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
  local rev="$(git rev-list --count HEAD).$(git rev-parse --short HEAD)"
  local script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

  build arm64 canary canary-v$rev
  build amd64 canary canary-v$rev

  "$script_dir"/docker-publish-tags.sh canary-$rev canary
}

main

