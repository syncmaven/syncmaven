function publish_manifest() {
  local tag=$1
  docker manifest create syncmaven/syncmaven:$tag syncmaven/syncmaven:$tag-arm64 syncmaven/syncmaven:$tag-amd64 --amend
  docker manifest annotate syncmaven/syncmaven:$tag syncmaven/syncmaven:$tag-arm64 --os linux --arch arm64
  docker manifest annotate syncmaven/syncmaven:$tag syncmaven/syncmaven:$tag-amd64 --os linux --arch amd64
  docker manifest push syncmaven/syncmaven:canary
}

function remove_docker_tag() {
  local tag=$1
  #docker rmi syncmaven/syncmaven:$tag
  if [ "$DOCKERHUB_USERNAME" != "" ] && [ "$DOCKERHUB_TOKEN" != "" ]; then
    local token=$(curl -s -H "Content-Type: application/json" -X POST -d "{\"username\": \"$DOCKERHUB_USERNAME\", \"password\": \"$DOCKERHUB_TOKEN\"}" https://hub.docker.com/v2/users/login/ | jq -r .token)
    if [ "$token" = "" ]; then
      echo "Failed to authenticate with Docker Hub"
    else
      echo "Removing $tag from Docker Hub"
      curl -s -H "Authorization: JWT ${token}" -X DELETE "https://hub.docker.com/v2/repositories/syncmaven/syncmaven/tags/${tag}"
    fi

  else
    echo "DOCKERHUB_USERNAME and DOCKER_TOKEN not set"
  fi

}

function build() {
  local platform=$1
  local tag=$2
  local version=$3
  docker buildx build --platform linux/$platform -t syncmaven/syncmaven:$tag-$platform -t syncmaven/syncmaven:$version-$platform --push .
}



function main() {
  local rev="$(git rev-list --count HEAD).$(git rev-parse --short HEAD)"

  #build arm64 canary canary-v$rev
  #build amd64 canary canary-v$rev

  #publish_manifest canary-$rev
  #publish_manifest canary

  remove_docker_tag canary-$rev-amd64
  remove_docker_tag canary-$rev-arm64
  remove_docker_tag canary-amd64
  remove_docker_tag canary-arm64
}

main

