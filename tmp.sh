
TOKEN=$(curl -s -H "Content-Type: application/json" -X POST -d '{"username": "'$DOCKER_USERNAME'", "password": "'$DOCKER_PASSWORD'"}' https://hub.docker.com/v2/users/login/ | jq -r .token)
curl -i -X DELETE -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" "https://hub.docker.com/v2/repositories/syncmaven/tmp-base-image-for-node-connectors/tags/9321153497/"