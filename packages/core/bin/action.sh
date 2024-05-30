#!/usr/bin/env bash

env

SYNC_ARGS=""

if [ ! -z $INPUT_DIR ]; then
  SYNC_ARGS="$SYNC_ARGS --project-dir $GITHUB_WORKSPACE/$INPUT_DIR"
else
  SYNC_ARGS="$SYNC_ARGS --project-dir $GITHUB_WORKSPACE"
fi

if [ ! -z $INPUT_SELECT ]; then
  SYNC_ARGS="$SYNC_ARGS --select $INPUT_SELECT"
fi

if [ ! -z $INPUT_STATE ]; then
  SYNC_ARGS="$SYNC_ARGS --state $INPUT_STATE"
fi

if [ ! -z $INPUT_DEBUG ]; then
  SYNC_ARGS="$SYNC_ARGS --debug"
fi

if [ ! -z $INPUT_FULL ]; then
  SYNC_ARGS="$SYNC_ARGS --full-refresh"
fi

export RPC_PORT=8081

node /syncmaven/packages/core/dist/src/index.js sync $SYNC_ARGS