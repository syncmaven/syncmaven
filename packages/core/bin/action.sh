#!/usr/bin/env bash

env

SYNC_ARGS=""

if [ ! -z $INPUT_DIR ];
then SYNC_ARGS="$SYNC_ARGS --project-dir $GITHUB_WORKSPACE/$INPUT_DIR"
else
then SYNC_ARGS="$SYNC_ARGS --project-dir $GITHUB_WORKSPACE"
fi

if [ ! -z $INPUT_SELECT ];
then SYNC_ARGS="$SYNC_ARGS --select $INPUT_SELECT"
fi


node main.js sync