#!/bin/bash

# Function to parse command line arguments and return a map
parse_args() {
  declare -A params
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      --*)
        key="${1:2}"
        shift
        if [[ "$1" =~ ^-- ]]; then
          params[$key]=""
        else
          params[$key]="$1"
          shift
        fi
        ;;
      *)
        shift
        ;;
    esac
  done

  # Return the associative array
  echo "$(declare -p params)"
}