#!/usr/bin/env bash

results_dir=results

usage() { echo "Usage: $0 [-q <query>] [-f] [-r]"; exit 1; }

while getopts ":q:fr" o; do
  case "$o" in
    q)
      query=${OPTARG}
      ;;
    f)
      filter=true
      ;;
    r)
      remove=true
      ;;
    *)
      usage
      ;;
  esac
done

for config_dir in $results_dir/*; do
  if [[ -d $config_dir ]]; then
    config=$(cat $config_dir/configuration.json)
    query_result=$(echo $config | jq -r "$query")

    if [[ "$filter" == "true" ]]; then
      if [[ "$query_result" == "true" ]]; then
        echo $config_dir
      fi
    elif [[ "$remove" == "true" ]]; then
      if [[ "$query_result" == "true" ]]; then
        echo "Removing $config_dir"
        rm -rf $config_dir
      fi
    else
      echo $config_dir
      echo $query_result
    fi
  fi
done
