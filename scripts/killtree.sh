#!/bin/bash

pid=$1

if [ -n "$pid" ]; then
  for cid in `pgrep -P $pid`; do
    $0 $cid
  done
  kill $pid
fi

