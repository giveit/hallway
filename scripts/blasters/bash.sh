#!/usr/bin/env bash

GRAPH_BASE='https://graph.facebook.com/'

for i in $(seq 1 $1); do
  echo $i
  for endpoint in 'me' 'me/feed' 'me/checkins' 'me/friends' 'me/home' 'me/accounts'; do
    url=$GRAPH_BASE$endpoint
    echo $url
    (curl -s $url?access_token=$FBTOKEN | wc -c) &
  done
done
