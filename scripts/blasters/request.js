var async = require('async');
var request = require('request');
var http = require('http');
var https = require('https');

http.globalAgent.maxSockets = 1000;
https.globalAgent.maxSockets = 1000;

var GRAPH_BASE = 'https://graph.facebook.com/';

var ENDPOINTS = [
  'me',
  'me/feed',
  'me/checkins',
  'me/friends',
  'me/home',
  'me/accounts'
];

var workers = parseInt(process.argv[2], 10);
var fetches = parseInt(process.argv[3] || 100, 10);

console.log("Fetching", fetches, "times with", workers, "workers");

var totalRequested = 0;
var count = 0;
var start;

function fetch(endpoint, callback) {
  request.get(GRAPH_BASE + endpoint, {
    qs: {
      access_token: process.env.FBTOKEN
    }
  }, function (err, response, body) {
    totalRequested += body.length;
    count++;
    callback();
  });
}

var work = async.queue(fetch, workers);

function checkAllFinished() {
  if (count !== fetches) {
    return;
  }

  var diff = Date.now() - start;

  console.log(totalRequested + ' bytes in ' + diff + 'ms: ' +
    (totalRequested / (diff / 1000)) + 'b/s');

  process.exit(0);
}

start = Date.now();

for (var i = 0; i < fetches; i++) {
  work.push(ENDPOINTS[i % ENDPOINTS.length], checkAllFinished);
}
