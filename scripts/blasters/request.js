var async = require('async');
var request = require('request');

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
var start = Date.now();

function fetch(endpoint, callback) {
  request.get(GRAPH_BASE + endpoint, {
    qs: {
      access_token: process.env.FBTOKEN
    }
  }, function(err, response, body) {
    totalRequested += body.length;
    callback();
  });
}

var work = async.queue(fetch, workers);

for (var i = 0; i < fetches; i++) {
  work.push(ENDPOINTS[i % ENDPOINTS.length]);
}

work.drain = function() {
  var diff = Date.now() - start;
  console.log(totalRequested, "bytes in", diff, "ms: ",
      totalRequested / (diff / 1000), "b/s");
};

