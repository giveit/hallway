var async = require('async');
var http = require('http');
var https = require('https');

http.globalAgent.maxSockets = 1000;
https.globalAgent.maxSockets = 1000;

var ENDPOINTS = [
  '/me',
  '/me/feed',
  '/me/checkins',
  '/me/friends',
  '/me/home',
  '/me/accounts'
];

var workers = parseInt(process.argv[2], 10);
var fetches = parseInt(process.argv[3] || 100, 10);

console.log("Fetching", fetches, "times with", workers, "workers");

var totalRequested = 0;
var count = 0;
var start;

function fetch(endpoint, callback) {
  var options = {
    hostname: 'graph.facebook.com',
    port: 443,
    path: endpoint + '?access_token=' + process.env.FBTOKEN
  };

  https.get(options, function (res) {
    res.setEncoding('utf8');

    var body = '';

    res.on('data', function (chunk) {
      body += chunk;
    });

    res.on('end', function () {
      totalRequested += body.length;
      count++;
      callback();
    });
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

// Precompute the array so the first call to drain doesn't end the script
for (var i = 0; i < fetches; i++) {
  work.push(ENDPOINTS[i % ENDPOINTS.length], checkAllFinished);
}
