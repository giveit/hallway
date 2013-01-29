/*
*
* Copyright (C) 2011, The Locker Project
* All rights reserved.
*
* Please see the LICENSE file for more information.
*
*/

var path = require('path');
var tw = require(path.join(__dirname, 'lib.js'));
var twitterClient = require(path.join(__dirname, 'twitter_client.js'));

exports.sync = function(pi, cb) {
  pi.tc    = twitterClient(pi.auth.consumerKey, pi.auth.consumerSecret);
  var resp = {data: {}};

  var arg = {
    path   : '/friends/ids.json',
    cursor : pi.config.cursor,
    slice  : pi.config.slice
  };

  tw.getFFchunk(pi, arg, function(err, contacts) {
    if (contacts) {
      var base = 'contact:' + pi.auth.profile.id + '@twitter/friends';
      resp.data[base] = contacts;
    }
    resp.config = {
      cursor : arg.cursor,
      slice  : arg.slice
    };
    cb(err, resp);
  });
};
