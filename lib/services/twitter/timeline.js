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

exports.sync = function(pi, cb) {
  pi.tc = require(path.join(__dirname, 'twitter_client.js'))(pi.auth.consumerKey, pi.auth.consumerSecret);
  var resp = {data:{}, config:{}};
  var since=1;
  var max=0;
  var newest=0;
  // if existing since, start from there
  if (pi.config && pi.config.newest) newest = pi.config.newest;
  if (pi.config && pi.config.since) since = pi.config.since;
  if (pi.config && pi.config.max) max = pi.config.max;
  var arg = {screen_name:pi.auth.profile.screen_name, since_id:since};
  if (max > 0) arg.max_id = max; // we're paging down results
  tw.getTimeline(pi, arg, function(err, js){
    if (err) return cb(err);
    if (!Array.isArray(js)) return cb("no array");
    var timeline = [];
    // find the newest and oldest, and filter
    js.forEach(function(item){
      if (item.user && item.user.id === pi.auth.profile.id) return; // skip author's own, they show up in tweets.js
      if (item.id > newest) newest = item.id + 10; // js not-really-64bit crap, L4M30
      if (item.id < max || max === 0) max = item.id;
      timeline.push(item);
    });
    if (js.length <= 1 || max <= since) {
      since = newest; // hit the end, always reset since to the newest known
      max = 0; // only used when paging
    }
    var base = 'tweet:'+pi.auth.profile.id+'@twitter/timeline';
    resp.data[base] = timeline;
    resp.config.newest = newest;
    resp.config.since = since;
    resp.config.max = max;
    if (max > 1) resp.config.nextRun = -1; // run again if paging
    cb(err, resp);
  });
};
