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
  pi.tc      = twitterClient(pi.auth.consumerKey, pi.auth.consumerSecret);
  var resp   = {
    data   : {},
    config : {}
  };
  var since  = 1;
  var max    = 0;
  var newest = 0;

  // if existing since, start from there
  if (pi.config && pi.config.newest) newest = pi.config.newest;
  if (pi.config && pi.config.since) since = pi.config.since;
  if (pi.config && pi.config.max) max = pi.config.max;

  var arg = {
    screen_name : pi.auth.profile.screen_name,
    since_id    : since
  };
  if (max > 0) arg.max_id = max; // we're paging down results

  tw.getMentions(pi, arg, function(err, js){
    if (err) return cb(err);
    if (!Array.isArray(js)) return cb("no array");

    // find the newest and oldest!
    js.forEach(function(item){
      // js not-really-64bit crap, L4M30
      if (item.id > newest) newest = item.id + 10;
      if (item.id < max || max === 0) max = item.id;
    });

    if (js.length <= 1 || max <= since) {
      since = newest; // hit the end, always reset since to the newest known
      max = 0; // only used when paging
    }

    var base = 'tweet:'+pi.auth.profile.id+'@twitter/mentions';
    resp.data[base] = js;

    resp.config.newest = newest;
    resp.config.since = since;
    resp.config.max = max;

    if (max > 1) resp.config.nextRun = -1; // run again if paging
    cb(err, resp);
  });
};
