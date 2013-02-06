var async = require('async');

var dMap = require('dMap');
var idr = require('idr');
var ijod = require('ijod');
var fUtils = require('friends-utilities');
var logger = require('logger').logger('friends');
var taskmanNG = require('taskman-ng');

var DEFAULT_AVATARS = [
  /images.instagram.com\/profiles\/anonymousUser.jpg/, // Instagram
  /static-ak\/rsrc.php\/v2\/yL\/r\/HsTZSDw4avx.gif/,   // FB Male
  /static-ak\/rsrc.php\/v2\/yp\/r\/yDnr5YfbJCH.gif/,   // FB Female
  /4sqi\.net\/img\/blank_(boy|girl)/,                  // Foursquare
  /foursquare\.com\/img\/blank_/,                      // Foursquare also
  /twimg.com\/sticky\/default_profile_images/          // Twitter
];

// when merging profile info, which fields win out
var BESTY_FIELDS = {
  "facebook": ["thumbnail_url", "name"],
  "twitter": ["url", "description"]
};

var CATEGORIES = { "inner": 0, "ids": 1, "outer": 2, "interests": 3 };

// parallels are groupd into categories, since they're stored 4-per-row (for
// now, bit of a hack to fit current entries data model)
exports.parCats = function () {
  return CATEGORIES;
};

// convert an id into its cat ver, just shift the last nib by the cat value
exports.parCat = function (id, cat) {
  id = id.toLowerCase();

  if (!CATEGORIES[cat]) return id;

  var x = parseInt(id.substr(-1, 1), 16) + CATEGORIES[cat];

  return id.substr(0, 31) + (x.toString(16)).substr(-1, 1);
};

// parallels are 32bit integers that align contact info - inner parallels are
// ones that dedup contacts into one, name, email, etc - outer parallels are
// ones that group contacts together, interactions, interests, relationships,
// etc

// return an array of the INNER parallels used for deduping, 4 32bit integers
// (hexified) first name, last name, email, handle || phone#
// TODO someday, if multiple emails/phones and there's room in the 4, include
// them
exports.parallels = function (entry) {
  var ret = [];
  var oe = dMap.get('oembed', entry.data, entry.idr);

  if (!oe || oe.type !== 'contact') return ret;

  // extract first/last
  if (oe.title) {
    // first byte is 3-char sort, other three bytes is full hash
    var name = exports.name(oe.title);

    ret.push(fUtils.parts2par([
      dMap.parallelType('first'),
      fUtils.str2sort(name.first),
      fUtils.str2num(name.first, 2)
    ]));

    ret.push(fUtils.parts2par([
      dMap.parallelType('last'),
      fUtils.str2sort(name.last),
      fUtils.str2num(name.last, 2)
    ]));
  }

  // any email address
  if (oe.email) {
    ret.push(fUtils.parts2par([dMap.parallelType('email'),
      fUtils.str2num(oe.email, 3)]));
  }

  // any phone#
  if (oe.phone) {
    // TODO normalize phone better!
    var phone = fUtils.phoneHome(oe.phone);
    ret.push(fUtils.parts2par([dMap.parallelType('phone'),
      fUtils.str2num(phone, 3)]));
  } else if (oe.handle) {
    // alternatively, any handle
    // TODO, maybe if no handle but there is email and the email is @gmail
    // @yahoo etc, use the username part?
    ret.push(fUtils.parts2par([dMap.parallelType('handle'),
      fUtils.str2num(oe.handle, 3)]));
  }

  return ret;
};

// simple utils
exports.name = function (name) {
  var parts = (name) ? name.toLowerCase().split(/\s+/) : [];

  return {
    first: (parts.shift() || ''),
    last: (parts.pop() || '')
  };
};

// brute force, but we need a way to force contacts to be re-indexed (skip hash
// check) when the logic is changed
exports.vpump = function (cset, auth, cbDone) {
  var ndx = {};
  var ver = fUtils.reversion(auth);

  dMap.types('contacts').forEach(function (key) {
    ndx[key] = "contact";
  });

  cset.forEach(function (entry) {
    var types = dMap.typeOf(entry.idr);

    if (types.indexOf('contacts') >= 0) entry._v = ver;
  });

  cbDone(null, cset);
};

// process them post-ijod so that only new/updated contacts are efficiently
// indexed
exports.bump = function (cset, auth, cbDone) {
  if (!auth || !auth.apps) return cbDone(null, cset); // sanity check
  var pids = {};
  var iact = { inter: {}, act: {}, photos: {}, ll: {} };
  // first just build an index by pid to work with versus flat array
  var deviceId = false;
  var self = false;

  cset.forEach(function (entry) {
    if (!entry.saved) return; // only process *new* content!

    // we process our self entry specially to index global ids across the whole
    // app
    var id = idr.parse(entry.idr);

    if (id.path === 'self') self = entry;

    // process based on the type
    var types = dMap.typeOf(entry.idr, entry.types);

    if (types.length === 0) return;

    if (types.indexOf('contacts') >= 0 || id.path === 'self') {
      // below we do a lot more work on contacts, including self
      id = idr.parse(entry.idr);

      // a device contact id is in the format devicename.accountid.appid@devices
      if (id.host === 'devices') deviceId = id.auth.split('.');

      var dest = encodeURIComponent(id.hash) + '@' + id.host;

      pids[dest] = entry;
      // TODO: for normal non-device contacts we should be looking for matching
      // phone/email's too, as they may not be friends on the same network but
      // across networks and still the same person
    }

    var participants = ijod.participants(entry);

    if (participants.length > 0) {
      var me = idr.parse(entry.idr).auth;

      // index of everyone we have interactions with
      if (participants.length > 1 && participants.indexOf(me) >= 0) {
        participants.forEach(function (p) {
          if (!iact.inter[p]) iact.inter[p] = 0;

          iact.inter[p]++;
        });
      }

      // keep track of sharing activity of others
      if (participants[0] !== me) {
        var author = participants[0];
        if (!iact.act[author]) iact.act[author] = 0;
        iact.act[author]++;
        // additionally track photos as a sub-sortable option
        if (types.indexOf('photos_feed') >= 0) {
          if (!iact.photos[author]) iact.photos[author] = 0;
          iact.photos[author]++;
        }
        // track last seen location
        var ll = dMap.get('ll', entry.data, entry.idr);
        if (ll && entry.at > (iact.ll[author] || 0)) iact.ll[author] = ll;
      }
    }
  });

  logger.debug("bump",
    Object.keys(pids).length,
    Object.keys(iact).map(function (key) {
      return {
        key: key,
        len: Object.keys(iact[key]).length
      };
    }));

  // this could be parallel, but that could be a lot of mysql too since they
  // internally parallelize
  async.series([
    function (cb) {
      fUtils.indexMe(auth, self, cb);
    },
    function (cb) {
      fUtils.friendex(auth, pids, cb);
    },
    function (cb) { // device contacts are special, per-app
      if (deviceId) fUtils.devicePeers(deviceId[1], deviceId[2], pids, cb);
      else fUtils.peerCheck(auth, pids, cb);
    },
    function (cb) {
      fUtils.interactive(auth, iact, cb);
    }
  ], function () {
    cbDone(null, cset);
  });
};

// fetch all the bases and return a merged set
exports.baseMerge = function (bases, options, callback) {
  var ndx = {};
  var ids = {};

  async.forEach(bases, function (base, cbBase) {
    // when this base is the ace (status) one, it can't freshen or get options
    // applied
    taskmanNG.fresh((options.fresh && (base !== options.ace) && base),
      function (err) {
      if (err) logger.warn("fresh error", base, err);

      ijod.getPars(base, (options.ace === base) ? { xids: true } : options,
        function (err, pars) {
        if (err) logger.warn("pars error", base, err);

        if (!pars) return cbBase();

        // loop through and build sorta an inverse index for merging checks
        Object.keys(pars).forEach(function (id) {
          if (pars[id].pars.length === 0) return; // skip non-indexed entries

          ids[id] = pars[id];
          ids[id].id = id.toLowerCase() + '_' + idr.partition(base);
          ids[id].mergies = [];
          ids[id].base = base;

          ids[id].pars.forEach(function (par) {
            // stash the data a few ways using the name of the type for sanity's
            // sake
            var type = fUtils.ptype(par);

            ids[id][type] = par;

            if (type === "email" ||
              type === "phone" ||
              type === "handle" ||
              type === "url") {
              ids[id].mergies.push(par); // all direct mergable fields
            }

            // all service ids
            if (parseInt(par.substr(0, 2), 16) >= 100) {
              ids[id].mergies.push(par);
            }

            if (!ndx[type]) ndx[type] = {};
            if (!ndx[type][par]) ndx[type][par] = [];

            ndx[type][par].push(id);
          });
        });

        cbBase();
      });
    });
  }, function () {
    // util to increment during merge
    function inc(friend, id, field) {
      if (!ids[id][field]) return;
      if (!friend[field]) friend[field] = 0;

      friend[field] += parseInt(ids[id][field].substr(2), 16);
    }

    // util to merge
    function merge(friend, id) {
      if (ids[id].merged) return; // already merged
      if (friend.ids[id]) return; // edge case catch, since we recurse!

      friend.ids[id] = true;
      friend.connected++;
      friend.profiles.push(ids[id]);

      if (ids[id].q) friend.matched = true;
      if (ids[id].bio) friend.bio = true;

      // for sorting
      if (!friend.first && ids[id].first) friend.first = ids[id].first;
      if (!friend.last && ids[id].last) friend.last = ids[id].last;
      if (ids[id].xid) friend.peer = ids[id].xid; // cross-ref id

      inc(friend, id, "interactions");
      inc(friend, id, "activity");
      inc(friend, id, "photos");

      // now also recurse in and see if the merged id had other matchable bits
      seek(id, friend);

      ids[id].merged = friend;
    }

    var friends = [];
    // check if this id is mergeable w/ any others, add to friend
    function seek(id, friend) {
      if (ids[id].merged) return; // id already merged

      // merge the mergies! (exact id matches)
      ids[id].mergies.forEach(function (par) {
        ndx[fUtils.ptype(par)][par].forEach(function (dup) {
          merge(friend, dup);
        });
      });

      // only merge when first and last match exactly
      if (ids[id].first) {
        ndx.first[ids[id].first].forEach(function (dup) {
          if (ids[id].last === ids[dup].last) merge(friend, dup);
        });
      }
    }

    // do the merging
    Object.keys(ids).forEach(function (id) {
      var friend = { profiles: [], ids: {}, connected: 0 };
      seek(id, friend); // look for duplicates
      merge(friend, id); // always add self
      friends.push(friend);
    });

    if (options.q) {
      friends = friends.filter(function (friend) {
        return friend.matched;
      });
    }

    if (options.bio) {
      friends = friends.filter(function (friend) {
        return friend.bio;
      });
    }

    callback(null, friends);
  });
};

// make sure this friend matches the query
exports.validate = function (friend, options) {
  if (options.q && !fUtils.checkPlease(options.q, ijod.qtext(friend), true)) {
    return false;
  }

  if (options.bio &&
    !fUtils.checkPlease(options.bio, fUtils.bioget(friend), false)) {
    return false;
  }

  return true;
};

// utility to map all sorting options to actionables
exports.sorts = function (sort, a, b) {
  if (!a || a === '') a = undefined;
  if (!b || b === '') b = undefined;

  if (a === undefined && b === undefined) return 0;
  if (a === undefined) return 1;
  if (b === undefined) return -1;

  if (sort === 'first' || sort === 'last') {
    return (a < b) ? -1 : ((a > b) ? 1 : 0);
  }

  if (['connected', 'interactions', 'activity', 'photos'].indexOf(sort) >= 0) {
    return b - a; // descending
  }

  return a - b;
};

// gen a toc for the list, sort = first||last||connected
exports.ginTonic = function (list, sort) {
  var toc = {
    "meta": {
      "length": list.length,
      "sort": sort
    }
  };

  if (sort === 'connected') { // totally different style
    var current = list[0].connected;
    var start = 0;

    for (var i = 0; i < list.length; i++) {
      if (list[i].connected === current) continue;
      toc[current.toString()] = { "offset": start, "length": (i - start) };
      current = list[i].connected;
      start = i;
    }

    toc[current.toString()] = {
      "offset": start,
      "length": (list.length - 1) - start
    };

    return toc;
  }

  // first || last
  var map = fUtils.tocmap(sort);
  var on = map.shift();
  on.start = 0;

  function check(offset) {
    if (!on.c || (map[0] && parseInt(list[offset][sort], 16) < map[0].v)) {
      return;
    }

    toc[on.c] = { "offset": on.start, "length": offset - on.start };

    on = map.shift() || {};
    on.start = offset;

    return check(offset);
  }

  for (var j = 0; j < list.length; j++) {
    check(j);
  }

  toc["*"] = { "offset": on.start, "length": (list.length - 1) - on.start };

  return toc;
};

// combine multiple oembeds into one
exports.contactMerge = function (profile, entry, options) {
  options = options || {};

  if (!profile) profile = { services: {} };
  if (!entry) return profile;

  // TODO remove once all email's are map'd into oembed.email
  if (entry.data && entry.data.email) profile.email = entry.data.email;

  var oembed = dMap.get('oembed', entry.data, entry.idr);
  if (!oembed) return profile;
  // convenient to have and keep consistent
  if (!oembed.id) oembed.id = idr.parse(entry.idr).hash;
  oembed.entry = entry.id;

  var service = oembed.provider_name;
  profile.services[service] = oembed;

  // unoembedize
  oembed.name = oembed.title;

  delete oembed.type;
  delete oembed.provider_name;
  delete oembed.title;

  // remove any default thumbnails
  if (oembed.thumbnail_url) {
    DEFAULT_AVATARS.forEach(function (avatar) {
      if (oembed.thumbnail_url && oembed.thumbnail_url.match(avatar)) {
        delete oembed.thumbnail_url;
      }
    });
  }

  Object.keys(oembed).forEach(function (key) {
    // don't copy up some service-specific fields
    if (key === 'id' || key === 'entry') return;

    if (!profile[key] ||
      (BESTY_FIELDS[service] && BESTY_FIELDS[service].indexOf(key) !== -1)) {
      profile[key] = oembed[key]; // copy up unique values
    }

    // don't keep dups around
    if (options.light && profile[key] === oembed[key]) delete oembed[key];
  });

  if (options.full) {
    if (!profile.full) profile.full = {};

    profile.full[service] = entry;
  }

  return profile;
};
