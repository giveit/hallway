var async = require('async');
var crypto = require('crypto');
var mmh = require('murmurhash3');

var dal = require('dal');
var dMap = require('dMap');
var idr = require('idr');
var ijod = require('ijod');
var logger = require('logger').logger('friends');
var qix = require('qix');

// change this to force re-indexing of any contact info on next run, whenever
// the indexing logic here changes
var VERSION = 8;

var STATUSES = { "peers": 0, "invited": 1, "requested": 2, "blocked": 3 };
var CATEGORIES = { "inner": 0, "ids": 1, "outer": 2, "interests": 3 };

// convenience, string par to string type
var ptype = exports.ptype = function (par) {
  return dMap.parallelType(parseInt(par.substr(0, 2), 16));
};

// parallels are grouped into categories, since they're stored 4-per-row (for
// now, bit of a hack to fit current entries data model)
exports.parallelCategories = function () {
  return CATEGORIES;
};

// convert an id into its cat ver, just shift the last nib by the cat value
exports.parallelCategory = function (id, cat) {
  id = id.toLowerCase();

  if (!CATEGORIES[cat]) return id;

  var x = parseInt(id.substr(-1, 1), 16) + CATEGORIES[cat];

  return id.substr(0, 31) + (x.toString(16)).substr(-1, 1);
};

// return an object of the statuses set in this list of pars
exports.status = function (pars) {
  var ret = {};

  pars.forEach(function (par) {
    if (ptype(par) !== 'status') return;

    var bits = parseInt(par, 16).toString(2).split('');

    Object.keys(STATUSES).forEach(function (status) {
      if (bits[8 + STATUSES[status]] === '1') ret[status] = true;
    });
  });

  return ret;
};

// zero-pad hex number conversion
var zeroPadHex = exports.zeroPadHex = function (num, len) {
  var base = '00000000';
  base += num.toString(16);
  return base.substr(-len, len);
};

// combine bytes into, either [type, 24bit int] or [type, 8bit int, 16bit in]
var parts2par = exports.parts2par = function (parts) {
  if (typeof parts[0] === 'string') parts[0] = dMap.parallelType(parts[0]);

  var ret = zeroPadHex(parts.shift(), 2);

  if (parts.length === 1) return ret + zeroPadHex(parts.shift(), 6);

  ret += zeroPadHex(parts.shift(), 2);

  if (parts.length === 1) return ret + zeroPadHex(parts.shift(), 4);

  return ret + zeroPadHex(parts.shift(), 2) + zeroPadHex(parts.shift(), 2);
};

// just get one par or return blank
var parSelect = exports.parSelect = function (pars, type) {
  var ret = parts2par([type, 0]); // default blank

  if (!pars) pars = [];

  pars.forEach(function (par) {
    if (ptype(par) === 'status') ret = par;
  });

  return ret;
};

// increment a par
var parInc = exports.parInc = function (par, val) {
  if (!val) return par;

  var cur = parseInt(par.substr(2), 16);

  cur += val;

  return par.substr(0, 2) + zeroPadHex(cur, 6);
};

// just a simple hash into a number
var str2num = exports.str2num = function (str, bytes) {
  bytes = bytes || 4;

  return (parseInt(mmh.murmur32HexSync(str.toLowerCase()), 16) %
    Math.pow(256, bytes));
};

// convert any character to it's 0-26 alpha only range
var str26 = exports.str26 = function (str) {
  var code = str.charCodeAt(0);

  // alpha preserved only, else below z
  return ((code && code > 96 && code < 123) ? code - 97 : 26).toString(27);
};

// convert string into an alphanumeric sortable byte, max 3 chars
var str2sort = exports.str2sort = function (str) {
  // max first three, all required
  str = (str.toLowerCase() + '...').substr(0, 3);

  // the magic number is just short of base 27^3
  return Math.floor((parseInt(str.split('').map(str26).join(''),
    27) / 19682) * 255);
};

// update any status parallel to include the new one
var parUpdateStatus = exports.parUpdateStatus = function (pars, status, value) {
  var spar = parts2par(['status', 0]); // default blank
  var ret = [];

  if (!pars) pars = [];

  pars.forEach(function (par) {
    // extract any existing one
    if (ptype(par) === 'status') spar = par;
    else ret.push(par);
  });

  // binary flip the bit
  var bits = parseInt(spar, 16).toString(2).split('');
  bits[8 + STATUSES[status]] = (value) ? "1" : "0";
  spar = zeroPadHex(parseInt(bits.join(''), 2), 8);
  ret.unshift(spar);
  return ret;
};

// just check current status
var parStatus = exports.parStatus = function (pars, status) {
  var ret = false;
  if (!pars) return false;

  pars.forEach(function (par) {
    if (ptype(par) !== 'status') return;
    var bits = parseInt(par, 16).toString(2).split('');
    if (bits[8 + STATUSES[status]] === '1') ret = true;
  });

  return false;
};

// ugly, two dynamic lists
var genPairs = exports.genPairs = function (accounts, rows) {
  logger.debug("find pairs between", accounts, rows);

  var pairs = {};

  rows.forEach(function (row) {
    accounts.forEach(function (account) {
      // there could super edge case be multiple ways they're pair'd, this
      // forces just one for sanity
      pairs[[account, row.account].join('\t')] = row.profile;
    });
  });

  var ret = [];

  Object.keys(pairs).forEach(function (key) {
    var parts = key.split('\t');
    if (parts[0] === parts[1]) return; // skip self duh
    ret.push({ src: parts[0], dest: parts[1], pid: pairs[key] });
  });

  return ret;
};

var phoneHome = exports.phoneHome = function (phone) {
  phone = phone.replace(/[^0-9]+/g, '');
  if (phone.length === 10) phone = "1" + phone;
  return phone;
};

// generate a fingerprint to tell if this entry should be re-indexed
exports.reversion = function (auth) {
  var accts = [];

  if (auth.apps) {
    Object.keys(auth.apps).forEach(function (app) {
      if (auth.apps[app].accounts) {
        Object.keys(auth.apps[app].accounts).forEach(function (account) {
          accts.push(account);
        });
      }
    });
  }

  accts.sort();

  return crypto.createHash('md5').update(VERSION  + " " + accts.join(" "))
    .digest('hex');
};

// all ids extracted from this device contact
var allIds = exports.allIds = function (data) {
  var ids = {};

  if (data.phone) ids[phoneHome(data.phone)] = true;
  if (data.email) ids[data.email.toLowerCase()] = true;

  if (Array.isArray(data.phones)) {
    data.phones.forEach(function (phone) {
      ids[phoneHome(phone)] = true;
    });
  }

  if (Array.isArray(data.emails)) {
    data.emails.forEach(function (email) {
      ids[email.toLowerCase()] = true;
    });
  }

  return ids;
};

// id@service to it's par representation
var pid2par = exports.pid2par = function (pid) {
  var parts = pid.split('@');

  return parts2par([parts[1], str2num(parts[0], 3)]);
};

// convenient since it has to be done twice, update status to a peer
var friendPeer = exports.friendPeer = function (app, src, dest, pid, cbDone) {
  // for every found pairing, get any already indexed id parallels and add this
  // to the set
  // construct the per-app-account idr where the statuses are saved
  var id = 'friend:' + src + '@' + app + '/friends#' + dest;

  ijod.getOnePars(id, "ids", function (err, one) {
    var pars = one && one.pars;

    if (parStatus(pars, "peers")) return cbDone(false);

    // new peering!
    logger.debug("new peering found ", app, src, dest, pid);

    pars = parUpdateStatus(pars, "peers", true);

    var par = pid2par(pid);

    // also be sure to index the pid for it to match
    if (pars.indexOf(par) === -1) pars.unshift(par);

    ijod.setOneCat(id, "ids", { pars: pars }, function () {
      cbDone(true);
    });
  });
};

// get bio text
var bioget = exports.bioget = function (friend, oe) {
  if (!oe) oe = dMap.get('oembed', friend.data, friend.idr) || {};
  var ret = [oe.description];
  var entities = dMap.get('entities', friend.data, friend.idr) || [];
  entities.forEach(function (entity) { ret.push(entity.name); });
  return ret.join(' ');
};

// this could be static, just makes an array mapping char to the hex part for
// sorting
exports.tocmap = function (sort) {
  return 'abcdefghijklmnopqrstuvwxyz'.split('').map(function (c) {
    return {
      c: c,
      v: parseInt(parts2par([sort, str2sort(c + 'aa'), 0]), 16)
    };
  });
};

exports.checkPlease = function (search, body, sensitive) {
  var b = qix.chunk(body, sensitive);
  var s = qix.chunk(search, sensitive);

  var matches = 0;

  b.forEach(function (bpart) {
    s.forEach(function (spart) {
      if (bpart.indexOf(spart) >= 0) matches++;
    });
  });

  if (matches < s.length) {
    logger.warn("couldn't find match ", s.join(','), "in", b.join(','));

    return false;
  }

  return true;
};

// update pars for individuals who're doing stuff
exports.interactive = function (auth, iact, cbDone) {
  // to be efficient we need to get all actual friends, so build that list
  var bases = dMap.types('contacts', [auth.pid]);
  var options = {};

  // fetch all friends for this base, that are also in this list
  async.forEach(bases, function (base, cbBase) {
    var all = {};
    function build(obj) {
      Object.keys(obj).forEach(function (uid) {
        var id = base + '#' + encodeURIComponent(uid);
        all[idr.hash(id).toUpperCase()] = idr.parse(id);
      });
    }

    Object.keys(iact).forEach(function (key) {
      build(iact[key]);
    });

    options['in'] = Object.keys(all);

    if (options['in'].length === 0) {
      return process.nextTick(cbBase);
    }

    ijod.getPars(base, options, function (err, pars) {
      if (err) logger.warn("pars error", base, err);
      if (!pars) return cbBase();

      async.forEachLimit(Object.keys(pars), 10, function (idh, cbPars) {
        if (pars[idh].pars.length === 0) {
          return process.nextTick(cbPars); // skip non-indexed entries
        }

        if (!all[idh]) {
          logger.warn("mysterious interactive things, no match:", idh, base,
            pars[idh]);

          return process.nextTick(cbPars);
        }

        // get any existing values to increment
        var id = all[idh];
        var options = { pars: [] };

        options.pars.push(parInc(parSelect(pars[idh].pars,
          'interactions'), iact.inter[id.hash]));

        options.pars.push(parInc(parSelect(pars[idh].pars,
          'activity'), iact.act[id.hash]));

        options.pars.push(parInc(parSelect(pars[idh].pars,
          'photos'), iact.photos[id.hash]));

        // TODO geo latlng
        logger.debug("updating interactives for", id.hash, options);

        ijod.setOneCat(idr.toString(id), "outer", options, function () {
          cbPars();
        });
      }, function () {
        cbBase();
      });
    });
  }, cbDone);
};

// now, see if there is a peering relationship
exports.peerCheck = function (auth, pids, cbDone) {
  if (!auth.apps || Object.keys(pids).length === 0) return cbDone();
  // this has to be done app by app
  async.forEach(Object.keys(auth.apps), function (app, cbApp) {
    // dumb safety check
    if (!auth.apps[app].accounts) {
      return process.nextTick(cbApp);
    }
    var ids = Object.keys(pids).map(function (id) {
      return "'" + id + "'";
    }).join(",");

    var sql = "SELECT account, profile from Accounts where app = ? and profile in (" + ids + ")";

    // bulk query efficiently
    logger.debug("bulk querying", sql);

    dal.query(sql, [app], function (err, rows) {
      if (!rows || rows.length === 0) return cbApp();

      var pairs = genPairs(Object.keys(auth.apps[app].accounts), rows);

      logger.debug("found pairs", auth.pid, app, pairs);

      async.forEachLimit(pairs, 10, function (pair, cbPair) {
        // set up peering from this account to the other
        friendPeer(app, pair.src, pair.dest, pair.pid, function (isNew) {
          // importantly, make sure reverse is set too!
          friendPeer(app, pair.dest, pair.src, auth.pid, function () {
            // if (isNew) TODO, send notification to app if any
            cbPair();
          });
        });
      }, cbApp);
    });
  }, cbDone);
};

// index additional attributes on friends
exports.friendex = function (auth, friends, cbDone) {
  logger.debug("friendex", auth.pid, Object.keys(friends).length);

  async.forEachLimit(Object.keys(friends), 10, function (pid, cbFriends) {
    var friend = friends[pid];
    var oe = dMap.get('oembed', friend.data, friend.idr) || {};

    async.waterfall([
      function (cb) { // handle indexing the related ids
        var options = { pars: [] };
        options.pars.push(pid2par(pid));

        if (oe.url) {
          options.pars.push(parts2par([
            dMap.parallelType("url"),
            str2num(oe.url)
          ]));
        }

        if (oe.website) {
          options.pars.push(parts2par([
            dMap.parallelType("url"),
            str2num(oe.website)
          ]));
        }

        // TODO add a par for relation, matching school/employer to auth.profile
        // index bio text
        var biotext = bioget(friend, oe);
        var buf = qix.buf(biotext);

        if (buf) {
          options.q = [];

          options.q.push(buf.slice(0, 8).toString('hex'));
          options.q.push(buf.slice(8, 16).toString('hex'));
          options.q.push(buf.slice(16, 24).toString('hex'));
          options.q.push(buf.slice(24).toString('hex'));

          options.text = biotext; // pass along raw for ijod eventing
        }

        ijod.setOneCat(idr.toString(friend.idr), "ids", options, cb);
      },
      function (cb) {
        // TODO interest par (keywords from bio, interests on facebook, device
        // types) driven by apps?
        cb();
      }
    ], function () {
      cbFriends();
    });
  }, cbDone);
};

// we need to index our own unique identifiers in an easily matchable way to
// find peers in an app
exports.indexMe = function (auth, entry, cbDone) {
  if (!entry) return cbDone();

  logger.debug("indexing me", entry.idr);
  var id = idr.parse(entry.idr);
  var isDevice = (id.host === 'devices' && id.protocol === 'contact');

  // first get all the unique ids, the device information is more rich than
  // oembed (plurals), use that if so
  var ids = allIds(isDevice ?
    entry.data :
    dMap.get('oembed', entry.data, entry.idr) || {});

  // get a list of apps from either auth or device
  var apps = auth && auth.apps;

  if (isDevice) {
    // have to spoof the auth.apps.accounts[] pattern
    var parts = idr.parse(entry.idr).auth.split('.');

    apps = {};
    apps[parts[2]] = { accounts: {} };
    apps[parts[2]].accounts[parts[1]] = true;
  }

  // now for each one save this account to it's app-wide entry
  var entries = [];

  Object.keys(apps).forEach(function (app) {
    if (typeof(apps[app].accounts) !== 'object') return;

    Object.keys(apps[app].accounts).forEach(function (account) {
      var base = 'index:' + app + '/account';
      logger.debug("deviceMe", base, ids);

      Object.keys(ids).forEach(function (id) {
        var index = {};
        index.idr = base + '#' + encodeURIComponent(id);
        index.account = account;
        index.data = {};
        index.via = idr.toString(entry.idr);
        entries.push(index);
      });
    });
  });

  ijod.batchSmartAdd(entries, cbDone);
};

// are any of these compadres using the app too?
exports.devicePeers = function (account, app, pids, cbDone) {
  if (Object.keys(pids).length === 0) return cbDone();

  var base = 'index:' + app + '/account';

  async.forEachLimit(Object.keys(pids), 5, function (pid, cbPids) {
    var ids = allIds(pids[pid].data);

    if (Object.keys(ids).length === 0) return process.nextTick(cbPids);

    async.forEach(Object.keys(ids), function (id, cbIds) {
      ijod.getOne(base + '#' + encodeURIComponent(id), function (err, entry) {
        if (!entry || !entry.account) return cbIds();

        // set up peering from this account to the other, pid is a localized
        // id@devices
        friendPeer(app, account, entry.account, pid, function (isNew) {
          // importantly, make sure reverse is set too!
          // use their localized device id for me
          var viaPid = encodeURIComponent(idr.parse(entry.via).hash) + '@devices';

          friendPeer(app, entry.account, account, viaPid, function () {
            // if (isNew) TODO, send notification to app if any
            pids[pid].peer = entry.account; // convenience for immediate action
            cbIds();
          });
        });
      });
    }, cbPids);
  }, cbDone);
};
