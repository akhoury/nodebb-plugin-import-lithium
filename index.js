

/*


 SELECT metrics2.id as id, name, value FROM metrics2_name JOIN metrics2 ON metrics2_name.id = metrics2.name_id limit 1000



 SELECT
 message2.id as _pid,
 title.node_id as _tid, // WRONG< LOOKS MORE LIKE A CID, see root_id and parent_id
 message2.user_id as _uid,
 message2_content.body as _content,
 message2.post_date as _datetime,
 message2.deleted as _deleted,

 title.nvalue as _board_title,
 message2.root_id as _l_root_id,
 message2.parent_id as _l_parent_id,
 message2_content.unique_id as _l_uuid,
 message2_content.subject as _l_subject,
 message2_content.teaser as _l_teaser,
 nodes.parent_node_id _l_parent_node_id,
 nodes.type_id as _l_type_id,
 nodes.display_id as _l_display_id


 FROM message2
 LEFT JOIN nodes ON nodes.node_id = message2.node_id
 LEFT JOIN settings AS title ON title.node_id = message2.node_id AND title.param="board.title"
 LEFT JOIN message2_content ON message2_content.unique_id = message2.unique_id

 LIMIT 1000


 */

var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-lithium]';

(function(Exporter) {

	var csvToArray = function(v) {
		return !Array.isArray(v) ? ('' + v).split(',').map(function(s) { return s.trim(); }) : v;
	};

	Exporter.setup = function(config, callback) {
		Exporter.log('setup');

		// mysql db only config
		// extract them from the configs passed by the nodebb-plugin-import adapter
		var _config = {
			host: config.dbhost || config.host || 'localhost',
			user: config.dbuser || config.user || 'user',
			password: config.dbpass || config.pass || config.password || undefined,
			port: config.dbport || config.port || 3306,
			database: config.dbname || config.name || config.database || 'lithium'
		};

		Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || '');

		config.custom = config.custom || {};
		if (typeof config.custom === 'string') {
			try {
				config.custom = JSON.parse(config.custom)
			} catch (e) {}
		}

		Exporter.config('custom', config.custom || {});

		Exporter.connection = mysql.createConnection(_config);
		Exporter.connection.connect();

		callback(null, Exporter.config());
	};

	Exporter.query = function(query, callback) {
		if (!Exporter.connection) {
			var err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		console.log('\n\n====QUERY====\n\n' + query + '\n');
		Exporter.connection.query(query, function(err, rows) {
			if (rows) {
				console.log('returned: ' + rows.length + ' results');
			}
			callback(err, rows)
		});
	};


	Exporter.getGroups = function(callback) {
		return Exporter.getPaginatedGroups(0, -1, callback);
	};

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix') || '';
		var startms = +new Date();

		var query = 'SELECT '
				+ prefix + 'roles.id as _gid, '
				+ prefix + 'roles.name as _name '
				+ ' FROM ' + prefix + 'roles '

				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					//normalize here
					var map = {};
					rows.forEach(function(row) {
						map[row._gid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.countUsers = function (callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix') || '';
		var query = 'SELECT count(*) '
				+ 'FROM ' + prefix + 'users_dec ';

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					callback(null, rows[0]['count(*)']);
				});
	};

	Exporter.getUsers = function(callback) {
		return Exporter.getPaginatedUsers(0, -1, callback);
	};
	Exporter.getPaginatedUsers = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix') || '';
		var startms = +new Date();

		var query = ''
				+ 'SELECT '
				+ prefix + 'users_dec.id as _uid, ' + '\n'
				+ prefix + 'users_dec.secure_id as _suid, ' + '\n'
				+ prefix + 'users_dec.nlogin as _username, ' + '\n'
				+ prefix + 'users_dec.login_canon as _alternativeUsername, ' + '\n'
				+ prefix + 'users_dec.npasswd as _hashed_password, ' + '\n'
				+ prefix + 'users_dec.email as _email, ' + '\n'
				+ prefix + 'users_dec.registration_time as _joindate, ' + '\n'
				+ prefix + 'users_dec.last_visit_time as _lastonline, ' + '\n'

				// + prefix + 'users_dec.metrics_id as _l_metrics_id, ' + '\n'
				// + prefix + 'users_dec.ranking_id as _l_ranking_id, ' + '\n'

				+ 'rankings.equals_role as _level, ' + '\n'
				+ 'rankings.rank_name as _rank, ' + '\n'

				+ 'website.nvalue as _website, ' + '\n'
				+ 'location.nvalue as _location, ' + '\n'
				+ 'signature.nvalue as _signature ' + '\n'

				+ 'FROM ' + prefix + 'users_dec ' + '\n'

				+ 'LEFT JOIN ' + prefix + 'user_rankings AS rankings ON rankings.id=' + prefix + 'users_dec.ranking_id ' + '\n'

				+ 'LEFT JOIN ' + prefix + 'user_profile_dec AS website ON website.user_id=' + prefix + 'users_dec.id AND website.param="profile.url_homepage" ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'user_profile_dec AS location ON location.user_id=' + prefix + 'users_dec.id AND location.param="profile.location" ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'user_profile_dec AS signature ON signature.user_id=' + prefix + 'users_dec.id AND signature.param="profile.signature" ' + '\n'

				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {

						// lower case the email for consistency
						row._email = (row._email || '').toLowerCase();

						row._website = Exporter.validateUrl(row._website);

						map[row._uid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.countMessages = function(callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');

		var query = 'SELECT count(*) '
				+ 'FROM ' + prefix + 'pm '
				+ 'LEFT JOIN ' + prefix + 'pmtext ON ' + prefix + 'pmtext.pmtextid=' + prefix + 'pm.pmtextid ';

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					callback(null, rows[0]['count(*)']);
				});
	};

	Exporter.getMessages = function(callback) {
		return Exporter.getPaginatedMessages(0, -1, callback);
	};

	Exporter.getPaginatedMessages = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var startms = +new Date();
		var prefix = Exporter.config('prefix') || '';
		var query = 'SELECT '
				+ prefix + 'pm.pmid as _mid, '
				+ prefix + 'pmtext.fromuserid as _fromuid, '
				+ prefix + 'pm.userid as _touid, '
				+ prefix + 'pmtext.message as _content, '
				+ prefix + 'pmtext.dateline as _timestamp '
				+ 'FROM ' + prefix + 'pm '
				+ 'LEFT JOIN ' + prefix + 'pmtext ON ' + prefix + 'pmtext.pmtextid=' + prefix + 'pm.pmtextid '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					//normalize here
					var map = {};
					rows.forEach(function(row) {
						row._timestamp = ((row._timestamp || 0) * 1000) || startms;
						map[row._mid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.countCategories = function(callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT count(*) FROM ' + prefix + 'forum ';

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					callback(null, rows[0]['count(*)']);
				});
	};

	Exporter.getCategories = function(callback) {
		return Exporter.getPaginatedCategories(0, -1, callback);
	};

	Exporter.getPaginatedCategories = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix');
		var startms = +new Date();

		var query = 'SELECT '
				+ prefix + 'forum.forumid as _cid, '
				+ prefix + 'forum.title as _name, '
				+ prefix + 'forum.description as _description, '
				+ prefix + 'forum.displayorder as _order '
				+ 'FROM ' + prefix + 'forum ' // filter added later
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						row._name = row._name || 'Untitled Category ';
						row._description = row._description || 'No decsciption available';
						row._timestamp = ((row._timestamp || 0) * 1000) || startms;
						map[row._cid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.countTopics = function(callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT count(*) '
				+ 'FROM ' + prefix + 'thread '
				+ 'JOIN ' + prefix + 'post ON ' + prefix + 'thread.firstpostid=' + prefix + 'post.postid ';

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					callback(null, rows[0]['count(*)']);
				});
	};

	Exporter.getTopics = function(callback) {
		return Exporter.getPaginatedTopics(0, -1, callback);
	};
	Exporter.getPaginatedTopics = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
				+ prefix + 'thread.threadid as _tid, '
				+ prefix + 'post.userid as _uid, '
				+ prefix + 'thread.firstpostid as _pid, '
				+ prefix + 'thread.forumid as _cid, '
				+ prefix + 'post.title as _title, '
				+ prefix + 'post.pagetext as _content, '
				+ prefix + 'post.username as _guest, '
				+ prefix + 'post.ipaddress as _ip, '
				+ prefix + 'post.dateline as _timestamp, '
				+ prefix + 'thread.views as _viewcount, '
				+ prefix + 'thread.open as _open, '
				+ prefix + 'thread.sticky as _pinned '
				+ 'FROM ' + prefix + 'thread '
				+ 'JOIN ' + prefix + 'post ON ' + prefix + 'thread.firstpostid=' + prefix + 'post.postid '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
						row._timestamp = ((row._timestamp || 0) * 1000) || startms;
						row._locked = row._open ? 0 : 1;

						map[row._tid] = row;
					});

					callback(null, map, rows);
				});
	};

	Exporter.countPosts = function(callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT count(*)  '
				+ 'FROM ' + prefix + 'post WHERE ' + prefix + 'post.parentid<>0 ';

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					callback(null, rows[0]['count(*)']);
				});
	};

	var processFirstPostsHash = function(arr) {
		var hash = {};
		arr.forEach(function(topic) {
			hash[topic._pid] = 1;
		});
		Exporter.firstPostsHash = hash;
		return hash;
	};

	var getFirstPostsHash = function(callback) {
		if (Exporter.firstPostsHash) {
			return callback(null, Exporter.firstPostsHash)
		}

		Exporter.getTopics(function(err, map, arr) {
			if (err) return callback(err);

			callback(null, processFirstPostsHash(arr));
		});
	};

	Exporter.getPosts = function(callback) {
		return Exporter.getPaginatedPosts(0, -1, callback);
	};

	Exporter.getPaginatedPosts = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
				+ prefix + 'post.postid as _pid, '
				+ prefix + 'post.threadid as _tid, '
				+ prefix + 'post.userid as _uid, '
				+ prefix + 'post.username as _guest, '
				+ prefix + 'post.ipaddress as _ip, '
				+ prefix + 'post.pagetext as _content, '
				+ prefix + 'post.dateline as _timestamp '
				+ 'FROM ' + prefix + 'post WHERE ' + prefix + 'post.parentid<>0 '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		getFirstPostsHash(function(err, topicsPids) {
			if (err) {
				return callback(err);
			}

			Exporter.query(query,
					function(err, rows) {
						if (err) {
							Exporter.error(err);
							return callback(err);
						}

						//normalize here
						var map = {};
						rows.forEach(function(row) {
							if (topicsPids[row._pid]) {
								return;
							}

							row._content = row._content || '';
							row._timestamp = ((row._timestamp || 0) * 1000) || startms;
							map[row._pid] = row;
						});

						callback(null, map);
					});
		});
	};

	Exporter.teardown = function(callback) {
		Exporter.log('teardown');
		Exporter.connection.end();

		Exporter.log('Done');
		callback();
	};

	Exporter.testrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			//function(next) {
			//	Exporter.getGroups(next);
			//},
			function(next) {
				Exporter.getUsers(next);
			},
			//function(next) {
			//	Exporter.getMessages(next);
			//},
			//function(next) {
			//	Exporter.getCategories(next);
			//},
			//function(next) {
			//	Exporter.getTopics(next);
			//},
			//function(next) {
			//	Exporter.getPosts(next);
			//},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.paginatedTestrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getPaginatedGroups(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedUsers(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedMessages(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedCategories(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedTopics(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedPosts(1001, 2000, next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.warn = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.warn.apply(console, args);
	};

	Exporter.log = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.log.apply(console, args);
	};

	Exporter.error = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.error.apply(console, args);
	};

	Exporter.config = function(config, val) {
		if (config != null) {
			if (typeof config === 'object') {
				Exporter._config = config;
			} else if (typeof config === 'string') {
				if (val != null) {
					Exporter._config = Exporter._config || {};
					Exporter._config[config] = val;
				}
				return Exporter._config[config];
			}
		}
		return Exporter._config;
	};

	// from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
	Exporter.validateUrl = function(url) {
		var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
		return url && url.length < 2083 && url.match(pattern) ? url : '';
	};

	Exporter.truncateStr = function(str, len) {
		if (typeof str != 'string') return str;
		len = _.isNumber(len) && len > 3 ? len : 20;
		return str.length <= len ? str : str.substr(0, len - 3) + '...';
	};

	Exporter.whichIsFalsy = function(arr) {
		for (var i = 0; i < arr.length; i++) {
			if (!arr[i])
				return i;
		}
		return null;
	};

})(module.exports);
