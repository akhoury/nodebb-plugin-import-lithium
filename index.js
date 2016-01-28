
var async = require('async');
var mysql = require('mysql');

//var utils = module.parent.require('../../public/src/utils.js');

var _ = require('underscore');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-lithium]';

var PLACEHOLDER = '___________________________placeholder___________________________';
var RTRIMREGEX = /\s+$/g;

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

		var startms = +new Date();
		console.log('\n\n====QUERY====\n\n' + query + '\n');
		Exporter.connection.query(query, function(err, rows) {
			if (rows) {
				console.log('returned: ' + rows.length + ' results in: ' + ((+new Date) - startms) + 'ms');
			}
			callback(err, rows)
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
				//+ prefix + 'users_dec.secure_id as _suid, ' + '\n'
				+ prefix + 'users_dec.nlogin as _username, ' + '\n'
				+ prefix + 'users_dec.login_canon as _alternativeUsername, ' + '\n'
				//+ prefix + 'users_dec.npasswd as _hashed_password, ' + '\n'
				+ prefix + 'users_dec.email as _email, ' + '\n'
				+ prefix + 'users_dec.registration_time as _joindate, ' + '\n'
				+ prefix + 'users_dec.last_visit_time as _lastonline, ' + '\n'

				+ 'bans.deleted as _deleted, ' + '\n'
				//+ 'bans.date_start as _l_ban_date_start, ' + '\n'
				//+ 'bans.date_end as _l_ban_date_end, ' + '\n'

				// todo: need to figure out how to map those rankings
				// + prefix + 'users_dec.ranking_id as _l_ranking_id, ' + '\n'

				+ 'rankings.rank_name as _rank, ' + '\n'
				+ 'rankings.equals_role as _level, ' + '\n'
				+ 'roles.name as _role, ' + '\n'

				+ 'website.nvalue as _website, ' + '\n'
				+ 'location.nvalue as _location, ' + '\n'
				+ 'signature.nvalue as _signature ' + '\n'

				+ 'FROM ' + prefix + 'users_dec ' + '\n'

				+ 'LEFT JOIN ' + prefix + 'user_role AS role ON role.role_id=' + prefix + 'users_dec.id ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'roles AS roles ON roles.id=role.role_id ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'user_rankings AS rankings ON rankings.id=' + prefix + 'users_dec.ranking_id ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'user_bans AS bans ON bans.user_id=' + prefix + 'users_dec.id ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'user_profile_dec AS website ON website.user_id=' + prefix + 'users_dec.id AND website.param="profile.url_homepage" ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'user_profile_dec AS location ON location.user_id=' + prefix + 'users_dec.id AND location.param="profile.location" ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'user_profile_dec AS signature ON signature.user_id=' + prefix + 'users_dec.id AND signature.param="profile.signature" ' + '\n'
				+ 'GROUP BY ' + prefix + 'users_dec.id ' + '\n'

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

						var gid = row._level || row._role;
						if (gid && gid.toLowerCase() !== "administrator" && gid.toLowerCase() !== "moderator") {
							row._groups = [gid];
						}

						if (row._uid == 10) {
							row._level = "moderator";
						}

						// if
						// the user was deleted, ban that user
						// or if the ban ends if the future, ban that user
						// or if the ban starts if the future, ban that user
						row._banned = row._deleted || (row._l_ban_date_start && row._l_ban_date_start > startms) || (row._l_ban_date_end && row._l_ban_date_end > startms) ? 1 : 0;

						map[row._uid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getCategories = function(callback) {
		return Exporter.getPaginatedCategories(0, -1, callback);
	};

	Exporter.getPaginatedCategories = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix');
		var startms = +new Date();

		var query = ''
				+ 'SELECT ' + '\n'
				+ 'category.node_id as _cid, ' + '\n'
				+ prefix + 'nodes.parent_node_id as _parentCid, ' + '\n'
				+ 'category.nvalue as _name ' + '\n'

				+ 'FROM ' + prefix + 'nodes ' + '\n'

				+ 'LEFT JOIN ' + prefix + 'settings AS category ON category.node_id = ' + prefix + 'nodes.node_id '
				+ 'AND (category.param="board.title" OR category.param="category.title") ' + '\n'
				+ 'WHERE category.node_id IS NOT NULL \n'

				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row, i) {
						map[row._cid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.countTopics = function(callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT count(*) ' + '\n'
				+ 'FROM ' + prefix + 'message2 ' + '\n'
				+ 'WHERE ' + prefix + 'message2.id = ' + prefix + 'message2.root_id ' + '\n';

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

		var query = ''
				+ 'SELECT ' + '\n'
				+ prefix + 'message2.unique_id as _tid, ' + '\n'
				+ 'category.node_id as _cid, ' + '\n'
				+ prefix + 'message2.user_id as _uid, ' + '\n'
				+ prefix + 'message2_content.subject as _title, ' + '\n'
				+ prefix + 'message2_content.body as _content, ' + '\n'
				+ prefix + 'message2.post_date as _timestamp, ' + '\n'
				+ prefix + 'message2.edit_date as _edited, ' + '\n'
				+ prefix + 'message2.deleted as _deleted ' + '\n'
				+ 'FROM ' + prefix + 'message2 ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'settings AS category ON category.node_id = ' + prefix + 'message2.node_id AND (category.param="board.title" OR category.param="category.title") ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'message2_content ON ' + prefix + 'message2_content.unique_id = ' + prefix + 'message2.unique_id ' + '\n'
				+ 'WHERE ' + prefix + 'message2.id = ' + prefix + 'message2.root_id ' + '\n'
				// + 'AND ' + prefix + 'message2.user_id != -1 '+ '\n'
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row, i) {
						row._title = row._title && row._title.replace(RTRIMREGEX, '') ? row._title : PLACEHOLDER;
						row._content = row._content && row._content.replace(RTRIMREGEX, '') ? row._content : PLACEHOLDER;
						map[row._tid] = row;
					});

					callback(null, map, rows);
				});
	};

	Exporter.countPosts = function(callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT count(*) ' + '\n'
				+ 'FROM ' + prefix + 'message2 ' + '\n'
				+ 'WHERE ' + prefix + 'message2.id != ' + prefix + 'message2.root_id ' + '\n';

		Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					callback(null, rows[0]['count(*)']);
				});
	};

	Exporter.getPosts = function(callback) {
		return Exporter.getPaginatedPosts(0, -1, callback);
	};

	Exporter.getPaginatedPosts = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix');
		var startms = +new Date();

		var query = ''
				+ 'SELECT ' + '\n'
				+ prefix + 'message2.unique_id as _pid, ' + '\n'
				+ 'topics.unique_id as _tid, ' + '\n'
				+ 'parents.unique_id as _toPid, ' + '\n'

				+ prefix + 'message2.user_id as _uid, ' + '\n'
				+ prefix + 'message2_content.body as _content, ' + '\n'
				+ prefix + 'message2.post_date as _timestamp, ' + '\n'
				+ prefix + 'message2.edit_date as _edited, ' + '\n'
				+ prefix + 'message2.deleted as _deleted ' + '\n'
				+ 'FROM ' + prefix + 'message2 ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'message2_content ON ' + prefix + 'message2_content.unique_id = ' + prefix + 'message2.unique_id ' + '\n'
				+ 'LEFT JOIN ' + prefix + 'message2 AS topics ON topics.id = ' + prefix + 'message2.root_id AND topics.node_id = ' + prefix + 'message2.node_id  \n'
				+ 'LEFT JOIN ' + prefix + 'message2 AS parents ON parents.id = ' + prefix + 'message2.parent_id AND parents.node_id = ' + prefix + 'message2.node_id  \n'

				+ 'WHERE ' + prefix + 'message2.id != ' + prefix + 'message2.root_id ' + '\n'
				// + 'AND ' + prefix + 'message2.user_id != -1 '+ '\n'

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
						if (row._tid === row._toPid) {
							delete row._toPid;
						}
						row._content = row._content && row._content.replace(RTRIMREGEX, '') ? row._content : PLACEHOLDER;
						map[row._pid] = row;
					});

					callback(null, map);
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

				// use the name as gid and group by name, since there seems to be duplicate groups names
				// then in getUsers, use the names in _groups
				+ prefix + 'roles.name as _gid, '

				+ prefix + 'roles.name as _name '

				+ 'FROM ' + prefix + 'roles '
				+ 'WHERE ' + prefix + 'roles.name != "Administrator" '
				+ 'GROUP BY ' + prefix + 'roles.name '
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
			function(next) {
				Exporter.getGroups(next);
			},
			function(next) {
				Exporter.getUsers(next);
			},
			function(next) {
				Exporter.getCategories(next);
			},
			function(next) {
				Exporter.getTopics(next);
			},
			function(next) {
				Exporter.getPosts(next);
			},
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
				console.log("groups");

				Exporter.getPaginatedUsers(10000, 10250, next);
			},
			function(next) {
				console.log("users");

				Exporter.getPaginatedCategories(0, 1000, next);
			},
			function(next) {
				console.log("categories");

				Exporter.getPaginatedTopics(0, 1000, next);
			},
			function(next) {
				console.log("topics");

				Exporter.getPaginatedPosts(1001, 2000, next);
			},
			function(next) {
				console.log("posts");

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
