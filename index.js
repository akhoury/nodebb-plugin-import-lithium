
var extend = require('extend');
var async = require('async');
var mysql = require('mysql');
var fs = require('fs-extra');
var path = require('path');

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
			// , socketPath: '/Applications/MAMP/tmp/mysql/mysql.sock'
		};

		Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || '');

		config.custom = config.custom || {};
		if (typeof config.custom === 'string') {
			try {
				config.custom = JSON.parse(config.custom)
			} catch (e) {}
		}
		config.custom = extend(true, {}, {
			timemachine: {
				users: {},
				topics: {},
				categories: {},
				posts: {}
			}
		}, config.custom);


		Exporter.config('custom', config.custom || {});

		Exporter.connection = mysql.createConnection(_config);
		Exporter.connection.connect();

		setInterval(function() {
			Exporter.connection.query("SELECT 1", function(){});
		}, 60000);

		callback(null, Exporter.config());
	};

	Exporter.query = function(query, callback) {
		if (!Exporter.connection) {
			var err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		var startms = +new Date();
		console.log('\n\n====QUERY====\n\n' + (new Date()).toString() + '\n' + query + '\n');
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

			+ 'GROUP_CONCAT('+ prefix + 'user_role.role_id) as _groups ' + '\n'

			+ 'FROM ' + prefix + 'users_dec ' + '\n'
			+ 'LEFT JOIN ' + prefix + 'user_role ON user_role.user_id=' + prefix + 'users_dec.id ' + '\n'

			+ 'LEFT JOIN ' + prefix + 'user_rankings AS rankings ON rankings.id=' + prefix + 'users_dec.ranking_id ' + '\n'
			+ 'LEFT JOIN ' + prefix + 'user_bans AS bans ON bans.user_id=' + prefix + 'users_dec.id ' + '\n'

			+ 'GROUP BY ' + prefix + 'users_dec.id ' + '\n'

			+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		geUsersProfileDec(function(err, userProfileDecMap) {
			getAllGroupsFlattened(function (err, groups) {

				var replaceChildGroupWithRootGroup = function (_gid) {
					if (!groups[_gid]) {
						return null;
					}
					return groups[_gid]._rootGid ? parseInt(groups[_gid]._rootGid, 10) : parseInt(_gid, 10);
				};

				Exporter.query(query,
					function(err, rows) {
						if (err) {``
							Exporter.error(err);
							return callback(err);
						}

						//normalize here
						var map = {};
						rows.forEach(function(row) {

							// lower case the email for consistency
							row._email = (row._email || '').toLowerCase();

							var profile = userProfileDecMap[row._uid] || {};
							row._signature = profile.signature;
							row._location = profile.location;
							row._website = Exporter.validateUrl(profile.website);
							row._picture = profile.picture;
							row._level = row._level ? row._level.toLowerCase() : row._level;

							row._groups = row._groups ? csvToArray(row._groups) : [];
							row._groups = row._groups.map(replaceChildGroupWithRootGroup);

							row._groups = row._groups.filter(function (_gid, index) {
								return !!_gid && this.indexOf(_gid) == index;
							}, row._groups);

							// if
							// the user was deleted, ban that user
							// or if the ban ends in the future, ban that user
							// or if the ban starts in the future, ban that user
							row._banned = row._deleted || (row._l_ban_date_start && row._l_ban_date_start > startms) || (row._l_ban_date_end && row._l_ban_date_end > startms) ? 1 : 0;

							map[row._uid] = row;
						});

						callback(null, map);
					});
			});
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
			+ 'SELECT \n'
			+ 'category.node_id as _cid, \n'
			+ prefix + 'nodes.hidden as _disabled, \n'
			+ prefix + 'nodes.position as _order, \n'
			+ prefix + 'nodes.parent_node_id as _parentCid, \n'
			+ 'category.nvalue as _name, \n'
			+ 'GROUP_CONCAT(' + prefix + 'roles.name) as _groupNames, \n'
			+ 'GROUP_CONCAT(' + prefix + 'roles.id) as _gids \n'

			+ 'FROM ' + prefix + 'nodes \n'

			+ 'LEFT JOIN ' + prefix + 'settings AS category ON category.node_id = ' + prefix + 'nodes.node_id AND (category.param="board.title" OR category.param="category.title") \n'
			+ 'LEFT JOIN ' + prefix + 'roles ON ' + prefix + 'roles.node_id = category.node_id \n'
			+ 'WHERE category.node_id IS NOT NULL \n'
			+ 'GROUP BY 1 '
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
					row._groupNames = csvToArray(row._groupNames);
					row._gids = csvToArray(row._gids);
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

	function geUsersProfileDec (callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix') || '';

		Exporter.query('select * from user_profile_dec ', function(err, rows) {
			if (err) {
				return callback(err)
			}
			var map = {};
			rows.forEach(function(row) {
				map[row.user_id] = map[row.user_id] || {};
				if (row.param == 'profile.location') {
					map[row.user_id].location = row.nvalue;
				} else if (row.param == 'profile.signature') {
					map[row.user_id].signature = replaceLiImages(row.nvalue);
				} else if (row.param == 'profile.url_homepage') {
					map[row.user_id].website = row.nvalue;
				} else if (row.param == 'profile.url_icon' && row.nvalue && !/^avatar:/.test(row.nvalue)) {
					map[row.user_id].picture = replaceLocalImage(row.nvalue);
				}
			});
			Exporter._usersProfileDec = map;
			callback(null, map);
		});
	}

	var pad = function (val, len) {
		val = String(val);
		len = len || 4;
		while (val.length < len) val = "0" + val;
		return val;
	};

	// todo: expose the baseUrl options from the UI via custom configs maybe?
	// .dat ? really lithium?
	// this whole thing is crap
	var copyDATAttachmentAndGetNewUrl = Exporter.copyDATAttachmentAndGetNewUrl = function (_aid, filename, options) {
		options = options || {
				baseUrl: '/uploads/_imported_attachments/',
				attachmentsDir: path.join(__dirname, '/../../public', '/uploads/_imported_attachments/dat_files')
		};
		var thousand = Math.floor(_aid / 1000);
		var parentDir = pad(thousand, 4); // todo: what happens if the lithium folders go over 9999?
		var originalFile = options.attachmentsDir + '/' + parentDir + '/' + pad(_aid, 4) + '.dat';
		var newFilePath = parentDir + '/' + _aid + '_' + filename;
		var newFileFullFSPath = path.join(__dirname, '/../../public', '/uploads/_imported_attachments/', newFilePath);

		// TODO: wtf is that? sync copy? meh
		if (!fs.existsSync(newFileFullFSPath)) {
			fs.copySync(originalFile, newFileFullFSPath);
		}

		return options.baseUrl + newFilePath;
	};

	function filterNonImage (attachment) {
		return !filterImage(attachment);
	}
	function filterImage (attachment) {
		return /^image/.test(attachment.mime);
	}

	// find the first (default) img src in a string
	var _findImgsRE = /<img[^>]+src='?"?([^'"\s>]+)'?"?\s*.*\/?>/gi;
	function findImgSrc (str, options) {
		options = options || {first: true, last: false, index: 0};
		var results = _findImgsRE.exec(str || '');
		if(results) {
			return results[options.first ? 1 : options.last ? results.length - 1 : options.index + 1];
		}
	}


    function replaceLocalImage (img) {
        // todo: i wonder if should be customizeable, via custom options or something, just in case someone didn't want to place them in /uploads/_imported_images ...
        // one would then write a quick script to cahgne them, but still...

        return (img || '')
        // to replace these
        // /t5/image/serverpage/image-id/15729i1DC8C447E1A52650/image-size/avatar?v=mpbl-1&px=64
        // http://community.ubnt.com/t5/image/serverpage/image-id/303i4EB092C27479A960/image-size/avatar?v=mpbl-1&px=64
        // https://community.ubnt.com/t5/image/serverpage/image-id/303i4EB092C27479A960/image-size/avatar?v=mpbl-1&px=64
        // http://ubnt.i.lithium.com/t5/image/serverpage/image-id/90761iE903E8F26321A876/image-size/avatar?v=v2&px=64
        // https://ubnt.i.lithium.com/t5/image/serverpage/image-id/69492iDE646C12F51EB728/image-size/avatar?v=mpbl-1&px=64
            .replace(/(.*)\/t\d*\/image\/serverpage\/image-id\/(\w+)\/image-size\/.*\?/g, '/uploads/_imported_images/$2\?')
            // http://community.ubnt.com/legacyfs/online/avatars/931_wifi-coffee.gif
            .replace(/(.*)\/legacyfs\/online\/avatars\/(.*)/g, '/uploads/_imported_images/legacy_avatars/$2\?');
    }

    function replaceLiImages (content) {
		content = content || '';

		//todo: redundant, use replaceLocalImage()
		content = content.replace(/(http.*)?\/t\d*\/image\/serverpage\/image-id\/(\w+)\/image-size\/.*\?/ig, '/uploads/_imported_images/$2\?');
        content = content.replace(/(http.*)?\/legacyfs\/online\/avatars\/(.*)/ig, '/uploads/_imported_images/legacy_avatars/$2\?');

        content = content.replace(/<li-image[^>]+id='?"?([^'"\s>]+)'?"?/ig, '<li-image src="/uploads/_imported_images/$1"');
        content = content.replace(/li-image/ig, 'img');
        return content;
    }

    var getAttachmentsMap = function (callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');

		if (Exporter['_attachmentsMap']) {
			return callback(null, Exporter['_attachmentsMap']);
		}

		var query = 'SELECT '
			+ prefix + 'tblia_attachment.attachment_id as _aid, '
			+ prefix + 'tblia_attachment.file_name as _fname, '
			+ prefix + 'tblia_attachment.content_type as _mime, '
			+ prefix + 'tblia_message_attachments.message_uid as _tpid, '
			+ prefix + 'tblia_message_attachments.attach_num as _num '

			+ 'FROM ' + prefix + 'tblia_attachment '
			+ 'JOIN ' + prefix + 'tblia_message_attachments ON ' + prefix + 'tblia_message_attachments.attachment_id=' + prefix + 'tblia_attachment.attachment_id '

		var byNum = function (a, b) {
			return a.order - b.order;
		};

		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				var map = {};
				rows.forEach(function(row) {
					// use both, the tpid and the aid to make sure topic/post and the attachment ids match
					map[row._tpid] = map[row._tpid] || [];
					map[row._tpid].push({url: copyDATAttachmentAndGetNewUrl(row._aid, row._fname), filename: row._fname, order: row._num, mime: row._mime});
					map[row._tpid].sort(byNum);
				});

				Exporter['_attachmentsMap'] = map;

				callback(err, map);
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
			+ 'GREATEST( ' + prefix + 'message2.views, 0) as _viewcount, ' + '\n'
			+ prefix + 'message2.post_date as _timestamp, ' + '\n'
			+ prefix + 'message2.edit_date as _edited, ' + '\n'
			+ prefix + 'message2.deleted as _deleted, ' + '\n'
			+ 'GROUP_CONCAT('+ prefix + 'tags.tag_text_canon) as _tags ' + '\n'
			+ 'FROM ' + prefix + 'message2 ' + '\n'
			+ 'LEFT JOIN ' + prefix + 'settings AS category ON category.node_id = ' + prefix + 'message2.node_id AND (category.param="board.title" OR category.param="category.title") ' + '\n'
			+ 'LEFT JOIN ' + prefix + 'message2_content ON ' + prefix + 'message2_content.unique_id = ' + prefix + 'message2.unique_id ' + '\n'
			+ 'LEFT JOIN ' + prefix + 'tag_events_message ON ' + prefix + 'tag_events_message.target_id = ' + prefix + 'message2.unique_id ' + '\n'
			+ 'LEFT JOIN ' + prefix + 'tags ON ' + prefix + 'tag_events_message.tag_id = ' + prefix + 'tags.tag_id ' + '\n'
			+ 'WHERE ' + prefix + 'message2.id = ' + prefix + 'message2.root_id ' + '\n'
			// + 'AND ' + prefix + 'message2.unique_id = 538376 '+ '\n'
			+ 'GROUP BY ' + prefix + 'message2.unique_id '
			+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		getAttachmentsMap(function(err, attachmentsMap) {
			if (err) callback(err);

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
						row._content = row._content && row._content.replace(RTRIMREGEX, '') ? replaceLiImages(row._content) : PLACEHOLDER;
						row._views = row._views && row._views > 0 ? row._views : 0;
						row._attachments = (attachmentsMap[row._tid] || []).filter(filterNonImage);
						row._images  = (attachmentsMap[row._tid] || []).filter(filterImage);
						row._thumb = row._images[0] ? row._images[0].url || row._images[0] : replaceLocalImage(findImgSrc(row._content));
						map[row._tid] = row;
					});

					callback(null, map, rows);
				});
		});

	};

	Exporter.countPosts = function(callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var timemachine = Exporter.config('custom').timemachine;
		console.log(Exporter.config('custom'));
		var prefix = Exporter.config('prefix');
		var query = 'SELECT count(*) ' + '\n'
			+ 'FROM ' + prefix + 'message2 ' + '\n'
			+ 'WHERE ' + prefix + 'message2.id != ' + prefix + 'message2.root_id ' + '\n'
			+ (timemachine.posts.from ? ' AND ' + prefix + 'message2.post_date >= ' + timemachine.posts.from : '')
			+ (timemachine.posts.to ? ' AND ' + prefix + 'message2.post_date <= ' + timemachine.posts.to : '');


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
		var timemachine = Exporter.config('custom').timemachine;
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
			+ (timemachine.posts.from ? ' AND ' + prefix + 'message2.post_date >= ' + timemachine.posts.from : '')
			+ (timemachine.posts.to ? ' AND ' + prefix + 'message2.post_date <= ' + timemachine.posts.to : '')
			+ ' ORDER BY ' + prefix + 'message2.post_date \n'
			+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		getAttachmentsMap(function(err, attachmentsMap) {
			if (err) callback(err);

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
							// delete row._toPid;
							row._toPid = null;
						}
						row._content = row._content && row._content.replace(RTRIMREGEX, '') ? replaceLiImages(row._content) : PLACEHOLDER;
						row._attachments = (attachmentsMap[row._pid] || []).filter(filterNonImage);
						row._images  = (attachmentsMap[row._pid] || []).filter(filterImage);
						map[row._pid] = row;
					});

					callback(null, map);
				});
		});

	};

	Exporter.getVotes = function(callback) {
		return Exporter.getPaginatedVotes(0, -1, callback);
	};

	Exporter.getPaginatedVotes = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix') || '';
		var startms = +new Date();

		var query = 'SELECT '
			+ prefix + 'tag_events_score_message.event_id as _vid, '
			+ prefix + 'tag_events_score_message.source_id as _uid, '
			+ prefix + 'users_dec.email as _uemail, '
			+ prefix + 'tag_events_score_message.target_id as _pid, '
			+ prefix + 'tag_events_score_message.target_group1_id as _tid, '
			+ prefix + 'tag_events_score_message.tag_weight as _action '

			+ 'FROM ' + prefix + 'tag_events_score_message '
			+ 'LEFT JOIN ' + prefix + 'users_dec ON ' + prefix + 'users_dec.id = ' + prefix + 'tag_events_score_message.source_id '
			+ 'GROUP BY ' + prefix + 'tag_events_score_message.event_id '

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
					if (row._pid == row._tid) {
						delete row._pid;
					} else {
						delete row._tid;
					}
					if (row._action < 1) {
						row._action = -1;
					} else {
						row._action = 1;
					}
					map[row._vid] = row;
				});

				callback(null, map);
			});
	};


	var getAllGroupsFlattened = function(callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix') || '';
		var startms = +new Date();

		var query = 'SELECT '

			+ prefix + 'roles.id as _gid, '
			+ prefix + 'roles.name as _name, '

			+ prefix + 'roles.node_id as _mainCid, '
			+ 'parent_category.node_id as _parentCid, '

			+ 'category.nvalue as _mainCategoryName, '
			+ 'parent_category.nvalue as _parentCategoryName, '
			+ 'count(' + prefix + 'user_role.user_id) as _usersCount, '

			+ prefix + 'roles.deleted as _deleted '
			+ 'FROM ' + prefix + 'roles '
			+ 'LEFT JOIN ' + prefix + 'settings AS category ON category.node_id = ' + prefix + 'roles.node_id AND (category.param="board.title" OR category.param="category.title") '
			+ 'LEFT JOIN ' + prefix + 'nodes on category.node_id = ' + prefix + 'nodes.node_id '
			+ 'LEFT JOIN ' + prefix + 'settings AS parent_category ON parent_category.node_id = ' + prefix + 'nodes.parent_node_id AND (parent_category.param="board.title" OR parent_category.param="category.title") '
			+ 'LEFT JOIN ' + prefix + 'user_role ON ' + prefix + 'roles.id = ' + prefix + 'user_role.role_id '
			+ 'LEFT JOIN ' + prefix + 'users_dec ON ' + prefix + 'users_dec.id = ' + prefix + 'user_role.user_id /* AND ' + prefix + 'roles.node_id = 1 */ '
			+ 'WHERE 1 = 1 '

			// + 'AND ' + prefix + 'roles.deleted != 1 '

			+ 'AND ' + prefix + 'roles.name != "No PM" '
			+ 'AND ' + prefix + 'roles.name != "No Sig" '

			/*
			 + 'AND ' + prefix + 'roles.name != "Moderator" '
			 + 'AND ' + prefix + 'roles.name != "Regional Moderator" '
			 + 'AND ' + prefix + 'roles.name != "Community Team" '
			 + 'AND ' + prefix + 'roles.name!= "Banned Users" '
			 */
			+ 'GROUP BY 1 ';

		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}

				var map = {};

				var rootGroups = {};
				var rootGroupsByName = {};

				var childGroups = {};
				var childGroupsByName = {};

				var allGroups = {};

				rows.forEach(function(row) {
					if (row._parentCid) {
						row._root = false;
						childGroups[row._gid] = row;
						childGroupsByName[row._name] = row;
					} else {
						row._root = true;
						rootGroups[row._gid] = row;
						rootGroupsByName[row._name] = row;
					}
					allGroups[row._gid] = row;
				});

				var findRootGroup = function (_gid) {
					var group = allGroups[_gid];
					if (!group) {
						return null;
					}
					if (!group._parentCid) {
						return _gid;
					}
					var rootGroup = rootGroupsByName[group._name];
					if (!rootGroup) {
						return _gid;
					}
					return rootGroup._gid;
				}

				Object.keys(childGroups).forEach(function (_gid) {
					var row = childGroups[_gid];
					var mainCid = row._mainCid;
					row._rootGid = findRootGroup(_gid);
					if (row._rootGid && rootGroups[row._rootGid]) {
						rootGroups[row._rootGid]._cids = rootGroups[row._rootGid]._cids || [];
						rootGroups[row._rootGid]._cids.push(mainCid);
					}
				});
				callback(null, allGroups);
			});
	};

	Exporter.getGroups = function(callback) {
		return Exporter.getPaginatedGroups(0, -1, callback);
	};

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		getAllGroupsFlattened(function (err, groups) {
			var map = {};

			Object.keys(groups).forEach(function(_gid) {
				var group = groups[_gid];
				if (group._root && !group._deleted && group._usersCount > 0) {
					group._system = 0;
					group._hidden = 0;
					if (group._cids && group._cids.length) {
						group._private = 1;
						group._disableJoinRequests = 1;
						group._userTitleEnabled = 1;
					}
					map[group._gid] = group;
				}
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

				Exporter.getPaginatedUsers(10000, 1000, next);
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

				Exporter.getPaginatedVotes(0, -1, next);
			},
			function(next) {
				console.log("votes");

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
