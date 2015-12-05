var fs = require('fs-extra');

require('./index').testrun({
    dbhost: 'localhost',
    dbport: 3306,
    dbname: 'lithium',
    dbuser: 'user',
    dbpass: 'password',

    tablePrefix: ''
}, function(err, results) {

	fs.writeFileSync('./tmp.json', JSON.stringify(results, undefined, 2));
});