const config = require('./config');
const winston = require('winston');

winston.level = config.logLevel;
winston.remove(winston.transports.Console);
winston.add(winston.transports.File, {'colorize': true, 'filename': config.logFile, 'timestamp': true});
winston.info('Logger is set up!');

module.exports = winston;
