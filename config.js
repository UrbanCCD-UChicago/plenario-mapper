var config = {};

config.consumerToken = process.env.CONSUMER_TOKEN;
config.postgresUri = process.env.POSTGRES_URI;
config.redshiftUri = process.env.REDSHIFT_URI;
config.logFile = process.env.LOG_FILE || 'mapper.log';
config.logLevel = process.env.LOG_LEVEL || 'error';
config.plenarioHost = process.env.PLENARIO_HOST || 'localhost:8080';
config.publisherUrl = process.env.PUBLISHER_URL || 'http://streaming.plenar.io/';

module.exports = config;
