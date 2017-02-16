const config = require('./config');
const pg = require('pg');
const url = require('url');


var postgres_params = url.parse(config.postgresUri);
var postgres_auth = postgres_params.auth.split(':');

var pg_config = {
  user: postgres_auth[0],
  password: postgres_auth[1],
  host: postgres_params.hostname,
  port: postgres_params.port,
  database: postgres_params.pathname.split('/')[1],
  ssl: true,
  max: 10,
  idleTimeoutMillis: 30000
};


var params = url.parse(config.redshiftUri);
var auth = params.auth.split(':');

var rs_config = {
  user: auth[0],
  password: auth[1],
  host: params.hostname,
  port: params.port,
  database: params.pathname.split('/')[1],
  ssl: true,
  max: 10,
  idleTimeoutMillis: 30000
};


module.exports.postgres = new pg.Pool(pg_config);
module.exports.redshift = new pg.Pool(rs_config);

