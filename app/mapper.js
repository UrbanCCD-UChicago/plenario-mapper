var pg = require('pg');
var promise = require('promise');
var util = require('util');
var request = require('request');
var winston = require('winston');

var logger = require('./util/logger');
var log = logger().getLogger('mapper');

// Configure the logger
winston.level = process.env.LOG_LEVEL || "info";
winston.remove(winston.transports.Console);
winston.add(winston.transports.Console, {'colorize': true, 'timestamp': true});

// Connect to the publisher
var socket = require('socket.io-client')('http://streaming.plenar.io/', {reconnect: true, query: 'consumer_token=' + process.env.CONSUMER_TOKEN});
socket.on('connect', function() { winston.info('Connected to the publisher.'); });

var pg_config = {
    user: process.env.DB_USER,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    max: 10,
    idleTimeoutMillis: 30000
};
var rs_config = {
    user: process.env.RS_USER,
    database: process.env.RS_NAME,
    password: process.env.RS_PASSWORD,
    host: process.env.RS_HOST,
    port: process.env.RS_PORT,
    max: 10,
    idleTimeoutMillis: 30000
};
var rs_pool = new pg.Pool(rs_config);
var pg_pool = new pg.Pool(pg_config);
var map = {};
var type_map = {};
// array of all sensor names whose metadata appears to be incorrect
var blacklist = [];

/**
 * takes in and handles sensor readings
 * updates map and type_map if necessary
 *
 * @param {Object} obs = observation
 * in format:
 * { node_id: "00a",
 *  meta_id: 23,
 *  datetime: "2016-08-05T00:00:08.246000",
 *  sensor: "HTU21D",
 *  data: { temperature: 37.90,
 *            humidity: 27.48 } }
 */
var parse_data = function (obs) {
    // log.info(obs);
    // put network name, all nodes, sensors, and data keys to lower case for internal comparisons
    obs.node_id = obs.node_id.toLowerCase();
    obs.sensor = obs.sensor.toLowerCase();

    Object.keys(obs.data).forEach(function (key) {
        if (key != key.toLowerCase()) {
            obs.data[key.toLowerCase()] = obs.data[key];
            delete obs.data[key];
        }
    });
    // Pulls postgres immediately if sensor is not known or properties are not reported as expected.
    // The Object.keys(type_map).length == 0 catch prevents coerce_types from running with an empty type_map on startup,
    // which would throw TypeErrors
    if (invalid_keys(obs).length > 0 || Object.keys(type_map).length == 0 ||
        (Object.keys(type_map).length > 0 && Object.keys(coerce_types(obs).errors).length > 0)) {
        log.info('discrepancy in map');
        update_map().then(function () {
            update_type_map().then(function () {
                log.info('map updated');
                if (!(obs.sensor in map)) {
                    log.info('sensor not in new map');
                    // this means we don't have the mapping for a sensor and it's not in postgres
                    // send error message to apiary if message not already sent
                    send_error(obs.sensor, 'does_not_exist', {network: obs.network});
                    insert_emit(obs);
                }
                else if (invalid_keys(obs).length > 0 || Object.keys(coerce_types(obs).errors).length > 0) {
                    log.info('invalid keys in new map');
                    // this means there is an unknown or faulty key being sent from beehive
                    // or the types of this observation cannot be correctly coerced
                    // send error message to apiary if message not already sent
                    send_error(obs.sensor, 'invalid_key',
                        {
                            unknown_keys: invalid_keys(obs),
                            coercion_errors: coerce_types(obs).errors,
                            network: obs.network
                        });
                    insert_emit(obs);
                }
                else {
                    log.info('new map fixed everything');
                    // updating the map fixed the discrepancy
                    // send resolve message if sensor in blacklist
                    send_resolve(obs.sensor);
                    insert_emit(obs);
                }
            }, function (err) {
                log.error(err)
            })
        }, function (err) {
            log.error(err)
        })

    }
    else {
        // checks show that the mapping will work to input values into the database - go for it
        insert_emit(obs);
    }
};

/**
 * pulls from postgres to create most up-to-date mapping from each sensor name to its array of properties
 *
 * @return {promise} yields map on fulfillment
 * in format:
 * { tmp112: { temperature: 'temperature.temperature' },
 * bmp340: { temp: 'temperature.temperature', relhum:'humidity.humidity' },
 * ubq120: { x: 'magnetic_field.x', y: 'magnetic_field.x', z: 'magnetic_field.x' },
 * pre450: { atm_pressure: 'atmospheric_pressure.pressure' } }
 *
 * where each key is the expected key from the node and each value is the equivalent FoI.property
 */
function update_map() {
    var p = new promise(function (fulfill, reject) {
        pg_pool.query('SELECT * FROM sensor__sensor_metadata', function (err, result) {
            if (err) {
                reject('error running query in update_map ' + err);
            }
            var new_map = {};
            for (var i = 0; i < result.rows.length; i++) {
                new_map[result.rows[i].name.toLowerCase()] =
                    JSON.parse(JSON.stringify(result.rows[i].observed_properties).toLowerCase());
            }
            map = new_map;
            fulfill();
        });
    });
    return p
}

/**
 * pulls from postgres to create most up-to-date mapping of sensor names to array of properties
 *
 * @return {promise} yields map on fulfillment
 * in format:
 * { temperature: { temperature: 'FLOAT' },
 * magnetic_field: { x: 'FLOAT', y:'FLOAT', z:'FLOAT' } }
 *
 * where each key is the FoI and each value is a dictionary of observed properties and their SQL types
 */
function update_type_map() {
    var p = new promise(function (fulfill, reject) {
        pg_pool.query('SELECT * FROM sensor__feature_metadata', function (err, result) {
            if (err) {
                reject('error running query in update_type_map ' + err);
            }
            var new_type_map = {};
            for (var i = 0; i < result.rows.length; i++) {
                var feature_type_map = {};
                for (var j = 0; j < result.rows[i].observed_properties.length; j++) {
                    feature_type_map[result.rows[i].observed_properties[j].name.toLowerCase()] =
                        result.rows[i].observed_properties[j].type.toLowerCase();
                }
                new_type_map[result.rows[i].name.toLowerCase()] = feature_type_map;
            }
            type_map = new_type_map;
            fulfill();
        });
    });
    return p
}

/**
 * coerces data to correct type to avoid errors inserting into redshift
 *
 * @param {Object} obs = observation
 * @return {Object}
 * { result:
 *     { coerced obs },
 *   errors:
 *   { Temp: true }
 * }
 */
function coerce_types(obs) {
    var errors = {};
    Object.keys(obs.data).forEach(function (key) {
        if (invalid_keys(obs).indexOf(key) < 0) {
            var feature = map[obs.sensor][key].split('.')[0];
            var property = map[obs.sensor][key].split(/\.(.+)?/)[1];
            var type = type_map[feature][property];
            if (type == 'varchar' || type == 'string') {
                obs.data[key] = String(obs.data[key]);
            }
            else if (type == 'integer' || type == 'int') {
                if (isNaN(parseInt(obs.data[key]))) {
                    errors[key] = obs.data[key];
                }
                else {
                    obs.data[key] = parseInt(obs.data[key]);
                }
            }
            else if (type == 'float' || type == 'double' || type == 'double precision') {
                if (isNaN(Number(obs.data[key]))) {
                    errors[key] = obs.data[key];
                }
                else {
                    obs.data[key] = Number(obs.data[key]);
                }
            }
            else if (type == 'bool' || type == 'boolean') {
                if (obs.data[key] == '1' ||
                    (typeof obs.data[key] == 'string' && obs.data[key].toUpperCase() == 'TRUE') ||
                    obs.data[key] == true) {
                    obs.data[key] = true;
                }
                else if (obs.data[key] == '0' ||
                    (typeof obs.data[key] == 'string' && obs.data[key].toUpperCase() == 'FALSE') ||
                    obs.data[key] == false) {
                    obs.data[key] = false;
                }
                else {
                    errors[key] = obs.data[key];
                }
            }
            else {
                errors[key] = obs.data[key];
                log.error(
                    'unrecognized type ' + type +
                    ' of property ' + property +
                    ' of feature ' + feature);
            }
        }
    });
    return {result: obs, errors: errors}
}

/**
 * inserts observation into redshift and emits to socket
 *
 * @param {Object} obs = observation
 */
function insert_emit(obs) {
    if (invalid_keys(obs).length > 0 || Object.keys(coerce_types(obs).errors).length > 0) {
        // split obs into one copy containing all valid keys, one copy containing all invalid keys
        // insert all valid-keyed-values into feature tables, invalid-keyed-values into unknown_feature table
        var misfit_obs = JSON.parse(JSON.stringify(obs));
        var bad_keys = invalid_keys(obs).concat(Object.keys(coerce_types(obs).errors));
        Object.keys(obs.data).forEach(function (key) {
            if (bad_keys.indexOf(key) < 0) {
                delete misfit_obs.data[key]
            }
            else {
                delete obs.data[key]
            }
        });
        rs_pool.query(misfit_query_text(misfit_obs), function (err) {
            if (err) {
                log.error('error inserting data into unknown_feature table ', err)
            }
        });
        if (Object.keys(obs.data).length > 0) {
            insert_emit(obs);
        }
    }
    else {
        obs = coerce_types(obs).result;
        var all_features = [];
        Object.keys(obs.data).forEach(function (key) {
            var feature = map[obs.sensor][key].split('.')[0];
            if (all_features.indexOf(feature) < 0) {
                all_features.push(feature)
            }
        });
        for (var j = 0; j < all_features.length; j++) {
            var feature = all_features[j];
            rs_pool.query(feature_query_text(obs, feature), function (err) {
                if (err) {
                    log.error('error inserting data into ' +
                        obs.network + '__' + feature.toLowerCase() + ' table ', err)
                }
            });
        }
        // emit salvageable data to socket
        var obs_list = format_obs(obs);
        for (var i = 0; i < obs_list.length; i++) {
            // log.info('Sending: ' + obs_list[i]);
            socket.emit('internal_data', obs_list[i]);
        }
    }
}

/**
 * generates query text to insert data into the unknown_feature table
 *
 * @param {Object} obs
 * @return {String} query_text
 */
function misfit_query_text(obs) {
    return util.format("INSERT INTO %s__unknown_feature " +
        "VALUES ('%s', '%s', %s, '%s', '%s');",
        obs.network, obs.node_id, obs.datetime, obs.meta_id, obs.sensor, JSON.stringify(obs.data));
}

/**
 * generates query text to insert data into a given feature of interest table
 *
 * @param {Object} obs
 * @param {String} feature
 * @return {String} query_text
 */
function feature_query_text(obs, feature) {
    // log.info("---------- feature_query_text() ----------");
    // log.info(obs);
    var query_text = util.format("INSERT INTO %s__%s (node_id, datetime, meta_id, sensor, ",
        obs.network, feature.toLowerCase());
    var c = 0;
    Object.keys(obs.data).forEach(function (key) {
        if (map[obs.sensor][key].split('.')[0] == feature) {
            if (c != 0) {
                query_text += ', '
            }
            var property = map[obs.sensor][key].split(/\.(.+)?/)[1];
            // Surrounding the column with "" is needed to specify columns with
            // names that begin with numbers such as "500nm"
            query_text += '"' + property + '"';
            c++;
        }
    });
    query_text = util.format(query_text + ") " +
        "VALUES ('%s', '%s', %s, '%s'",
        obs.node_id.toLowerCase(), obs.datetime, obs.meta_id, obs.sensor.toLowerCase());
    Object.keys(obs.data).forEach(function (key) {
        if (map[obs.sensor][key].split('.')[0] == feature) {
            var property = map[obs.sensor][key].split(/\.(.+)?/)[1];
            var type = type_map[feature][property];
            if (type == 'varchar' || type == 'string') {
                query_text += ', ' + "'" + obs.data[key] + "'";
            }
            else if (type == 'bool' || type == 'boolean') {
                query_text += ', ' + String(obs.data[key]).toUpperCase();
            }
            else {
                query_text += ', ' + obs.data[key];
            }
        }
    });
    query_text += ');';
    // log.info(query_text);
    return query_text
}

/**
 * splits observation into formatted feature of interest output JSON
 *
 * @param {Object} obs = observation
 * @return {Array} array of formatted observation objects
 * in format:
 * [
 *  { node_id: "00A",
 *  datetime: "2016-08-05T00:00:08.246000",
 *  meta_id: 23,
 *  sensor: "HTU21D",
 *  feature: "temperature",
 *  results: { temperature: 37.90 } },
 *
 *  { node_id: "00A",
 *  datetime: "2016-08-05T00:00:08.246000",
 *  meta_id: 23,
 *  sensor: "HTU21D",
 *  feature: "humidity",
 *  results: { humidity: 27.48 } }
 *  ]
 */
function format_obs(obs) {
    // features is simply an array of feature names matching the order of obs_list for easy key finding
    var obs_list = [];
    var features = [];
    Object.keys(obs.data).forEach(function (key) {
        var feature = map[obs.sensor][key].split('.')[0];
        var property = map[obs.sensor][key].split(/\.(.+)?/)[1];
        if (features.indexOf(feature) < 0) {
            obs_list.push({
                feature: feature,
                node: obs.node_id,
                sensor: obs.sensor,
                datetime: obs.datetime,
                network: obs.network,
                results: {}
            });
            features.push(feature)
        }
        obs_list[features.indexOf(feature)].results[property] = obs.data[key];
    });
    return obs_list
}

/**
 * determines if observation can be properly mapped
 *
 * @param {Object} obs
 * @return {Object} {unknown_keys:[...], coercion_errors:{key:{sensor:x, data={...}}, ...}}
 */
function invalid_keys(obs) {
    var keys = [];
    Object.keys(obs.data).forEach(function (key) {
        if (!(obs.sensor in map) || (obs.sensor in map && !(key in map[obs.sensor]))) {
            keys.push(key);
        }
    });
    return keys
}

/**
 * sends error message to apiary if sensor not already in blacklist
 *
 * @param {String} sensor = sensor name
 * @param {String} message_type
 * @param {Object} args = {network: network_name,
 *    if 'invalid_key' --  unknown_keys:[...], coercion_errors:{key:value, ...}}
 */
function send_error(sensor, message_type, args) {
    var message;
    if (message_type == 'does_not_exist') {
        message = [ 'Sensor ' + sensor + ' not found in sensor metadata ' +
            'for network ' + args.network + '. Please add this sensor.' ];
    }
    else if (message_type == 'invalid_key') {
        message = [];
        if (args.unknown_keys && args.unknown_keys.length > 0) {
            message.push('Received data from sensor ' + sensor +
                ' with unknown key(s) ' + args.unknown_keys +
                'from network ' + args.network +
                '. Please update the keys and properties in this sensors metadata.')
        }
        if (args.coercion_errors) {
            Object.keys(args.coercion_errors).forEach(function (key) {
                var feature = map[sensor][key].split('.')[0];
                var property = map[sensor][key].split(/\.(.+)?/)[1];
                message.push('Property ' + property + ' of sensor ' + sensor +
                    ' expected type ' + type_map[feature][property] +
                    ' and could not correctly coerce value ' + args.coercion_errors[key] +
                    ' of type ' + typeof args.coercion_errors[key])
            });
        }
    }

    if (blacklist.indexOf(sensor) < 0) {
        request.post('http://' + process.env.PLENARIO_HOST + '/apiary/send_message',
            {
                json: {
                    name: sensor,
                    value: message
                }
            }, function (err, response, body) {
                if (err) {
                    log.error(err);
                }
            });
        blacklist.push(sensor)
    }
}

/**
 * sends message to apiary communicating that updating the map from postgres resolved previous discrepancies
 *
 * @param {String} sensor = sensor name
 */
function send_resolve(sensor) {
    request.post('http://' + process.env.PLENARIO_HOST + '/apiary/send_message',
        {
            json: {
                name: sensor,
                value: "resolve"
            }
        }, function (err, response, body) {
            if (err) {
                log.error(err);
            }
        });
    blacklist.splice(blacklist.indexOf(sensor), 1)
}

module.exports.parse_data = parse_data;