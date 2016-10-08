/**
 * to run these tests:
 * 
 * $ npm install nodeunit -g
 *
 * $ node ../configure_tests.js setup
 * $ nodeunit integration_tests.js
 * $ node ../configure_tests.js teardown
 */
var rewire = require('rewire');
var mapper = rewire('../../app/mapper');
var _ = require('underscore');
var pg = require('pg');

var pg_config = {
    user: process.env.DB_USER,
    database: 'sensor_test',
    password: process.env.DB_PASSWORD,
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    max: 10,
    idleTimeoutMillis: 1000
};
var rs_config = {
    user: process.env.RS_USER,
    database: 'sensor_test',
    password: process.env.RS_PASSWORD,
    host: process.env.RS_HOST,
    port: process.env.RS_PORT,
    max: 10,
    idleTimeoutMillis: 1000
};
var pg_pool = new pg.Pool(pg_config);
var rs_pool = new pg.Pool(rs_config);


// test update_map
exports.update_map = function (test) {
    mapper.__set__('map', {});
    mapper.__set__('pg_pool', pg_pool);
    mapper.__get__('update_map')().then(function () {
        test.ok(_.isEqual(mapper.__get__('map'),
            {
                htu21d: {
                    temp: "temperature.temperature",
                    humidity: "relative_humidity.humidity"
                },
                hmc5883l: {
                    x: "magnetic_field.x",
                    y: "magnetic_field.y",
                    z: "magnetic_field.z"
                },
                camera: {
                    standing_water: "computer_vision.standing_water",
                    cloud_type: "computer_vision.cloud_type",
                    num_pedestrians: "computer_vision.num_pedestrians",
                    traffic_density: "computer_vision.traffic_density"
                }
            }));
        test.done();
    }, function (err) {
        throw err;
    });
};

// test update_type_map
exports.update_type_map = function (test) {
    mapper.__set__('type_map', {});
    mapper.__set__('pg_pool', pg_pool);
    mapper.__get__('update_type_map')().then(function () {
        test.ok(_.isEqual(mapper.__get__('type_map'),
            {
                temperature: {
                    temperature: 'float'
                },
                relative_humidity: {
                    humidity: 'float'
                },
                magnetic_field: {
                    x: 'float',
                    y: 'float',
                    z: 'float'
                },
                computer_vision: {
                    standing_water: 'bool',
                    cloud_type: 'varchar',
                    num_pedestrians: 'integer',
                    traffic_density: 'float'
                }
            }));
        test.done();
    }, function (err) {
        throw err;
    });
};

// test parsing data, inserting into redshift, emitting to the socket, and sending errors to apiary
// the whole shabang - the complete rigmarole - tip to tail - soup to nuts
exports.parse_data = function (test) {
    mapper.__set__('map', {});
    mapper.__set__('type_map', {});
    mapper.__set__('pg_pool', pg_pool);
    mapper.__set__('rs_pool', rs_pool);

    // all valid keys
    var obs1 = {
        node_id: "001",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "htu21d",
        data: {
            Temp: 37.91,
            Humidity: 27.48
        }
    };
    // all valid keys, partial observation
    var obs2 = {
        node_id: "002",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "hmc5883l",
        data: {
            Y: 32.11,
            Z: 90.92
        }
    };
    // invalid keys x1 and y1
    var obs3 = {
        node_id: "003",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "hmc5883l",
        data: {
            x1: 56.77,
            y1: 32.11,
            Z: 90.92
        }
    };
    // incoercible standing_water value
    var obs4 = {
        node_id: "004",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "camera",
        data: {
            standing_water: 10,
            cloud_type: "cumulonimbus",
            num_pedestrians: 9,
            traffic_density: .38
        }
    };
    // everything invalid or incoercible
    var obs5 = {
        node_id: "005",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "htu21d",
        data: {
            Temp: "high",
            Humdrum: 27.48
        }
    };
    // unknown sensor
    var obs6 = {
        node_id: "006",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "wubdb89",
        data: {
            intensity: 90
        }
    };
    // all types, everything valid
    var obs7 = {
        node_id: "007",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "camera",
        data: {
            standing_water: true,
            cloud_type: "cumulonimbus",
            num_pedestrians: 11,
            traffic_density: .22
        }
    };

    var http = require('http');
    var express = require('express');
    var app = express();
    var bodyParser = require('body-parser');
    app.use(bodyParser.json());

    // mapper will send post requests to 8080, mock apiary server will listen for requests on 8080
    var apiary_server = http.createServer(app);
    apiary_server.listen(8080);
    process.env['PLENARIO_HOST'] = 'localhost:8080';

    // mock socket server listens on 8081
    var socket_server = http.createServer(app);
    socket_server.listen(8081);
    mapper.__set__('socket', require('socket.io-client')('http://localhost:8081/'));

    var error_count = 0;
    var resolve_count = 0;
    app.post('/apiary/send_message', function (req, res) {
        if (req.body.name == 'htu21d') {
            if (req.body.value == 'resolve') {
                resolve_count++;
            }
            else {
                test.equals(req.body.value.length, 2);
                error_count += 2;
            }
        }
        else if (req.body.name == 'hmc5883l') {
            if (req.body.value == 'resolve') {
                resolve_count++;
            }
            else {
                test.equals(req.body.value.length, 1);
                test.ok(req.body.value[0].includes('unknown key'));
                error_count++;
            }
        }
        else if (req.body.name == 'wubdb89') {
            if (req.body.value == 'resolve') {
                test.ok(false);
            }
            else {
                test.equals(req.body.value.length, 1);
                test.ok(req.body.value[0].includes('not found'));
                error_count++;
            }
        }
        else if (req.body.name == 'camera') {
            if (req.body.value == 'resolve') {
                resolve_count++;
            }
            else {
                test.equals(req.body.value.length, 1);
                test.ok(req.body.value[0].includes('could not correctly coerce'));
                error_count++;
            }
        }
        else {
            test.ok(false);
        }
    });

    var io = require('socket.io')(socket_server);
    var data_count = 0;
    io.on('connect', function (socket) {
        // necessary for cleanup so test doesn't run forever
        setTimeout(function () {
            socket.disconnect();
        }, 9000);
        socket.on('internal_data', function (data) {
            data_count++;
            if (data.node_id == '001' && data.feature == 'temperature') {
                test.ok(_.isEqual(data.results, { temperature: 37.91 }));
            }
            else if (data.node_id == '001' && data.feature == 'relative_humidity') {
                test.ok(_.isEqual(data.results, { humidity: 27.48 }));
            }
            else if (data.node_id == '002') {
                test.ok(_.isEqual(data.results, { y: 32.11, z: 90.92 }));
            }
            else if (data.node_id == '003') {
                test.ok(_.isEqual(data.results, { z: 90.92 }));
            }
            else if (data.node_id == '004') {
                test.ok(_.isEqual(data.results, {
                    cloud_type: "cumulonimbus",
                    num_pedestrians: 9,
                    traffic_density: .38
                }));
            }
            else if (data.node_id == '007') {
                test.ok(_.isEqual(data.results, {
                    standing_water: true,
                    cloud_type: "cumulonimbus",
                    num_pedestrians: 11,
                    traffic_density: .22
                }));
            }
            else {
                test.ok(false);
            }
        })
    });

    var parse_data = mapper.__get__('parse_data');
    parse_data(obs1);
    parse_data(obs2);
    parse_data(obs3);
    parse_data(obs4);
    parse_data(obs5);
    parse_data(obs6);
    parse_data(obs7);

    setTimeout(function () {
        test.equals(data_count, 6);
        test.equals(resolve_count, 3);
        test.equals(error_count, 5);
    }, 8000);

    setTimeout(function () {
        rs_pool.query("SELECT * FROM temperature WHERE node_id = '001';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].temperature, 37.91);
        });
        rs_pool.query("SELECT * FROM relative_humidity WHERE node_id = '001';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].humidity, 27.48);
        });

        rs_pool.query("SELECT * FROM magnetic_field WHERE node_id = '002';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].x, null);
            test.equals(result.rows[0].y, 32.11);
            test.equals(result.rows[0].z, 90.92);
        });

        rs_pool.query("SELECT * FROM magnetic_field WHERE node_id = '003';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].x, null);
            test.equals(result.rows[0].y, null);
            test.equals(result.rows[0].z, 90.92);
        });
        rs_pool.query("SELECT * FROM unknown_feature WHERE node_id = '003';", function (err, result) {
            if (err) throw err;
            test.ok(_.isEqual(JSON.parse(result.rows[0].data), {x1: 56.77, y1: 32.11}));
        });

        rs_pool.query("SELECT * FROM computer_vision WHERE node_id = '004';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].standing_water, null);
            test.equals(result.rows[0].cloud_type, 'cumulonimbus');
            test.equals(result.rows[0].num_pedestrians, 9);
            test.equals(result.rows[0].traffic_density, .38);
        });
        rs_pool.query("SELECT * FROM unknown_feature WHERE node_id = '004';", function (err, result) {
            if (err) throw err;
            test.ok(_.isEqual(JSON.parse(result.rows[0].data), {standing_water: 10}));
        });

        rs_pool.query("SELECT * FROM unknown_feature WHERE node_id = '005';", function (err, result) {
            if (err) throw err;
            test.ok(_.isEqual(JSON.parse(result.rows[0].data), {temp: "high", humdrum: 27.48}));
        });

        rs_pool.query("SELECT * FROM unknown_feature WHERE node_id = '006';", function (err, result) {
            if (err) throw err;
            test.ok(_.isEqual(JSON.parse(result.rows[0].data), {intensity: 90}));
            test.equals(result.rows[0].sensor, "wubdb89");
        });

        rs_pool.query("SELECT * FROM computer_vision WHERE node_id = '007';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].standing_water, true);
            test.equals(result.rows[0].cloud_type, 'cumulonimbus');
            test.equals(result.rows[0].num_pedestrians, 11);
            test.equals(result.rows[0].traffic_density, .22);
        });
    }, 8000);

    // end the test
    setTimeout(function () {
        test.done();
    }, 10000);
};

