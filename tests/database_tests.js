/**
 * $ npm install nodeunit -g
 *
 * $ nodeunit tests.js
 */
var rewire = require('rewire');
var mapper = rewire('../app/mapper');
var _ = require('underscore');
var pg = require('pg');

var pg_config = {
    user: process.env.DB_USER,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    max: 10,
    idleTimeoutMillis: 1000
};
var rs_config = {
    user: process.env.RS_USER,
    database: process.env.RS_NAME,
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
    mapper.__set__('pg_pool', pg_pool);
    mapper.__get__('update_map')().then(function (map) {
        test.ok(_.isEqual(map,
            {
                htu21d: {
                    Temp: "temperature.temperature",
                    Humidity: "relative_humidity.humidity"
                },
                hmc5883l: {
                    X: "magnetic_field.x",
                    Y: "magnetic_field.y",
                    Z: "magnetic_field.z"
                }
            }));
        test.done();
    }, function (err) {
        throw err;
    });
};

// test update_type_map
exports.update_type_map = function (test) {
    mapper.__set__('pg_pool', pg_pool);
    mapper.__get__('update_type_map')().then(function (map) {
        test.ok(_.isEqual(map,
            {
                temperature: {
                    temperature: 'FLOAT'
                },
                relative_humidity: {
                    humidity: 'FLOAT'
                },
                magnetic_field: {
                    x: 'FLOAT',
                    y: 'FLOAT',
                    z: 'FLOAT'
                }
            }));
        test.done();
    }, function (err) {
        throw err;
    });
};

// test inserting into redshift and emitting to the socket
exports.parse_insert_emit = function (test) {
    mapper.__set__('pg_pool', pg_pool);
    mapper.__set__('rs_pool', rs_pool);
    mapper.__set__('socket', require('socket.io-client')('http://localhost:8081/'));

    mapper.__set__('map', {
        htu21d: {
            Temp: "temperature.temperature",
            Humidity: "relative_humidity.humidity"
        },
        hmc5883l: {
            X: "magnetic_field.x",
            Y: "magnetic_field.y",
            Z: "magnetic_field.z"
        }
    });
    mapper.__set__('type_map', {
        temperature: {
            temperature: 'FLOAT'
        },
        relative_humidity: {
            humidity: 'FLOAT'
        },
        magnetic_field: {
            x: 'FLOAT',
            y: 'FLOAT',
            z: 'FLOAT'
        }
    });

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
    // incoercible X value
    var obs4 = {
        node_id: "004",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "hmc5883l",
        data: {
            X: "high",
            Y: 32.11,
            Z: 90.92
        }
    };
    // everything invalid or incoercible
    var obs5 = {
        node_id: "005",
        meta_id: 23,
        datetime: "2017-01-01T00:00:00",
        sensor: "hmc5883l",
        data: {
            X: "high",
            y1: 32.11,
            z1: 90.92
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

    var http = require('http');
    var express = require('express');
    var app = express();
    var server = http.createServer(app);
    var io = require('socket.io')(server);

    server.listen(8081);

    var data_count = 0;
    io.on('connect', function (socket) {
        // necessary for teardown
        setTimeout(function () {
            socket.disconnect();
        }, 5000);
        socket.on('internal_data', function (data) {
            data_count++;
            if (data.node_id == '001' && data.feature_of_interest == 'temperature') {
                test.ok(_.isEqual(data.results, { temperature: 37.91 }));
            }
            else if (data.node_id == '001' && data.feature_of_interest == 'relative_humidity') {
                test.ok(_.isEqual(data.results, { humidity: 27.48 }));
            }
            else if (data.node_id == '002') {
                test.ok(_.isEqual(data.results, { y: 32.11, z: 90.92 }));
            }
            else if (data.node_id == '003') {
                test.ok(_.isEqual(data.results, { z: 90.92 }));
            }
            else if (data.node_id == '004') {
                test.ok(_.isEqual(data.results, { y: 32.11, z: 90.92 }));
            }
            else {
                test.ok(false);
            }
        })
    });

    var parse_insert_emit = mapper.__get__('parse_insert_emit');
    parse_insert_emit(obs1);
    parse_insert_emit(obs2);
    parse_insert_emit(obs3);
    parse_insert_emit(obs4);
    parse_insert_emit(obs5);
    parse_insert_emit(obs6);

    setTimeout(function () {
        test.equals(data_count, 5);
    }, 3000);

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
            test.equals(result.rows[0].data, JSON.stringify({x1: 56.77, y1: 32.11}));
        });

        rs_pool.query("SELECT * FROM magnetic_field WHERE node_id = '004';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].x, null);
            test.equals(result.rows[0].y, 32.11);
            test.equals(result.rows[0].z, 90.92);
        });
        rs_pool.query("SELECT * FROM unknown_feature WHERE node_id = '004';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].data, JSON.stringify({X: "high"}));
        });

        rs_pool.query("SELECT * FROM unknown_feature WHERE node_id = '005';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].data, JSON.stringify({X: "high", y1: 32.11, z1: 90.92}));
        });

        rs_pool.query("SELECT * FROM unknown_feature WHERE node_id = '006';", function (err, result) {
            if (err) throw err;
            test.equals(result.rows[0].data, JSON.stringify({intensity: 90}));
            test.equals(result.rows[0].sensor, "wubdb89");
        });
    }, 3000);

    // tear down so that test doesn't run forever
    setTimeout(function () {
        server.close();
        test.done();
    }, 10000);
};

