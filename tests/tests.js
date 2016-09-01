/**
 * $ npm install nodeunit -g
 *
 * $ nodeunit tests.js
 */

var mapper = require('../EB_app/mapper');
var _ = require('underscore');

// test SQL query text generation to insert into unknown_feature table
exports.misfit_query_text = function (test) {
    var obs = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "HTU21D",
        data: {
            Temp: 37.91,
            Humidity: 27.48
        }
    };

    test.equal(mapper.misfit_query_text(obs), "INSERT INTO unknown_feature " +
        "VALUES ('00A', '2016-08-05T00:00:08.246000', 23, 'HTU21D', '{\"Temp\":37.91,\"Humidity\":27.48}');");
    test.done();
};

// test SQL query text generation to insert into a single or multiple feature of interest tables
exports.feature_query_text = function (test) {
    var obs1 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "HTU21D",
        data: {
            Temp: 37.91,
            Humidity: 27.48
        }
    };
    var obs2 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "HMC5883L",
        data: {
            X: 56.77,
            Y: 32.11,
            Z: 90.92
        }
    };
    var map = {
        HTU21D: {
            Temp: "temperature.temperature",
            Humidity: "relative_humidity.humidity"
        },
        HMC5883L: {
            X: "magnetic_field.x",
            Y: "magnetic_field.y",
            Z: "magnetic_field.z"
        }
    };

    test.equal(mapper.feature_query_text(obs1, map, 'temperature'), "INSERT INTO temperature " +
        "(node_id, datetime, meta_id, sensor, temperature) " +
        "VALUES ('00A', '2016-08-05T00:00:08.246000', 23, 'HTU21D', 37.91);");
    test.equal(mapper.feature_query_text(obs1, map, 'relative_humidity'), "INSERT INTO relative_humidity " +
        "(node_id, datetime, meta_id, sensor, humidity) " +
        "VALUES ('00A', '2016-08-05T00:00:08.246000', 23, 'HTU21D', 27.48);");
    test.equal(mapper.feature_query_text(obs2, map, 'magnetic_field'), "INSERT INTO magnetic_field " +
        "(node_id, datetime, meta_id, sensor, x, y, z) " +
        "VALUES ('00A', '2016-08-05T00:00:08.246000', 23, 'HMC5883L', 56.77, 32.11, 90.92);");
    test.done();
};

// test splitting and formatting of observations
exports.format_obs = function (test) {
    var obs1 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "HTU21D",
        data: {
            Temp: 37.91,
            Humidity: 27.48
        }
    };
    var obs2 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "HMC5883L",
        data: {
            X: 56.77,
            Y: 32.11,
            Z: 90.92
        }
    };
    var map = {
        HTU21D: {
            Temp: "temperature.temperature",
            Humidity: "relative_humidity.humidity"
        },
        HMC5883L: {
            X: "magnetic_field.x",
            Y: "magnetic_field.y",
            Z: "magnetic_field.z"
        }
    };

    test.ok(_.isEqual(mapper.format_obs(obs1, map), [
        {
            node_id: "00A",
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "HTU21D",
            feature_of_interest: "temperature",
            results: {
                temperature: 37.91
            }
        },
        {
            node_id: "00A",
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "HTU21D",
            feature_of_interest: "relative_humidity",
            results: {
                humidity: 27.48
            }
        }
    ]));
    test.ok(_.isEqual(mapper.format_obs(obs2, map), [
        {
            node_id: "00A",
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "HMC5883L",
            feature_of_interest: "magnetic_field",
            results: {
                x: 56.77,
                y: 32.11,
                z: 90.92
            }
        }
    ]));
    test.done();
};