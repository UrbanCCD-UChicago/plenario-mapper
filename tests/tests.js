/**
 * $ npm install nodeunit -g
 *
 * $ nodeunit tests.js
 */
var rewire = require('rewire');
var mapper = rewire('../app/mapper');
var _ = require('underscore');

mapper.__set__('map',
    {
        HTU21D: {
            Temp: "temperature.temperature",
            Humidity: "relative_humidity.humidity"
        },
        HMC5883L: {
            X: "magnetic_field.x",
            Y: "magnetic_field.y",
            Z: "magnetic_field.z"
        },
        camera: {
            standing_water: "cv.standing_water",
            cloud_type: "cv.cloud_type",
            num_pedestrians: "cv.num_pedestrians",
            traffic_density: "cv.traffic_density"
        }
    });

mapper.__set__('type_map',
    {
        cv: {
            standing_water: 'BOOL',
            cloud_type: 'VARCHAR',
            num_pedestrians: 'INTEGER',
            traffic_density: 'FLOAT'
        }
    });

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

    test.equal(mapper.__get__('misfit_query_text')(obs), "INSERT INTO unknown_feature " +
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

    test.equal(mapper.__get__('feature_query_text')(obs1, 'temperature'), "INSERT INTO temperature " +
        "(node_id, datetime, meta_id, sensor, temperature) " +
        "VALUES ('00A', '2016-08-05T00:00:08.246000', 23, 'HTU21D', 37.91);");
    test.equal(mapper.__get__('feature_query_text')(obs1, 'relative_humidity'), "INSERT INTO relative_humidity " +
        "(node_id, datetime, meta_id, sensor, humidity) " +
        "VALUES ('00A', '2016-08-05T00:00:08.246000', 23, 'HTU21D', 27.48);");
    test.equal(mapper.__get__('feature_query_text')(obs2, 'magnetic_field'), "INSERT INTO magnetic_field " +
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

    test.ok(_.isEqual(mapper.__get__('format_obs')(obs1), [
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
    test.ok(_.isEqual(mapper.__get__('format_obs')(obs2), [
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

// test type coercion
exports.coerce_types = function (test) {
    var obs1 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "camera",
        data: {
            standing_water: 1,
            cloud_type: 1,
            num_pedestrians: 1,
            traffic_density: 1
        }
    };

    var obs2 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "camera",
        data: {
            standing_water: "0",
            cloud_type: "0",
            num_pedestrians: "0",
            traffic_density: "0"
        }
    };

    var obs3 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "camera",
        data: {
            standing_water: "true",
            cloud_type: "true",
            num_pedestrians: "true",
            traffic_density: "true"
        }
    };

    var obs4 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "camera",
        data: {
            standing_water: false,
            cloud_type: false,
            num_pedestrians: false,
            traffic_density: false
        }
    };

    var obs5 = {
        node_id: "00A",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "camera",
        data: {
            standing_water: 10,
            cloud_type: 10,
            num_pedestrians: 10,
            traffic_density: 10
        }
    };

    test.ok(_.isEqual(mapper.__get__('coerce_types')(obs1), {
        result: {
            node_id: "00A",
            meta_id: 23,
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "camera",
            data: {
                standing_water: true,
                cloud_type: "1",
                num_pedestrians: 1,
                traffic_density: 1
            }
        }, errors: {}
    }));

    test.ok(_.isEqual(mapper.__get__('coerce_types')(obs2), {
        result: {
            node_id: "00A",
            meta_id: 23,
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "camera",
            data: {
                standing_water: false,
                cloud_type: "0",
                num_pedestrians: 0,
                traffic_density: 0
            }
        }, errors: {}
    }));
    
    test.ok(_.isEqual(mapper.__get__('coerce_types')(obs3), {
        result: {
            node_id: "00A",
            meta_id: 23,
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "camera",
            data: {
                standing_water: true,
                cloud_type: "true",
                num_pedestrians: "true",
                traffic_density: "true"
            }
        }, errors: {
            num_pedestrians: {
                sensor: "camera", value: "true"
            },
            traffic_density: {
                sensor: "camera", value: "true"
            }
        }
    }));

    test.ok(_.isEqual(mapper.__get__('coerce_types')(obs4), {
        result: {
            node_id: "00A",
            meta_id: 23,
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "camera",
            data: {
                standing_water: false,
                cloud_type: "false",
                num_pedestrians: false,
                traffic_density: 0
            }
        }, errors: {
            num_pedestrians: {
                sensor: "camera", value: false
            }
        }
    }));

    test.ok(_.isEqual(mapper.__get__('coerce_types')(obs5), {
        result: {
            node_id: "00A",
            meta_id: 23,
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "camera",
            data: {
                standing_water: 10,
                cloud_type: "10",
                num_pedestrians: 10,
                traffic_density: 10
            }
        }, errors: {
            standing_water: {
                sensor: "camera", value: 10
            }
        }
    }));

    test.done();
};