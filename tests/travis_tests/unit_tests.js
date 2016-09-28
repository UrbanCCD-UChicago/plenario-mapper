/**
 * to run these tests:
 *
 * $ npm install nodeunit -g
 *
 * $ nodeunit unit_tests.js
 */
var rewire = require('rewire');
var mapper = rewire('../../app/mapper');
var _ = require('underscore');

mapper.__set__('map',
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
    });

mapper.__set__('type_map',
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
    });

// test SQL query text generation to insert into unknown_feature table
exports.misfit_query_text = function (test) {
    var obs = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "htu21d",
        data: {
            temp: 37.91,
            humidity: 27.48
        }
    };

    test.equal(mapper.__get__('misfit_query_text')(obs), "INSERT INTO unknown_feature " +
        "VALUES ('00a', '2016-08-05T00:00:08.246000', 23, 'htu21d', '{\"temp\":37.91,\"humidity\":27.48}');");
    test.done();
};

// test SQL query text generation to insert into a single or multiple feature of interest tables
exports.feature_query_text = function (test) {
    var obs1 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "htu21d",
        data: {
            temp: 37.91,
            humidity: 27.48
        }
    };
    var obs2 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "hmc5883l",
        data: {
            x: 56.77,
            y: 32.11,
            z: 90.92
        }
    };
    var obs3 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "hmc5883l",
        data: {
            y: 32.11,
            z: 90.92
        }
    };
    var obs4 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "camera",
        data: {
            standing_water: true,
            cloud_type: "cumulonimbus",
            num_pedestrians: 13,
            traffic_density: .44
        }
    };

    // split features
    test.equal(mapper.__get__('feature_query_text')(obs1, 'temperature'), "INSERT INTO temperature " +
        "(node_id, datetime, meta_id, sensor, temperature) " +
        "VALUES ('00a', '2016-08-05T00:00:08.246000', 23, 'htu21d', 37.91);");
    test.equal(mapper.__get__('feature_query_text')(obs1, 'relative_humidity'), "INSERT INTO relative_humidity " +
        "(node_id, datetime, meta_id, sensor, humidity) " +
        "VALUES ('00a', '2016-08-05T00:00:08.246000', 23, 'htu21d', 27.48);");
    // full obs
    test.equal(mapper.__get__('feature_query_text')(obs2, 'magnetic_field'), "INSERT INTO magnetic_field " +
        "(node_id, datetime, meta_id, sensor, x, y, z) " +
        "VALUES ('00a', '2016-08-05T00:00:08.246000', 23, 'hmc5883l', 56.77, 32.11, 90.92);");
    // partial obs
    test.equal(mapper.__get__('feature_query_text')(obs3, 'magnetic_field'), "INSERT INTO magnetic_field " +
        "(node_id, datetime, meta_id, sensor, y, z) " +
        "VALUES ('00a', '2016-08-05T00:00:08.246000', 23, 'hmc5883l', 32.11, 90.92);");
    test.equal(mapper.__get__('feature_query_text')(obs4, 'computer_vision'), "INSERT INTO computer_vision " +
        "(node_id, datetime, meta_id, sensor, standing_water, cloud_type, num_pedestrians, traffic_density) " +
        "VALUES ('00a', '2016-08-05T00:00:08.246000', 23, 'camera', TRUE, 'cumulonimbus', 13, 0.44);");
    test.done();
};

// test splitting and formatting of observations
exports.format_obs = function (test) {
    var obs1 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "htu21d",
        data: {
            temp: 37.91,
            humidity: 27.48
        }
    };
    var obs2 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "hmc5883l",
        data: {
            x: 56.77,
            y: 32.11,
            z: 90.92
        }
    };
    var obs3 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "camera",
        data: {
            standing_water: true,
            cloud_type: "cumulonimbus",
            num_pedestrians: 13,
            traffic_density: .44
        }
    };

    test.ok(_.isEqual(mapper.__get__('format_obs')(obs1), [
        {
            node_id: "00a",
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "htu21d",
            feature_of_interest: "temperature",
            results: {
                temperature: 37.91
            }
        },
        {
            node_id: "00a",
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "htu21d",
            feature_of_interest: "relative_humidity",
            results: {
                humidity: 27.48
            }
        }
    ]));
    test.ok(_.isEqual(mapper.__get__('format_obs')(obs2), [
        {
            node_id: "00a",
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "hmc5883l",
            feature_of_interest: "magnetic_field",
            results: {
                x: 56.77,
                y: 32.11,
                z: 90.92
            }
        }
    ]));
    test.ok(_.isEqual(mapper.__get__('format_obs')(obs3), [
        {
            node_id: "00a",
            datetime: "2016-08-05T00:00:08.246000",
            sensor: "camera",
            feature_of_interest: "computer_vision",
            results: {
                standing_water: true,
                cloud_type: "cumulonimbus",
                num_pedestrians: 13,
                traffic_density: .44
            }
        }
    ]));
    test.done();
};

// test type coercion
exports.coerce_types = function (test) {
    var obs1 = {
        node_id: "00a",
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
        node_id: "00a",
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
        node_id: "00a",
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
        node_id: "00a",
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
        node_id: "00a",
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
            node_id: "00a",
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
            node_id: "00a",
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
            node_id: "00a",
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
            num_pedestrians: "true",
            traffic_density: "true"
        }
    }));

    test.ok(_.isEqual(mapper.__get__('coerce_types')(obs4), {
        result: {
            node_id: "00a",
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
            num_pedestrians: false
        }
    }));

    test.ok(_.isEqual(mapper.__get__('coerce_types')(obs5), {
        result: {
            node_id: "00a",
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
            standing_water: 10
        }
    }));
    test.done();
};

// test invalid_keys
exports.invalid_keys = function (test) {
    // all valid keys
    var obs1 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "htu21d",
        data: {
            temp: 37.91,
            humidity: 27.48
        }
    };
    // invalid keys x1 and y1
    var obs2 = {
        node_id: "00a",
        meta_id: 23,
        datetime: "2016-08-05T00:00:08.246000",
        sensor: "hmc5883l",
        data: {
            x1: 56.77,
            y1: 32.11,
            z: 90.92
        }
    };

    test.ok(_.isEqual(mapper.__get__('invalid_keys')(obs1), []));
    test.ok(_.isEqual(mapper.__get__('invalid_keys')(obs2), ['x1', 'y1']));
    test.done();
};