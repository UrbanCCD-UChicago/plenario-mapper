/**
 * $ npm install nodeunit -g
 *
 * $ nodeunit tests.js
 */
var rewire = require('rewire');
var mapper = rewire('../app/mapper');
var _ = require('underscore');

// test update_map
exports.update_map = function (test) {
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
        test.done()
    }, function (err) {
        console.log(err);
        test.ok(false);
        test.done()
    });
};

// test update_type_map
exports.update_type_map = function (test) {
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
        test.done()
    }, function (err) {
        console.log(err);
        test.ok(false);
        test.done()
    });
};

