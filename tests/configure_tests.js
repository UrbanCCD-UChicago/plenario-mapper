/**
 * setup testing with
 * $ node configure_tests.js setup
 *
 * tear down testing with
 * $ node configure_tests.js teardown
 */
var pg = require('pg');
var util = require('util');

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
var rs_pool = new pg.Pool(rs_config);
var pg_pool = new pg.Pool(pg_config);

// insert test metadata into database
if (process.argv[2] == 'setup') {

    // insert test data after clearing metadata tables of possible old test data
    pg_pool.query("DELETE FROM sensor__sensors", function (err) {
        if (err) throw err;
        pg_pool.query("INSERT INTO sensor__sensors VALUES ('htu21d', " +
            "'{\"Humidity\": \"relative_humidity.humidity\", \"Temp\": \"temperature.temperature\"}', '{}')", function (err) {
            if (err) throw err;
        });
        pg_pool.query("INSERT INTO sensor__sensors VALUES ('hmc5883l', " +
            "'{\"X\": \"magnetic_field.x\", \"Y\": \"magnetic_field.y\", \"Z\": \"magnetic_field.z\"}', '{}')", function (err) {
            if (err) throw err;
        });
        pg_pool.query("INSERT INTO sensor__sensors VALUES ('camera', " +
            "'{\"standing_water\": \"computer_vision.standing_water\", \"cloud_type\": \"computer_vision.cloud_type\", " +
            "\"traffic_density\": \"computer_vision.traffic_density\", \"num_pedestrians\": \"computer_vision.num_pedestrians\"}', '{}')", function (err) {
            if (err) throw err;
        });
    });
    pg_pool.query("DELETE FROM sensor__features_of_interest", function (err) {
        if (err) throw err;
        pg_pool.query("INSERT INTO sensor__features_of_interest VALUES ('temperature', " +
            "'[{\"name\": \"temperature\", \"type\": \"FLOAT\"}]')", function (err) {
            if (err) throw err;
        });
        pg_pool.query("INSERT INTO sensor__features_of_interest VALUES ('relative_humidity', " +
            "'[{\"name\": \"humidity\", \"type\": \"FLOAT\"}]')", function (err) {
            if (err) throw err;
        });
        pg_pool.query("INSERT INTO sensor__features_of_interest VALUES ('magnetic_field', " +
            "'[{\"name\": \"x\", \"type\": \"FLOAT\"}, " +
            "{\"name\": \"y\", \"type\": \"FLOAT\"}, " +
            "{\"name\": \"z\", \"type\": \"FLOAT\"}]')", function (err) {
            if (err) throw err;
        });
        pg_pool.query("INSERT INTO sensor__features_of_interest VALUES ('computer_vision', " +
            "'[{\"name\": \"standing_water\", \"type\": \"BOOL\"}, " +
            "{\"name\": \"cloud_type\", \"type\": \"VARCHAR\"}, " +
            "{\"name\": \"traffic_density\", \"type\": \"FLOAT\"}, " +
            "{\"name\": \"num_pedestrians\", \"type\": \"INTEGER\"}]')", function (err) {
            if (err) throw err;
        });
    });

    // create redshift tables and clear them of possible old test data
    rs_pool.query('CREATE TABLE IF NOT EXISTS array_of_things_chicago__temperature (' +
        '"node_id" VARCHAR NOT NULL, ' +
        'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, ' +
        '"meta_id" DOUBLE PRECISION NOT NULL, ' +
        '"sensor" VARCHAR NOT NULL, ' +
        '"temperature" DOUBLE PRECISION, ' +
        'PRIMARY KEY ("node_id", datetime)) ' +
        'DISTKEY(datetime) SORTKEY(datetime);', function (err) {
        if (err) throw err;
        rs_pool.query('DELETE FROM array_of_things_chicago__temperature', function (err) {
            if (err) throw err;
        });
    });
    rs_pool.query('CREATE TABLE IF NOT EXISTS array_of_things_chicago__relative_humidity (' +
        '"node_id" VARCHAR NOT NULL, ' +
        'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, ' +
        '"meta_id" DOUBLE PRECISION NOT NULL, ' +
        '"sensor" VARCHAR NOT NULL, ' +
        '"humidity" DOUBLE PRECISION, ' +
        'PRIMARY KEY ("node_id", datetime)) ' +
        'DISTKEY(datetime) SORTKEY(datetime);', function (err) {
        if (err) throw err;
        rs_pool.query('DELETE FROM array_of_things_chicago__relative_humidity', function (err) {
            if (err) throw err;
        });
    });
    rs_pool.query('CREATE TABLE IF NOT EXISTS array_of_things_chicago__magnetic_field (' +
        '"node_id" VARCHAR NOT NULL, ' +
        'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, ' +
        '"meta_id" DOUBLE PRECISION NOT NULL, ' +
        '"sensor" VARCHAR NOT NULL, ' +
        '"x" DOUBLE PRECISION, ' +
        '"y" DOUBLE PRECISION, ' +
        '"z" DOUBLE PRECISION, ' +
        'PRIMARY KEY ("node_id", datetime)) ' +
        'DISTKEY(datetime) SORTKEY(datetime);', function (err) {
        if (err) throw err;
        rs_pool.query('DELETE FROM array_of_things_chicago__magnetic_field', function (err) {
            if (err) throw err;
        });
    });
    rs_pool.query('CREATE TABLE IF NOT EXISTS array_of_things_chicago__computer_vision (' +
        '"node_id" VARCHAR NOT NULL, ' +
        'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, ' +
        '"meta_id" DOUBLE PRECISION NOT NULL, ' +
        '"sensor" VARCHAR NOT NULL, ' +
        '"standing_water" BOOLEAN, ' +
        '"cloud_type" VARCHAR, ' +
        '"num_pedestrians" INTEGER, ' +
        '"traffic_density" DOUBLE PRECISION, ' +
        'PRIMARY KEY ("node_id", datetime)) ' +
        'DISTKEY(datetime) SORTKEY(datetime);', function (err) {
        if (err) throw err;
        rs_pool.query('DELETE FROM array_of_things_chicago__computer_vision', function (err) {
            if (err) throw err;
        });
    });
    rs_pool.query('CREATE TABLE IF NOT EXISTS array_of_things_chicago__unknown_feature (' +
        '"node_id" VARCHAR NOT NULL, ' +
        'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, ' +
        '"meta_id" DOUBLE PRECISION NOT NULL, ' +
        '"sensor" VARCHAR NOT NULL, ' +
        '"data" VARCHAR, ' +
        'PRIMARY KEY ("node_id", datetime)) ' +
        'DISTKEY(datetime) SORTKEY(datetime);', function (err) {
        if (err) throw err;
        rs_pool.query('DELETE FROM array_of_things_chicago__unknown_feature', function (err) {
            if (err) throw err;
        });
    });
}

else if (process.argv[2] == 'teardown') {

    // clear metadata tables
    pg_pool.query("DELETE FROM sensor__sensors;", function (err) {
        if (err) throw err;
    });
    pg_pool.query("DELETE FROM sensor__features_of_interest", function (err) {
        if (err) throw err;
    });

    // delete redshift tables
    rs_pool.query('DROP TABLE IF EXISTS array_of_things_chicago__temperature;', function (err) {
        if (err) throw err;
    });
    rs_pool.query('DROP TABLE IF EXISTS array_of_things_chicago__relative_humidity;', function (err) {
        if (err) throw err;
    });
    rs_pool.query('DROP TABLE IF EXISTS array_of_things_chicago__magnetic_field;', function (err) {
        if (err) throw err;
    });
    rs_pool.query('DROP TABLE IF EXISTS array_of_things_chicago__computer_vision;', function (err) {
        if (err) throw err;
    });
    rs_pool.query('DROP TABLE IF EXISTS array_of_things_chicago__unknown_feature;', function (err) {
        if (err) throw err;
    });
}
else {
    console.log('invalid argument - must supply either "setup" or "teardown"');
}
