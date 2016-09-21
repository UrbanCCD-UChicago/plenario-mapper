var pg = require('pg');
var util = require('util');

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
var rs_pool = new pg.Pool(rs_config);
var pg_pool = new pg.Pool(pg_config);

// insert test metadata into database
if (process.argv[2] == 'setup') {

    // insert sensor metadata
    pg_pool.connect(function (err, pg_client, done) {
        if (err) error = err;
        pg_client.query("INSERT INTO sensor__sensors VALUES ('htu21d', " +
            "'{\"Humidity\": \"relative_humidity.humidity\", \"Temp\": \"temperature.temperature\"}', '{}')", function (err) {
            if (err) error = err;
            pg_client.query("INSERT INTO sensor__sensors VALUES ('hmc5883l', " +
                "'{\"X\": \"magnetic_field.x\", \"Y\": \"magnetic_field.y\", \"Z\": \"magnetic_field.z\"}', '{}')", function (err) {
                if (err) throw err;
                done();
            });
        });
    });

    // insert feature_of_interest metadata
    pg_pool.connect(function (err, pg_client, done) {
        pg_client.query("INSERT INTO sensor__features_of_interest VALUES ('temperature', " +
            "'[{\"name\": \"temperature\", \"type\": \"FLOAT\"}]')", function (err) {
            if (err) throw err;
            pg_client.query("INSERT INTO sensor__features_of_interest VALUES ('relative_humidity', " +
                "'[{\"name\": \"humidity\", \"type\": \"FLOAT\"}]')", function (err) {
                if (err) throw err;
                pg_client.query("INSERT INTO sensor__features_of_interest VALUES ('magnetic_field', " +
                    "'[{\"name\": \"x\", \"type\": \"FLOAT\"}, " +
                    "{\"name\": \"y\", \"type\": \"FLOAT\"}, " +
                    "{\"name\": \"z\", \"type\": \"FLOAT\"}]')", function (err) {
                    if (err) throw err;
                    done();
                });
            });
        });
    });

    // create redshift tables
    rs_pool.connect(function (err, rs_client, done) {
        if (err) throw err;
        rs_client.query('CREATE TABLE temperature (' +
            '"node_id" VARCHAR NOT NULL, ' +
            'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, ' +
            '"meta_id" DOUBLE PRECISION NOT NULL, ' +
            '"sensor" VARCHAR NOT NULL, ' +
            '"temperature" DOUBLE PRECISION, ' +
            'PRIMARY KEY ("node_id", datetime)) ' +
            'DISTKEY(datetime) SORTKEY(datetime);', function (err) {
            if (err) throw err;
            rs_client.query('CREATE TABLE relative_humidity (' +
                '"node_id" VARCHAR NOT NULL, ' +
                'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, ' +
                '"meta_id" DOUBLE PRECISION NOT NULL, ' +
                '"sensor" VARCHAR NOT NULL, ' +
                '"humidity" DOUBLE PRECISION, ' +
                'PRIMARY KEY ("node_id", datetime)) ' +
                'DISTKEY(datetime) SORTKEY(datetime);', function (err) {
                if (err) throw err;
                rs_client.query('CREATE TABLE magnetic_field (' +
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
                    rs_client.query('CREATE TABLE unknown_feature (' +
                        '"node_id" VARCHAR NOT NULL, ' +
                        'datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL, ' +
                        '"meta_id" DOUBLE PRECISION NOT NULL, ' +
                        '"sensor" VARCHAR NOT NULL, ' +
                        '"data" VARCHAR, ' +
                        'PRIMARY KEY ("node_id", datetime)) ' +
                        'DISTKEY(datetime) SORTKEY(datetime);', function (err) {
                        if (err) throw err;
                        done();
                    });
                });
            });
        });
    });
}
else if (process.argv[2] == 'teardown') {

    // clear metadata tables
    pg_pool.connect(function (err, pg_client, done) {
        if (err) error = err;
        pg_client.query("DELETE FROM sensor__sensors;", function (err) {
            if (err) error = err;
            pg_client.query("DELETE FROM sensor__features_of_interest", function (err) {
                if (err) throw err;
                done();
            });
        });
    });

    // delete redshift tables
    rs_pool.connect(function (err, rs_client, done) {
        if (err) throw err;
        rs_client.query('DROP TABLE temperature;', function (err) {
            if (err) throw err;
            rs_client.query('DROP TABLE relative_humidity;', function (err) {
                if (err) throw err;
                rs_client.query('DROP TABLE magnetic_field;', function (err) {
                    if (err) throw err;
                    rs_client.query('DROP TABLE unknown_feature;', function (err) {
                        if (err) throw err;
                        done();
                    });
                });
            });
        });
    });
}
else {
    throw 'no arguments given';
}
