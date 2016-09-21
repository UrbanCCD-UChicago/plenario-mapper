'use strict';

// satisfies Promise needs of pg-pool running underneath pg
global.Promise = require('promise');

var util = require('util');
var kcl = require('../');
var logger = require('../util/logger');
var mapper = require('../mapper');

/**
 * Be careful not to use the 'stderr'/'stdout'/'console' as log destination since it is used to communicate with the
 * {https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/package-info.java MultiLangDaemon}.
 */

function recordProcessor() {
    var log = logger().getLogger('recordProcessor');
    var shardId;

    return {

        initialize: function (initializeInput, completeCallback) {
            // log.info('In initialize');
            shardId = initializeInput.shardId;
            completeCallback();
        },

        processRecords: function (processRecordsInput, completeCallback) {
            // log.info('In processRecords');
            if (!processRecordsInput || !processRecordsInput.records) {
                completeCallback();
                return;
            }
            var records = processRecordsInput.records;
            var record, data, sequenceNumber, partitionKey;
            for (var i = 0; i < records.length; ++i) {
                record = records[i];
                data = new Buffer(record.data, 'base64').toString();
                sequenceNumber = record.sequenceNumber;
                partitionKey = record.partitionKey;
                // assumes a stringified JSON is being read from the stream
                // will catch and log malformed JSON
                try {
                    mapper.parse_insert_emit(JSON.parse(data));
                }
                catch (err) {
                    log.error(err)
                }
                // log.info(util.format('ShardID: %s, Record: %s, SeqenceNumber: %s, PartitionKey:%s', shardId, data, sequenceNumber, partitionKey));
            }
            if (!sequenceNumber) {
                completeCallback();
                return;
            }
            // If checkpointing, completeCallback should only be called once checkpoint is complete.
            processRecordsInput.checkpointer.checkpoint(sequenceNumber, function (err, sequenceNumber) {
                // log.info(util.format('Checkpoint successful. ShardID: %s, SeqenceNumber: %s', shardId, sequenceNumber));
                completeCallback();
            });
        },

        shutdown: function (shutdownInput, completeCallback) {
            // Checkpoint should only be performed when shutdown reason is TERMINATE.
            if (shutdownInput.reason !== 'TERMINATE') {
                completeCallback();
                return;
            }
            // Whenever checkpointing, completeCallback should only be invoked once checkpoint is complete.
            shutdownInput.checkpointer.checkpoint(function (err) {
                completeCallback();
            });
        }
    };
}

kcl(recordProcessor()).run();