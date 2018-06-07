'use strict';

const uuidv4 = require('uuid/v4');
const AWS = require('aws-sdk');
const log = require('./log');

/**
 *
 * @param {string} actionName - Name of the action the event is associated with, eg., 'UnTarArchive'
 * @param {string} eventType - Type of the even - either 'start' or 'end'
 * @param {string} transactionId - The unique identifier of the larger transactions with which
 *                                  this event is associated
 * @param {string} dataType - The data type as given by the PDR entry
 * @param {string} actionId - The UUID associated with a particular instance of an action
 * @param {string} streamName - The stream to which the event should be sent
 * @param {string} region - The AWS region in which the stream lives
 */
const sendEvent = async (actionName, eventType, transactionId, dataType, actionId, streamName, region) => {
  const data = {
    'transaction-id': transactionId,
    'data-type': dataType,
    'time-stamp': Date.now(),
    type: eventType,
    name: actionName,
    'action-transaction-id': actionId
  };

  const json = JSON.stringify(data);
  const buffer = Buffer.from(json);

  const params = {
    Data: buffer,
    PartitionKey: 'p1',
    StreamName: streamName
  };

  const kinesis = new AWS.Kinesis({ region: region });

  kinesis.putRecord(params, (err, _data) => {
    if (err) console.log(err, err.stack);
  });
};

/**
 * Send a 'start' event to the event stream
 *
 * @param {object} config An object containing the
 * @param {string} actionName The name of the action
 * @param {string} actionId The optional id to use to match start/end events. Will be generated
 * if not provided
 */
exports.sendStart = (config, actionName, actionId = null) => {
  const aid = actionId || uuidv4();
  const transactionId = config.transactionId || global.transactionId;
  const dataType = global.dataType || 'N/A';
  const streamName = config.profiling_stream_name || 'gitc-performance-test';
  const region = config.profiling_region || 'us-east-1';

  sendEvent(actionName, 'start', transactionId, dataType, aid, streamName, region);

  return aid;
};

exports.sendEnd = (config, actionName, actionId) => {
  const transactionId = config.transactionId || global.transactionId;
  const dataType = global.dataType || 'N/A';
  const streamName = config.profiling_stream_name || 'gitc-performance-test';
  const region = config.profiling_region || 'us-east-1';

  sendEvent(actionName, 'end', transactionId, dataType, actionId, streamName, region);
};


// Testing
// const streamName = 'gitc-performance-test';
// const actionName = 'CopyToS3';
// const config = {
//   execution_name: 'ABC123',
//   profiling_stream_name: streamName
// };

// const actionId = exports.sendStart(config, actionName);
// log.info(actionId);
// exports.sendEnd(config, actionName, actionId);

