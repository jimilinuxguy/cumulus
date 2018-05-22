'use strict';

const uuidv4 = require('uuid/v4');
const AWS = require('aws-sdk');
const log = require('./log');

/**
 * Kinesis Event
 */
class KinesisEvent {
  constructor(eventName, streamName, region = 'us-east-1') {
    this.uuid = uuidv4();
    this.eventName = eventName;
    this.streamName = streamName;

    this.kinesis = new AWS.Kinesis({ region: region });
  }

  async sendStart() {
    const record = {
      'transaction-id': this.uuid,
      'time-stamp': `${Date.now()}`,
      name: this.eventName,
      type: 'start'
    };
    return this.sendPacket(record);
  }

  /**
   * Creates a Kinesis DataRecord
   * 
   * @param {Object} hash - Data
   * @returns {Object} Kinesis DataRecord
   */
  dataRecord(hash) {
    return {
      "Data": JSON.stringify(hash),
      "PartitionKey": "GITC"
    }
  }

  async sendEnd() {
    const record = {
      'transaction-id': this.uuid,
      'time-stamp': `${Date.now()}`,
      name: this.eventName,
      type: 'end'
    };
    return this.sendPacket(record);
  }

  async sendPacket(record) {
    const params = {
      Records: [
        this.dataRecord(record)
      ],
      StreamName: this.streamName
    };

    this.kinesis.putRecords(params, function (err, data) {
      if(err) {
        log.error("Kinesis", err);
      } 
      else {
        log.info(`Kinesis uuid:${record["transaction-id"]} ${record.type} ${record.name} message sent`);
      }
    })
  }
}

exports.KinesisEvent = KinesisEvent;

// Testing
const streamName = 'GitcPerformanceTesting';
const eventName = 'SyncGranule';

const k = new KinesisEvent(eventName, streamName);
k.sendStart();
k.sendEnd();

