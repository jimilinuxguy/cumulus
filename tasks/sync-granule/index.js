'use strict';

const path = require('path');
const cumulusMessageAdapter = require('@cumulus/cumulus-message-adapter-js');
const errors = require('@cumulus/common/errors');
const lock = require('@cumulus/ingest/lock');
const granule = require('@cumulus/ingest/granule');
const log = require('@cumulus/common/log');

/**
 * Ingest a list of granules
 *
 * @param {Object} ingest - an ingest object
 * @param {string} bucket - the name of an S3 bucket, used for locking
 * @param {string} provider - the name of a provider, used for locking
 * @param {Object[]} granules - the granules to be ingested
 * @returns {Promise.<Array>} - the list of successfully ingested granules
 */
async function download(ingest, bucket, provider, granules) {
  const updatedGranules = [];

  const proceed = await lock.proceed(bucket, provider, granules[0].granuleId);

  if (!proceed) {
    const err =
      new errors.ResourcesLockedError('Download lock remained in place after multiple tries');
    log.error(err);
    throw err;
  }

  for (const g of granules) {
    try {
      const r = await ingest.ingest(g, bucket);
      updatedGranules.push(r);
    }
    catch (e) {
      await lock.removeLock(bucket, provider.id, g.granuleId);
      log.error(e);
      throw e;
    }
  }

  await lock.removeLock(bucket, provider.id, granules[0].granuleId);
  return updatedGranules;
}

/**
 * Ingest a list of granules
 *
 * @param {Object} event - contains input and config parameters
 * @returns {Promise.<Object>} - a description of the ingested granules
 */
exports.syncGranule = function syncGranule(event) {
  const config = event.config;
  const input = event.input;
  const stack = config.stack;
  const buckets = config.buckets;
  const provider = config.provider;
  const collection = config.collection;
  const forceDownload = config.forceDownload || false;
  const downloadBucket = config.downloadBucket;
  let duplicateHandling = config.duplicateHandling;
  if (!duplicateHandling && collection && collection.duplicateHandling) {
    duplicateHandling = collection.duplicateHandling;
  }

  // use stack and collection names to prefix fileStagingDir
  const fileStagingDir = path.join(
    (config.fileStagingDir || 'file-staging'),
    stack
  );

  if (!provider) {
    const err = new errors.ProviderNotFound('Provider info not provided');
    log.error(err);
    return Promise.reject(err);
  }

  const IngestClass = granule.selector('ingest', provider.protocol);
  const ingest = new IngestClass(
    buckets,
    collection,
    provider,
    fileStagingDir,
    forceDownload,
    duplicateHandling
  );

  return download(ingest, downloadBucket, provider, input.granules)
    .then((granules) => {
      if (ingest.end) ingest.end();

      const output = { granules };
      if (collection && collection.process) output.process = collection.process;
      if (config.pdr) output.pdr = config.pdr;

      return output;
    }).catch((e) => {
      if (ingest.end) ingest.end();

      let errorToThrow = e;
      if (e.toString().includes('ECONNREFUSED')) {
        errorToThrow = new errors.RemoteResourceError('Connection Refused');
      }
      else if (e.details && e.details.status === 'timeout') {
        errorToThrow = new errors.ConnectionTimeout('connection Timed out');
      }

      log.error(errorToThrow);
      throw errorToThrow;
    });
};

/**
 * Lambda handler
 *
 * @param {Object} event - a Cumulus Message
 * @param {Object} context - an AWS Lambda context
 * @param {Function} callback - an AWS Lambda handler
 * @returns {undefined} - does not return a value
 */
exports.handler = function handler(event, context, callback) {
  const startTime = Date.now();

  cumulusMessageAdapter.runCumulusTask(exports.syncGranule, event, context, (err, data) => {
    if (err) {
      callback(err);
    }
    else {
      const endTime = Date.now();
      const additionalMetaFields = {
        sync_granule_duration: endTime - startTime,
        sync_granule_end_time: endTime
      };
      const meta = Object.assign({}, data.meta, additionalMetaFields);
      callback(null, Object.assign({}, data, { meta }));
    }
  });
};
