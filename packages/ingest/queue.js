'use strict';

const {
  getS3Object,
  sendSQSMessage,
  parseS3Uri
} = require('@cumulus/common/aws');
const get = require('lodash.get');
const uuidv4 = require('uuid/v4');

/**
 * Create a message from a template stored on S3
 *
 * @param {string} templateUri - S3 uri to the workflow template
 * @returns {Promise} message object
 **/
async function getMessageFromTemplate(templateUri) {
  const parsedS3Uri = parseS3Uri(templateUri);
  const data = await getS3Object(parsedS3Uri.Bucket, parsedS3Uri.Key);
  return JSON.parse(data.Body);
}

/**
 * Enqueue a PDR to be parsed
 *
 * @param {Object} pdr - the PDR to be enqueued for parsing
 * @param {string} queueUrl - the SQS queue to add the message to
 * @param {string} parsePdrMessageTemplateUri - the S3 URI of template for
 * a granule ingest message
 * @param {Object} provider - the provider config to be attached to the message
 * @param {Object} collection - the collection config to be attached to the
 *   message
 * @returns {Promise} - resolves when the message has been enqueued
 */
async function enqueueParsePdrMessage(
  pdr,
  queueUrl,
  parsePdrMessageTemplateUri,
  provider,
  collection) {
  const message = await getMessageFromTemplate(parsePdrMessageTemplateUri);

  message.meta.provider = provider;
  message.meta.collection = collection;

  message.payload = {
    pdr
  };

  return sendSQSMessage(queueUrl, message);
}
module.exports.enqueueParsePdrMessage = enqueueParsePdrMessage;

/**
 * Enqueue a granule to be ingested
 *
 * @param {Object} granule - the granule to be enqueued for ingest
 * @param {string} queueUrl - the SQS queue to add the message to
 * @param {string} granuleIngestMessageTemplateUri - the S3 URI of template for
 * a granule ingest message
 * @param {Object} provider - the provider config to be attached to the message
 * @param {Object} collection - the collection config to be attached to the
 *   message
 * @param {Object} pdr - an optional PDR to be configured in the message payload
 * @returns {Promise} - resolves when the message has been enqueued
 */
async function enqueueGranuleIngestMessage(
  granule,
  queueUrl,
  granuleIngestMessageTemplateUri,
  provider,
  collection,
  pdr
) {
  // Build the message from a template
  const message = await getMessageFromTemplate(granuleIngestMessageTemplateUri);

  message.payload = {
    granules: [{
      granuleId: granule.granuleId,
      files: granule.files
    }]
  };
  if (pdr) message.payload.pdr = pdr;

  message.meta.provider = provider;
  message.meta.collection = collection;

  return sendSQSMessage(queueUrl, message);
}
exports.enqueueGranuleIngestMessage = enqueueGranuleIngestMessage;

/**
 * Queue a workflow to be picked up by SF starter
 * 
 * @param {*} event - event to queue with workflow and payload info
 */
async function queueWorkflowMessage(event) {
  const template = get(event, 'template');
  const provider = get(event, 'provider', {});
  const collection = get(event, 'collection', {});
  const payload = get(event, 'payload', {});

  const message = await getMessageFromTemplate(template);

  let queueUrl = null;

  if (message.resources) {
    queueUrl = message.resources.queues.startSF;
  }
  else {
    queueUrl = message.meta.queues.startSF;
  }

  message.provider = provider;
  message.payload = payload;
  message.cumulus_meta.execution_name = uuidv4();

  if (collection) {
    message.collection = {
      id: collection.name,
      meta: collection
    };
  }

  console.log('Message2: ' + JSON.stringify(message));

  // const x = sendSQSMessage(message.resources.queues.startSF, message);

  // console.log('\n\nQueue message ' + x);

  // return x;

  return sendSQSMessage(queueUrl, message);
}
exports.queueWorkflowMessage = queueWorkflowMessage;
