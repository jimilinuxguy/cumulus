/* eslint no-console: 0 */

'use strict';

const AWS = require('aws-sdk');
const util = require('util');
const exec = util.promisify(require('child_process').exec); // eslint-disable-line security/detect-child-process
const fs = require('fs');
const https = require('https');
const url = require('url');

/**
 * Return a list of environment variables that should be set but aren't
 *
 * @returns {Array<string>} a list of missing environment variables
 */
function missingEnvironmentVariables() {
  return [
    'asyncOperationId',
    'asyncOperationsTable',
    'lambdaName',
    'payloadUrl'
  ].filter((key) => process.env[key] === undefined);
}

/**
 * Fetch an object from S3 and parse it as JSON
 *
 * @param {string} Bucket - the S3 bucket
 * @param {string} Key - the S3 key
 * @returns {Object|Array} the parsed payload
 */
async function fetchPayload(Bucket, Key) {
  const s3 = new AWS.S3();

  let payloadResponse;
  try {
    payloadResponse = await s3.getObject({ Bucket, Key }).promise();
  }
  catch (err) {
    console.error(`Failed to fetch s3://${Bucket}/${Key}: ${err.message}`);
    throw err;
  }

  return JSON.parse(payloadResponse.Body.toString());
}

/**
 * Fetch and delete a lambda payload from S3
 *
 * @param {string} payloadUrl - the s3:// URL of the payload
 * @returns {Promise<Object>} a payload that can be passed as the event of a lambda call
 */
async function fetchAndDeletePayload(payloadUrl) {
  const parsedPayloadUrl = url.parse(payloadUrl);
  const Bucket = parsedPayloadUrl.hostname;
  const Key = parsedPayloadUrl.path.substring(1);

  const payload = await fetchPayload(Bucket, Key);

  console.log(`Deleting ${payloadUrl}`);
  await (new AWS.S3()).deleteObject({ Bucket, Key }).promise();

  return payload;
}

/**
 * Query AWS for the codeUrl, moduleFileName, and moduleFunctionName of a
 *   Lambda function.
 *
 * @param {string} FunctionName - the name of the Lambda function
 * @returns {Promise<Object>} an object with the codeUrl, moduleFileName, and
 *   moduleFunctionName
 */
async function getLambdaInfo(FunctionName) {
  const lambda = new AWS.Lambda();

  const getFunctionResponse = await lambda.getFunction({
    FunctionName
  }).promise();

  const handler = getFunctionResponse.Configuration.Handler;
  const [moduleFileName, moduleFunctionName] = handler.split('.');

  return {
    moduleFileName,
    moduleFunctionName,
    codeUrl: getFunctionResponse.Code.Location
  };
}

/**
 * Download a lambda function from AWS and extract it to the
 *   /home/task/lambda-function directory
 *
 * @param {string} codeUrl - an https URL to download the lambda code from
 * @returns {Promise<undefined>} resolves when the code has been downloaded
 */
async function fetchLambdaFunction(codeUrl) {
  await new Promise((resolve, reject) => {
    const file = fs.createWriteStream('/home/task/fn.zip');

    file.on('error', reject);
    file.on('finish', () => file.close());
    file.on('close', resolve);

    https.get(codeUrl, (res) => res.pipe(file));
  });

  return exec('unzip -o /home/task/fn.zip -d /home/task/lambda-function');
}
/**
 * Update an AsyncOperation item in DynamoDB
 *
 * For help with parameters, see:
 * https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#updateItem-property
 *
 * @param {Object} params - params
 * @param {string} params.TableName - the AsyncOperation DynamoDB table
 * @param {string} params.id - the id of the AsyncOperation
 * @param {Object} params.names - the ExpressionAttributeNames to update
 * @param {Object} params.values - the ExpressionAttributeValues to update
 * @param {string} params.expression - the UpdateExpression to update
 * @returns {Promise} resolves when the item has been updated
 */
function updateAsyncOperation(params) {
  const {
    TableName,
    id,
    names,
    values,
    expression
  } = params;

  const dynamodb = new AWS.DynamoDB();

  return dynamodb.updateItem({
    TableName,
    Key: { id: { S: id } },
    ExpressionAttributeNames: names,
    ExpressionAttributeValues: values,
    UpdateExpression: expression
  }).promise();
}

/**
 * Update DynamoDB with a successful result
 *
 * @param {string} TableName - the AsyncOperation DynamoDB table
 * @param {string} id - the id of the AsyncOperation
 * @param {Object} result - the result to store.  Will be converted to JSON.
 * @returns {Promise} resolves when the item has been updated
 */
function storeOperationSuccess(TableName, id, result) {
  return updateAsyncOperation({
    TableName,
    id,
    names: { '#S': 'status', '#R': 'result' },
    values: { ':s': { S: 'SUCCEEDED' }, ':r': { S: JSON.stringify(result) } },
    expression: 'SET #S = :s, #R = :r'
  });
}

/**
 * Update DynamoDB with a failed result
 *
 * @param {string} TableName - the AsyncOperation DynamoDB table
 * @param {string} id - the id of the AsyncOperation
 * @param {string} message - the error message to store
 * @returns {Promise} resolves when the item has been updated
 */
function storeOperationFailure(TableName, id, message) {
  return updateAsyncOperation({
    TableName,
    id,
    names: { '#S': 'status', '#E': 'error' },
    values: { ':s': { S: 'FAILED' }, ':e': { S: message } },
    expression: 'SET #S = :s, #E = :e'
  });
}

/**
 * Download and run a Lambda task locally.  On completion, write the results out
 *   to a DynamoDB table.
 *
 * @returns {Promise<undefined>} resolves when the task has completed
 */
async function runTask() {
  let lambdaInfo;
  let payload;

  try {
    // Get some information about the lambda function that we'll be calling
    lambdaInfo = await getLambdaInfo(process.env.lambdaName);

    // Download the task (to the /home/task/lambda-function directory)
    await fetchLambdaFunction(lambdaInfo.codeUrl);

    // Fetch the event that will be passed to the lambda function from S3
    payload = await fetchAndDeletePayload(process.env.payloadUrl);
  }
  catch (err) {
    console.error(err);
    await storeOperationFailure(
      process.env.asyncOperationsTable,
      process.env.asyncOperationId,
      `AsyncOperation failure: ${err.message}`
    );
    return;
  }

  try {
    // Load the lambda function
    const task = require(`/home/task/lambda-function/${lambdaInfo.moduleFileName}`); //eslint-disable-line global-require, import/no-dynamic-require, max-len, security/detect-non-literal-require

    // Run the lambda function
    const result = await task[lambdaInfo.moduleFunctionName](payload);

    // Write the result out to DynamoDb
    await storeOperationSuccess(
      process.env.asyncOperationsTable,
      process.env.asyncOperationId,
      result
    );
  }
  catch (err) {
    await storeOperationFailure(
      process.env.asyncOperationsTable,
      process.env.asyncOperationId,
      err.message
    );
  }
}

// Here's where the magic happens ...

// Make sure that all of the required environment variables are set
const missingVars = missingEnvironmentVariables();
if (missingVars.length === 0) runTask();
else console.error('Missing environment variables:', missingVars.join(', '));