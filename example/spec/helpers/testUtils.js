const fs = require('fs');
const { s3 } = require('@cumulus/common/aws');
const { Config } = require('kes');
const lodash = require('lodash');
const { exec } = require('child-process-promise');
const path = require('path');

jasmine.DEFAULT_TIMEOUT_INTERVAL = 10000000;

const timestampedTestDataPrefix = (prefix) => `${prefix}-${(new Date().getTime())}-test-data/pdrs`;

/**
 * Loads and parses the configuration defined in `./app/config.yml`
 *
 * @returns {Object} - Configuration object
*/
function loadConfig() {
  // make sure deployment env variable is set
  if (!process.env.DEPLOYMENT) {
    throw new Error(
      'You MUST set DEPLOYMENT environment variable with the name' +
      ' of your deployment before running tests.'
    );
  }

  const params = {
    deployment: process.env.DEPLOYMENT,
    configFile: './app/config.yml',
    kesFolder: './app'
  };

  const config = new Config(params);

  if (config.deployment === 'default') {
    throw new Error('the default deployment cannot be used for integration tests');
  }

  config.test_configs.buckets = config.buckets;
  config.test_configs.deployment = config.deployment;

  return config.test_configs;
}

/**
 * Creates a new file using a template file and configuration object which
 * defines fields to write to in the input template.
 *
 * @param   {Object} options - Options
 * @param   {string} options.inputTemplateFilename - File path and name of template file (json)
 * @param   {Object} options.config - Object to use to write to fields in the template
 * @returns {string} - File path and name of output file (json)
 */
function templateFile({ inputTemplateFilename, config }) {
  const inputTemplate = JSON.parse(fs.readFileSync(inputTemplateFilename));
  const templatedInput = lodash.merge(lodash.cloneDeep(inputTemplate), config);
  let jsonString = JSON.stringify(templatedInput, null, 2);
  jsonString = jsonString.replace('{{AWS_ACCOUNT_ID}}', config.AWS_ACCOUNT_ID);
  const templatedInputFilename = inputTemplateFilename.replace('.template', '');
  fs.writeFileSync(templatedInputFilename, jsonString);
  return templatedInputFilename;
}

/**
 * Upload a file from the test-data package to the S3 test data
 *
 * @param {string} file - filename of data to upload
 * @param {string} bucket - bucket to upload to
 * @param {string} prefix - S3 folder prefix
 * @returns {Promise<Object>} - promise returned from S3 PUT
 */
function uploadTestDataToS3(file, bucket, prefix = 'cumulus-test-data/pdrs') {
  const data = fs.readFileSync(require.resolve(file), 'utf8');
  const key = path.basename(file);
  console.log('\nUploading test data to ', prefix);
  return s3().putObject({
    Bucket: bucket,
    Key: `${prefix}/${key}`,
    Body: data
  }).promise();
}

/**
 * For the given bucket, upload all the test data files to S3
 *
 * @param {string} bucket - S3 bucket
 * @param {Array<string>} data - list of test data files
 * @param {string} prefix - S3 folder prefix
 * @returns {Array<Promise>} - responses from S3 upload
 */
function uploadTestDataToBucket(bucket, data, prefix) {
  return Promise.all(data.map((file) => uploadTestDataToS3(file, bucket, prefix)));
}

/**
 * Delete a folder on a given bucket on S3
 *
 * @param {string} bucket - the bucket name
 * @param {string} folder - the folder to delete
 * @returns {Promise} undefined
 */
async function deleteFolder(bucket, folder) {
  const l = await s3().listObjectsV2({
    Bucket: bucket,
    Prefix: folder
  }).promise();

  await Promise.all(l.Contents.map((item) =>
    s3().deleteObject({
      Bucket: bucket,
      Key: item.Key
    }).promise()));
}

/**
 * Returns execution ARN from a statement machine Arn and executionName
 *
 * @param {string} executionArn - execution ARN
 * @returns {string} return aws console url for the execution
 */
function getExecutionUrl(executionArn) {
  const region = process.env.AWS_DEFAULT_REGION || 'us-east-1';
  return `https://console.aws.amazon.com/states/home?region=${region}` +
         `#/executions/details/${executionArn}`;
}

/**
 * Redeploy the current Cumulus deployment
 *
 * @param {Object} config - configuration object from loadConfig()
 * @returns {undefined} none
 */
async function redeploy(config) {
  const deployCommand = `./node_modules/.bin/kes  cf deploy --kes-folder app --template node_modules/@cumulus/deployment/app --deployment ${config.deployment} --region us-east-1`;
  console.log(`Redeploying ${config.deployment}`);
  await exec(deployCommand)
    .then((result) => {
      console.log(result.stdout);
      if (result.error) {
        console.log(`Deployment error: ${result.error}`);
      }
    });
  console.log(`Redeploy of ${config.deployment} complete`);
}

module.exports = {
  timestampedTestDataPrefix,
  loadConfig,
  templateFile,
  uploadTestDataToS3,
  uploadTestDataToBucket,
  deleteFolder,
  getExecutionUrl,
  redeploy
};
