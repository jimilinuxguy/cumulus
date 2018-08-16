'use strict';

const fs = require('fs');
const moment = require('moment');
const os = require('os');
const path = require('path');
const aws = require('@cumulus/common/aws');
const log = require('@cumulus/common/log');

const { deconstructCollectionId } = require('../es/indexer');
const { Search, defaultIndexAlias } = require('../es/search');

/**
 * This module provides functionalities to generate EMS reports.
 * The following environment variables are used:
 * process.env.ES_SCROLL_SIZE: default to defaultESScrollSize
 * process.env.ES_INDEX: set for testing purpose, default to defaultIndexAlias
 * process.env.ems_provider: default to 'cumulus'
 * process.env.stackName: it's used as part of the report filename
 * process.env.bucket: the bucket to store the generated reports
 */

const defaultESScrollSize = 1000;

/**
 * For each EMS report type (ingest, archive, delete),
 * map the EMS fields to CUMULUS granule/deletedgranule record fields,
 */
const emsMappings = {
  ingest: {
    dbID: 'granuleId',
    product: 'collectionId', // shortName part
    versionID: 'collectionId', // versionID part
    productVolume: 'productVolume',
    productState: 'status',
    externalDataProvider: 'provider',
    processingStartDateTime: 'processingStartDateTime',
    processingEndDateTime: 'processingEndDateTime',
    timeToArchive: 'timeToArchive',
    timeToPreprocess: 'timeToPreprocess',
    timeToXfer: 'duration'
  },

  archive: {
    dbID: 'granuleId',
    product: 'collectionId', // shortName part
    productVolume: 'productVolume',
    totalFiles: 'files', // total # files
    insertTime: 'createdAt',
    beginningDateTime: 'beginningDateTime',
    endingDateTime: 'endingDateTime',
    productionDateTime: 'productionDateTime',
    localGranuleID: 'granuleId',
    versionID: 'collectionId', // versionID part
    // since we have separate 'delete' report,
    // deleteFromArchive shall have value 'N', deleteEffectiveDate shall be left blank
    deleteFromArchive: 'deleteFromArchive', // N
    deleteEffectiveDate: 'deleteEffectiveDate', // null
    lastUpdate: 'lastUpdateDateTime'
  },

  delete: {
    dbID: 'granuleId',
    deleteEffectiveDate: 'deletedAt'
  }
};

/**
 * build and elasticsearch query parameters
 *
 * @param {string} esIndex - es index to search on
 * @param {string} type - es document type to search on
 * @param {string} startTime - startTime of the records
 * @param {string} endTime - endTime of the records
 * @returns {Object} query parameters
 */
function buildSearchQuery(esIndex, type, startTime, endTime) {
  // types are 'granule' or 'deletedgranule'
  const timeFieldName = (type === 'granule') ? 'createdAt' : 'deletedAt';
  const params = {
    index: esIndex,
    type: type,
    scroll: '30s',
    size: process.env.ES_SCROLL_SIZE || defaultESScrollSize,
    body: {
      query: {
        bool: {
          must: [
            {
              range: {
                [`${timeFieldName}`]: {
                  gte: moment.utc(startTime).toDate().getTime(),
                  lt: moment.utc(endTime).toDate().getTime()
                }
              }
            },
            {
              terms: {
                // filter out 'running' status
                status: ['failed', 'completed']
              }
            }]
        }
      }
    }
  };
  if (type === 'deletedgranule') params._source = ['granuleId', 'deletedAt'];
  return params;
}

/**
 * get the value of EMS record field from corresponding field of the  granule record
 *
 * @param {Object} granule - es granule record
 * @param {string} emsField - EMS field
 * @param {string} granField - granule field
 * @returns {string} granule metadata for EMS
 */
function getEmsFieldFromGranField(granule, emsField, granField) {
  const metadata = granule[granField];
  let result = metadata;
  switch (emsField) {
  case 'product':
    result = deconstructCollectionId(metadata).name;
    break;
  case 'versionID':
    result = parseInt(deconstructCollectionId(metadata).version, 10);
    break;
  case 'deleteFromArchive':
    result = 'N';
    break;
  case 'totalFiles':
    result = metadata.length;
    break;
  case 'productState':
    result = (metadata === 'completed') ? 'Successful' : 'Failed';
    break;
  // datetime format YYYY-MM-DD HH:MMAMorPM GMT
  case 'deleteEffectiveDate':
  case 'insertTime':
    // milliseconds to string
    result = (metadata) ? moment.utc(new Date(metadata)).format('YYYY-MM-DD hh:mmA') : metadata;
    break;
  case 'lastUpdate':
  case 'processingStartDateTime':
  case 'processingEndDateTime':
  case 'beginningDateTime':
  case 'endingDateTime':
  case 'productionDateTime':
    // string to different format string
    result = (metadata) ? moment.utc(Date.parse(metadata)).format('YYYY-MM-DD hh:mmA') : metadata;
    break;
  default:
    break;
  }
  return result;
}

/**
 * build EMS records from es granules
 *
 * @param {Object} mapping - mapping of EMS fields to granule fields
 * @param {Object} granules - es granules
 * @returns {Array<string>} EMS records
 */
function buildEMSRecords(mapping, granules) {
  return granules
    .map((granule) =>
      Object.keys(mapping)
        .map((emsField) => getEmsFieldFromGranField(granule, emsField, mapping[emsField]))
        .join('|&|'));
}

/**
 * upload a report to s3, a rev file is created if the report already exists in s3
 *
 * @param {string} filename - file to be upload to s3
 * @returns {string} - uploaded file in s3
 */
async function uploadReportToS3(filename) {
  const bucket = process.env.bucket;
  const originalKey = `${process.env.stackName}/ems/${path.basename(filename)}`;
  let key = originalKey;
  let exists = await aws.fileExists(bucket, key);
  let i = 1;
  while (exists) {
    key = `${originalKey}.rev${i}`;
    exists = await aws.fileExists(bucket, key); // eslint-disable-line no-await-in-loop
    i += 1;
  }

  await aws.s3().putObject({
    Bucket: bucket,
    Key: key,
    Body: fs.createReadStream(filename)
  }).promise();

  fs.unlinkSync(filename);
  const s3Uri = `s3://${bucket}/${key}`;
  log.info(`uploaded ${s3Uri}`);
  return s3Uri;
}

/**
 * build report file name
 * The report filename is in format:
 * <YYYYMMDD> _<Provider>_<FileType>_<DataSource>.flt.rev<1-n>
 *
 * @param {string} reportType - report type (ingest, archive, delete)
 * @param {string} startTime - start time of the report in a format that moment
 *   can parse
 * @returns {string} - report file name
 */
function buildReportFileName(reportType, startTime) {
  // DataSource: designates the database table name or data source file/table name
  // use stackname as DataSource for now
  const provider = process.env.ems_provider || 'cumulus';
  const dataSource = process.env.stackName;
  const datestring = moment.utc(startTime).format('YYYYMMDD');
  let fileType = 'Ing';
  if (reportType === 'archive') fileType = 'Arch';
  else if (reportType === 'delete') fileType = 'ArchDel';
  return `${datestring}_${provider}_${fileType}_${dataSource}.flt`;
}

/**
 * generate an EMS report
 *
 * @param {string} reportType - report type (ingest, archive, delete)
 * @param {string} startTime - start time of the records
 * @param {string} endTime - end time of the records
 * @returns {Object} - report type and its file path {reportType, file}
 */
async function generateReport(reportType, startTime, endTime) {
  log.debug(`ems.generateReport ${reportType} startTime: ${startTime} endTime: ${endTime}`);

  if (!Object.keys(emsMappings).includes(reportType)) {
    throw new Error(`ems.generateReport report type not supported: ${reportType}`);
  }

  // create a temporary file for the report
  const name = buildReportFileName(reportType, startTime);
  const filename = path.join(os.tmpdir(), name);
  const stream = fs.createWriteStream(filename);

  // retrieve granule/deletedgranule records in batches, and generate EMS records for each batch
  const esClient = await Search.es();
  const type = (reportType !== 'delete') ? 'granule' : 'deletedgranule';

  const esIndex = process.env.ES_INDEX || defaultIndexAlias;
  const searchQuery = buildSearchQuery(esIndex, type, startTime, endTime);
  let response = await esClient.search(searchQuery);
  let granules = response.hits.hits.map((s) => s._source);
  let numRetrieved = granules.length;
  stream.write(buildEMSRecords(emsMappings[reportType], granules).join('\n'));

  while (response.hits.total !== numRetrieved) {
    response = await esClient.scroll({ // eslint-disable-line no-await-in-loop
      scrollId: response._scroll_id,
      scroll: '30s'
    });
    granules = response.hits.hits.map((s) => s._source);
    stream.write('\n');
    stream.write(buildEMSRecords(emsMappings[reportType], granules).join('\n'));
    numRetrieved += granules.length;
  }
  stream.end();
  log.debug(`EMS ${reportType} generated with ${numRetrieved} records: ${filename}`);

  // upload to s3
  const s3Uri = await uploadReportToS3(filename);
  return { reportType, file: s3Uri };
}

/**
 * generate all EMS reports given the time range of the records
 *
 * @param {string} startTime - start time of the records
 * @param {string} endTime - end time of the records
 * @returns {Array<Object>} - list of report type and its file path {reportType, file}
 */
function generateReports(startTime, endTime) {
  const jobs = Object.keys(emsMappings)
    .map((reportType) => generateReport(reportType, startTime, endTime));
  return Promise.all(jobs);
}

module.exports = {
  emsMappings,
  generateReport,
  generateReports
};
