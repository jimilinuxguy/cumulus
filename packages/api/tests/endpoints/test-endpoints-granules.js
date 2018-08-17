'use strict';

const fs = require('fs');
const sinon = require('sinon');
const test = require('ava');
const aws = require('@cumulus/common/aws');
const { StepFunction } = require('@cumulus/ingest/aws');
const { CMR } = require('@cumulus/cmrjs');
const { DefaultProvider } = require('@cumulus/ingest/crypto');
const { randomString } = require('@cumulus/common/test-utils');
const models = require('../../models');
const bootstrap = require('../../lambdas/bootstrap');
const granuleEndpoint = require('../../endpoints/granules');
const indexer = require('../../es/indexer');
const {
  fakeGranuleFactory,
  fakeUserFactory
} = require('../../lib/testUtils');
const { Search } = require('../../es/search');
const xml2js = require('xml2js');
const { xmlParseOptions } = require('@cumulus/cmrjs/utils');

// create all the variables needed across this test
let esClient;
let esIndex;
let g;
let authToken;
let userModel;
test.before(async () => {
  esIndex = randomString();
  process.env.GranulesTable = randomString();
  process.env.UsersTable = randomString();
  process.env.stackName = randomString();
  process.env.internal = randomString();

  g = new models.Granule();

  // create esClient
  esClient = await Search.es('fakehost');

  // add fake elasticsearch index
  await bootstrap.bootstrapElasticSearch('fakehost', esIndex);

  // create a fake bucket
  await aws.s3().createBucket({ Bucket: process.env.internal }).promise();

  // create fake Granules table
  await g.createTable();

  // create fake Users table
  userModel = new models.User();
  await userModel.createTable();

  authToken = (await userModel.create(fakeUserFactory())).password;
});

test.beforeEach(async (t) => {
  t.context.authHeaders = {
    Authorization: `Bearer ${authToken}`
  };

  // create fake granule records
  t.context.fakeGranules = ['completed', 'failed'].map(fakeGranuleFactory);

  for (let i = 0; i < t.context.fakeGranules.length; i += 1) {
    const granule = t.context.fakeGranules[i];
    const record = await g.create(granule); // eslint-disable-line no-await-in-loop
    await indexer.indexGranule(esClient, record, esIndex); // eslint-disable-line no-await-in-loop
  }

  await Promise.all(t.context.fakeGranules.map((granule) =>
    g.create(granule)
      .then((record) => indexer.indexGranule(esClient, record, esIndex))));
});

test.after.always(async () => {
  await g.deleteTable();
  await userModel.deleteTable();
  await esClient.indices.delete({ index: esIndex });
  await aws.recursivelyDeleteS3Bucket(process.env.internal);
});


test.serial('default returns list of granules', async (t) => {
  const event = {
    httpMethod: 'GET',
    headers: t.context.authHeaders
  };

  const response = await granuleEndpoint(event);

  const { meta, results } = JSON.parse(response.body);
  t.is(results.length, 2);
  t.is(meta.stack, process.env.stackName);
  t.is(meta.table, 'granule');
  t.is(meta.count, 2);
  const granuleIds = t.context.fakeGranules.map((i) => i.granuleId);
  results.forEach((r) => {
    t.true(granuleIds.includes(r.granuleId));
  });
});

test.serial('GET returns an existing granule', async (t) => {
  const event = {
    httpMethod: 'GET',
    pathParameters: {
      granuleName: t.context.fakeGranules[0].granuleId
    },
    headers: t.context.authHeaders
  };

  const response = await granuleEndpoint(event);

  const { granuleId } = JSON.parse(response.body);
  t.is(granuleId, t.context.fakeGranules[0].granuleId);
});

test.serial('GET fails if granule is not found', async (t) => {
  const event = {
    httpMethod: 'GET',
    pathParameters: {
      granuleName: 'unknownGranule'
    },
    headers: t.context.authHeaders
  };

  const response = await granuleEndpoint(event);

  t.is(response.statusCode, 404);
  const { message } = JSON.parse(response.body);
  t.is(message, 'Granule not found');
});

test.serial('PUT fails if action is not supported', async (t) => {
  const event = {
    httpMethod: 'PUT',
    pathParameters: {
      granuleName: t.context.fakeGranules[0].granuleId
    },
    headers: t.context.authHeaders,
    body: JSON.stringify({ action: 'reprocess' })
  };

  const response = await granuleEndpoint(event);

  t.is(response.statusCode, 400);
  const { message } = JSON.parse(response.body);
  t.true(message.includes('Action is not supported'));
});

test.serial('PUT fails if action is not provided', async (t) => {
  const event = {
    httpMethod: 'PUT',
    pathParameters: {
      granuleName: t.context.fakeGranules[0].granuleId
    },
    headers: t.context.authHeaders
  };

  const response = await granuleEndpoint(event);

  t.is(response.statusCode, 400);
  const { message } = JSON.parse(response.body);
  t.is(message, 'Action is missing');
});

test.serial('reingest a granule', async (t) => {
  const fakeSFResponse = {
    execution: {
      input: JSON.stringify({
        meta: {
          workflow_name: 'IngestGranule'
        },
        payload: {}
      })
    }
  };

  const event = {
    httpMethod: 'PUT',
    pathParameters: {
      granuleName: t.context.fakeGranules[0].granuleId
    },
    headers: t.context.authHeaders,
    body: JSON.stringify({ action: 'reingest' })
  };

  // fake workflow
  process.env.bucket = process.env.internal;
  const message = JSON.parse(fakeSFResponse.execution.input);
  const key = `${process.env.stackName}/workflows/${message.meta.workflow_name}.json`;
  await aws.s3().putObject({ Bucket: process.env.bucket, Key: key, Body: 'test data' }).promise();

  sinon.stub(
    StepFunction,
    'getExecutionStatus'
  ).callsFake(() => Promise.resolve(fakeSFResponse));

  const response = await granuleEndpoint(event);

  const body = JSON.parse(response.body);
  t.is(body.status, 'SUCCESS');
  t.is(body.action, 'reingest');

  const updatedGranule = await g.get({ granuleId: t.context.fakeGranules[0].granuleId });
  t.is(updatedGranule.status, 'running');

  StepFunction.getExecutionStatus.restore();
});

test.serial('apply an in-place workflow to an existing granule', async (t) => {
  const fakeSFResponse = {
    execution: {
      input: JSON.stringify({
        meta: {
          workflow_name: 'inPlaceWorkflow'
        },
        payload: {}
      })
    }
  };

  const event = {
    httpMethod: 'PUT',
    pathParameters: {
      granuleName: t.context.fakeGranules[0].granuleId
    },
    headers: t.context.authHeaders,
    body: JSON.stringify({
      action: 'applyWorkflow',
      workflow: 'inPlaceWorkflow',
      messageSource: 'output'
    })
  };

  //fake in-place workflow
  process.env.bucket = process.env.internal;
  const message = JSON.parse(fakeSFResponse.execution.input);
  const key = `${process.env.stackName}/workflows/${message.meta.workflow_name}.json`;
  await aws.s3().putObject({ Bucket: process.env.bucket, Key: key, Body: 'fake in-place workflow' }).promise();

  // return fake previous execution
  sinon.stub(
    StepFunction,
    'getExecutionStatus'
  ).callsFake(() => Promise.resolve({
    execution: {
      output: JSON.stringify({
        meta: {
          workflow_name: 'IngestGranule'
        },
        payload: {}
      })
    }
  }));

  const response = await granuleEndpoint(event);
  const body = JSON.parse(response.body);
  t.is(body.status, 'SUCCESS');
  t.is(body.action, 'applyWorkflow inPlaceWorkflow');

  const updatedGranule = await g.get({ granuleId: t.context.fakeGranules[0].granuleId });
  t.is(updatedGranule.status, 'running');

  StepFunction.getExecutionStatus.restore();
});

test.serial('remove a granule from CMR', async (t) => {
  const event = {
    httpMethod: 'PUT',
    pathParameters: {
      granuleName: t.context.fakeGranules[0].granuleId
    },
    headers: t.context.authHeaders,
    body: JSON.stringify({ action: 'removeFromCmr' })
  };

  sinon.stub(
    DefaultProvider,
    'decrypt'
  ).callsFake(() => Promise.resolve('fakePassword'));

  sinon.stub(
    CMR.prototype,
    'deleteGranule'
  ).callsFake(() => Promise.resolve());

  const response = await granuleEndpoint(event);

  const body = JSON.parse(response.body);
  t.is(body.status, 'SUCCESS');
  t.is(body.action, 'removeFromCmr');

  const updatedGranule = await g.get({ granuleId: t.context.fakeGranules[0].granuleId });
  t.is(updatedGranule.published, false);
  t.is(updatedGranule.cmrLink, null);

  CMR.prototype.deleteGranule.restore();
  DefaultProvider.decrypt.restore();
});

test.serial('DELETE deleting an existing granule that is published will fail', async (t) => {
  const event = {
    httpMethod: 'DELETE',
    pathParameters: {
      granuleName: t.context.fakeGranules[0].granuleId
    },
    headers: t.context.authHeaders
  };

  const response = await granuleEndpoint(event);

  t.is(response.statusCode, 400);
  const { message } = JSON.parse(response.body);
  t.is(
    message,
    'You cannot delete a granule that is published to CMR. Remove it from CMR first'
  );
});

test.serial('DELETE deleting an existing unpublished granule', async (t) => {
  const buckets = {
    protected: {
      name: randomString(),
      type: 'protected'
    },
    public: {
      name: randomString(),
      type: 'public'
    }
  };
  const newGranule = fakeGranuleFactory('failed');
  newGranule.published = false;
  newGranule.files = [
    {
      bucket: buckets.protected.name,
      name: `${newGranule.granuleId}.hdf`,
      filename: `s3://${buckets.protected.name}/${randomString()}/${newGranule.granuleId}.hdf`
    },
    {
      bucket: buckets.protected.name,
      name: `${newGranule.granuleId}.cmr.xml`,
      filename: `s3://${buckets.protected.name}/${randomString()}/${newGranule.granuleId}.cmr.xml`
    },
    {
      bucket: buckets.public.name,
      name: `${newGranule.granuleId}.jpg`,
      filename: `s3://${buckets.public.name}/${randomString()}/${newGranule.granuleId}.jpg`
    }
  ];

  await aws.s3().createBucket({ Bucket: buckets.protected.name }).promise();
  await aws.s3().createBucket({ Bucket: buckets.public.name }).promise();

  for (let i = 0; i < newGranule.files.length; i += 1) {
    const file = newGranule.files[i];
    const parsed = aws.parseS3Uri(file.filename);
    await aws.s3().putObject({ // eslint-disable-line no-await-in-loop
      Bucket: parsed.Bucket,
      Key: parsed.Key,
      Body: `test data ${randomString()}`
    }).promise();
  }

  // create a new unpublished granule
  await g.create(newGranule);

  const event = {
    httpMethod: 'DELETE',
    pathParameters: {
      granuleName: newGranule.granuleId
    },
    headers: t.context.authHeaders
  };

  const response = await granuleEndpoint(event);

  t.is(response.statusCode, 200);
  const { detail } = JSON.parse(response.body);
  t.is(detail, 'Record deleted');

  // verify the files are deleted
  for (let i = 0; i < newGranule.files.length; i += 1) {
    const file = newGranule.files[i];
    const parsed = aws.parseS3Uri(file.filename);
    t.false(await aws.fileExists(parsed.Bucket, parsed.Key)); // eslint-disable-line no-await-in-loop
  }

  await aws.recursivelyDeleteS3Bucket(buckets.protected.name);
  await aws.recursivelyDeleteS3Bucket(buckets.public.name);
});

test.serial('move a granule with no .cmr.xml file', async (t) => {
  const bucket = process.env.internal;
  const [secondBucket, thirdBucket] = [randomString(), randomString()];
  await aws.s3().createBucket({ Bucket: secondBucket }).promise();
  await aws.s3().createBucket({ Bucket: thirdBucket }).promise();

  const newGranule = fakeGranuleFactory();

  newGranule.files = [
    {
      bucket,
      name: `${newGranule.granuleId}.txt`,
      filepath: `${process.env.stackName}/original_filepath/${newGranule.granuleId}.txt`,
      filename: `s3://${bucket}/${process.env.stackName}/original_filepath/${newGranule.granuleId}.txt`
    },
    {
      bucket,
      name: `${newGranule.granuleId}.md`,
      filename: `s3://${bucket}/${process.env.stackName}/original_filepath/${newGranule.granuleId}.md`
    },
    {
      bucket: secondBucket,
      name: `${newGranule.granuleId}.jpg`,
      filepath: `${process.env.stackName}/original_filepath/${newGranule.granuleId}.jpg`,
      filename: `s3://${secondBucket}/${process.env.stackName}/original_filepath/${newGranule.granuleId}.jpg`
    }
  ];

  await g.create(newGranule);

  await Promise.all(newGranule.files.map(async (file) => {
    const filepath = file.filepath || aws.parseS3Uri(file.filename).Key;
    aws.s3().putObject({ Bucket: file.bucket, Key: filepath, Body: 'test data' }).promise();
  }));

  const destinationFilepath = `${process.env.stackName}/granules_moved`;
  const destinations = [
    {
      regex: '.*.txt$',
      bucket,
      filepath: destinationFilepath
    },
    {
      regex: '.*.md$',
      bucket: thirdBucket,
      filepath: destinationFilepath
    },
    {
      regex: '.*.jpg$',
      bucket,
      filepath: destinationFilepath
    }
  ];

  const event = {
    httpMethod: 'PUT',
    pathParameters: {
      granuleName: newGranule.granuleId
    },
    headers: t.context.authHeaders,
    body: JSON.stringify({
      action: 'move',
      destinations
    })
  };

  const response = await granuleEndpoint(event);

  const body = JSON.parse(response.body);
  t.is(body.status, 'SUCCESS');
  t.is(body.action, 'move');

  await aws.s3().listObjects({ Bucket: bucket, Prefix: destinationFilepath }).promise().then((list) => {
    t.is(list.Contents.length, 2);
    list.Contents.forEach((item) => {
      t.is(item.Key.indexOf(destinationFilepath), 0);
    });
  });

  await aws.s3().listObjects({ Bucket: thirdBucket, Prefix: destinationFilepath }).promise().then((list) => {
    t.is(list.Contents.length, 1);
    list.Contents.forEach((item) => {
      t.is(item.Key.indexOf(destinationFilepath), 0);
    });
  });

  // check the granule in table is updated
  const updatedGranule = await g.get({ granuleId: newGranule.granuleId });
  updatedGranule.files.forEach((file) => {
    t.true(file.filepath.startsWith(destinationFilepath));
    const destination = destinations.find((dest) => file.name.match(dest.regex));
    t.is(destination.bucket, file.bucket);
    t.true(file.filename.startsWith(aws.buildS3Uri(destination.bucket, destinationFilepath)));
  });

  await aws.recursivelyDeleteS3Bucket(secondBucket);
  await aws.recursivelyDeleteS3Bucket(thirdBucket);
});

test.serial('move a file and update metadata', async (t) => {
  const bucket = process.env.internal;
  process.env.bucket = bucket;
  const buckets = {
    protected: {
      name: process.env.internal,
      type: 'protected'
    },
    public: {
      name: randomString(),
      type: 'public'
    }
  };

  process.env.DISTRIBUTION_ENDPOINT = 'http://example.com/';
  await aws.s3().putObject({ Bucket: bucket, Key: `${process.env.stackName}/workflows/buckets.json`, Body: JSON.stringify(buckets) }).promise();

  await aws.s3().createBucket({ Bucket: buckets.public.name }).promise();
  const newGranule = fakeGranuleFactory();
  const metadata = fs.createReadStream('tests/data/meta.xml');

  newGranule.files = [
    {
      bucket,
      name: `${newGranule.granuleId}.txt`,
      filepath: `${process.env.stackName}/original_filepath/${newGranule.granuleId}.txt`,
      filename: `s3://${bucket}/${process.env.stackName}/original_filepath/${newGranule.granuleId}.txt`
    },
    {
      bucket: buckets.public.name,
      name: `${newGranule.granuleId}.cmr.xml`,
      filepath: `${process.env.stackName}/original_filepath/${newGranule.granuleId}.cmr.xml`,
      filename: `s3://${buckets.public.name}/${process.env.stackName}/original_filepath/${newGranule.granuleId}.cmr.xml`
    }
  ];

  await g.create(newGranule);

  await Promise.all(newGranule.files.map((file) => {
    if (file.name === `${newGranule.granuleId}.txt`) {
      return aws.s3().putObject({ Bucket: file.bucket, Key: file.filepath, Body: 'test data' }).promise();
    }
    return aws.s3().putObject({ Bucket: file.bucket, Key: file.filepath, Body: metadata }).promise();
  }));

  const destinationFilepath = `${process.env.stackName}/moved_granules`;
  const destinations = [
    {
      regex: '.*.txt$',
      bucket,
      filepath: destinationFilepath
    }
  ];

  const event = {
    httpMethod: 'PUT',
    pathParameters: {
      granuleName: newGranule.granuleId
    },
    headers: t.context.authHeaders,
    body: JSON.stringify({
      action: 'move',
      destinations
    })
  };

  sinon.stub(
    CMR.prototype,
    'ingestGranule'
  ).returns({ result: { 'concept-id': 'id204842' } });

  const response = await granuleEndpoint(event);

  const body = JSON.parse(response.body);
  t.is(body.status, 'SUCCESS');
  t.is(body.action, 'move');

  const list = await aws.s3().listObjects({ Bucket: bucket, Prefix: destinationFilepath }).promise();
  t.is(list.Contents.length, 1);
  list.Contents.forEach((item) => {
    t.is(item.Key.indexOf(destinationFilepath), 0);
  });

  const list2 = await aws.s3().listObjects({ Bucket: buckets.public.name, Prefix: `${process.env.stackName}/original_filepath` }).promise();
  t.is(list2.Contents.length, 1);
  t.is(newGranule.files[1].filepath, list2.Contents[0].Key);

  const file = await aws.s3().getObject({ Bucket: buckets.public.name, Key: newGranule.files[1].filepath }).promise();
  await aws.recursivelyDeleteS3Bucket(buckets.public.name);
  return new Promise((resolve, reject) => {
    xml2js.parseString(file.Body, xmlParseOptions, (err, data) => {
      if (err) return reject(err);
      return resolve(data);
    });
  }).then((xml) => {
    const newUrl = xml.Granule.OnlineAccessURLs.OnlineAccessURL[0].URL;
    const newDestination = `${process.env.DISTRIBUTION_ENDPOINT}${destinations[0].bucket}/${destinations[0].filepath}/${newGranule.files[0].name}`;
    t.is(newUrl, newDestination);
  });
});
