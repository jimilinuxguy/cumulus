'use strict';

const errors = require('@cumulus/common/errors');
const fs = require('fs-extra');
const test = require('ava');

const { recursivelyDeleteS3Bucket, s3 } = require('@cumulus/common/aws');

const { CollectionConfigStore } = require('@cumulus/common');
const {
  randomString,
  validateConfig,
  validateInput,
  validateOutput
} = require('@cumulus/common/test-utils');

const { parsePdr } = require('../index');

test.beforeEach(async (t) => {
  t.context.payload = {
    config: {
      stack: randomString(),
      bucket: randomString(),
      provider: {}
    },
    input: {
      pdr: {
        name: 'MOD09GQ.PDR',
        path: '/pdrs'
      }
    }
  };

  await s3().createBucket({ Bucket: t.context.payload.config.bucket }).promise();

  const collectionConfig = {
    name: 'MOD09GQ',
    granuleIdExtraction: '^(.*)\.hdf'
  };

  t.context.collectionConfigStore = new CollectionConfigStore(
    t.context.payload.config.bucket,
    t.context.payload.config.stack
  );
  await t.context.collectionConfigStore.put('MOD09GQ', '006', collectionConfig);
});

test.afterEach(async (t) => {
  await recursivelyDeleteS3Bucket(t.context.payload.config.bucket);
});

test.serial('parse PDR from FTP endpoint', async (t) => {
  t.context.payload.config.provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: '127.0.0.1',
    username: 'testuser',
    password: 'testpass'
  };
  t.context.payload.config.useList = true;

  await validateInput(t, t.context.payload.input);
  await validateConfig(t, t.context.payload.config);

  let output;
  try {
    output = await parsePdr(t.context.payload);

    await validateOutput(t, output);

    t.deepEqual(output.pdr, t.context.payload.input.pdr);
    t.is(output.granules.length, 1);
    t.is(output.granulesCount, 1);
    t.is(output.filesCount, 2);
    t.is(output.totalSize, 17909733);

    const granule = output.granules[0];
    t.is(granule.granuleId, 'MOD09GQ.A2017224.h09v02.006.2017227165020');
    t.is(granule.dataType, 'MOD09GQ');
    t.is(granule.granuleSize, 17909733);

    const hdfFile = granule.files.find((f) =>
      f.name === 'MOD09GQ.A2017224.h09v02.006.2017227165020.hdf');
    t.truthy(hdfFile);
    t.is(hdfFile.path, '/MODOPS/MODAPS/EDC/CUMULUS/FPROC/DATA');
    t.is(hdfFile.fileSize, 17865615);
    t.is(hdfFile.checksumType, 'CKSUM');
    t.is(hdfFile.checksumValue, 4208254019);

    const metFile = granule.files.find((f) =>
      f.name === 'MOD09GQ.A2017224.h09v02.006.2017227165020.hdf.met');
    t.truthy(metFile);
    t.is(metFile.path, '/MODOPS/MODAPS/EDC/CUMULUS/FPROC/DATA');
    t.is(metFile.fileSize, 44118);
  }
  catch (err) {
    if (err instanceof errors.RemoteResourceError || err.code === 'AllAccessDisabled') {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else t.fail(err);
  }
});

test.serial('parse PDR from HTTP endpoint', async (t) => {
  t.context.payload.config.provider = {
    id: 'MODAPS',
    protocol: 'http',
    host: 'http://127.0.0.1:3030'
  };

  await validateInput(t, t.context.payload.input);
  await validateConfig(t, t.context.payload.config);

  let output;
  try {
    output = await parsePdr(t.context.payload);

    await validateOutput(t, output);

    t.deepEqual(output.pdr, t.context.payload.input.pdr);
    t.is(output.granules.length, 1);
    t.is(output.granulesCount, 1);
    t.is(output.filesCount, 2);
    t.is(output.totalSize, 17909733);

    const granule = output.granules[0];
    t.is(granule.granuleId, 'MOD09GQ.A2017224.h09v02.006.2017227165020');
    t.is(granule.dataType, 'MOD09GQ');
    t.is(granule.granuleSize, 17909733);

    const hdfFile = granule.files.find((f) =>
      f.name === 'MOD09GQ.A2017224.h09v02.006.2017227165020.hdf');
    t.truthy(hdfFile);
    t.is(hdfFile.path, '/MODOPS/MODAPS/EDC/CUMULUS/FPROC/DATA');
    t.is(hdfFile.fileSize, 17865615);
    t.is(hdfFile.checksumType, 'CKSUM');
    t.is(hdfFile.checksumValue, 4208254019);

    const metFile = granule.files.find((f) =>
      f.name === 'MOD09GQ.A2017224.h09v02.006.2017227165020.hdf.met');
    t.truthy(metFile);
    t.is(metFile.path, '/MODOPS/MODAPS/EDC/CUMULUS/FPROC/DATA');
    t.is(metFile.fileSize, 44118);
  }
  catch (err) {
    if (err instanceof errors.RemoteResourceError || err.code === 'AllAccessDisabled') {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else t.fail(err);
  }
});

test.serial('parse PDR from SFTP endpoint', async (t) => {
  t.context.payload.config.provider = {
    id: 'MODAPS',
    protocol: 'sftp',
    host: '127.0.0.1',
    port: 2222,
    username: 'user',
    password: 'password'
  };

  await validateInput(t, t.context.payload.input);
  await validateConfig(t, t.context.payload.config);

  let output;
  try {
    output = await parsePdr(t.context.payload);

    await validateOutput(t, output);

    t.deepEqual(output.pdr, t.context.payload.input.pdr);
    t.is(output.granules.length, 1);
    t.is(output.granulesCount, 1);
    t.is(output.filesCount, 2);
    t.is(output.totalSize, 17909733);

    const granule = output.granules[0];
    t.is(granule.granuleId, 'MOD09GQ.A2017224.h09v02.006.2017227165020');
    t.is(granule.dataType, 'MOD09GQ');
    t.is(granule.granuleSize, 17909733);

    const hdfFile = granule.files.find((f) =>
      f.name === 'MOD09GQ.A2017224.h09v02.006.2017227165020.hdf');
    t.truthy(hdfFile);
    t.is(hdfFile.path, '/MODOPS/MODAPS/EDC/CUMULUS/FPROC/DATA');
    t.is(hdfFile.fileSize, 17865615);
    t.is(hdfFile.checksumType, 'CKSUM');
    t.is(hdfFile.checksumValue, 4208254019);

    const metFile = granule.files.find((f) =>
      f.name === 'MOD09GQ.A2017224.h09v02.006.2017227165020.hdf.met');
    t.truthy(metFile);
    t.is(metFile.path, '/MODOPS/MODAPS/EDC/CUMULUS/FPROC/DATA');
    t.is(metFile.fileSize, 44118);
  }
  catch (err) {
    if (err instanceof errors.RemoteResourceError || err.code === 'AllAccessDisabled') {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else t.fail(err);
  }
});

test.serial('Parse a PDR from an S3 provider', async (t) => {
  t.context.payload.config.provider = {
    id: 'MODAPS',
    protocol: 's3',
    host: randomString()
  };
  t.context.payload.input.pdr.path = '/pdrs';

  await validateInput(t, t.context.payload.input);
  await validateConfig(t, t.context.payload.config);

  await s3().createBucket({ Bucket: t.context.payload.config.provider.host }).promise();

  try {
    await s3().putObject({
      Bucket: t.context.payload.config.provider.host,
      Key: `${t.context.payload.input.pdr.path}/${t.context.payload.input.pdr.name}`,
      Body: fs.createReadStream('../../packages/test-data/pdrs/MOD09GQ.PDR')
    }).promise();

    const output = await parsePdr(t.context.payload);

    await validateOutput(t, output);

    t.deepEqual(output.pdr, t.context.payload.input.pdr);
    t.is(output.granules.length, 1);
    t.is(output.granulesCount, 1);
    t.is(output.filesCount, 2);
    t.is(output.totalSize, 17909733);

    const granule = output.granules[0];
    t.is(granule.granuleId, 'MOD09GQ.A2017224.h09v02.006.2017227165020');
    t.is(granule.dataType, 'MOD09GQ');
    t.is(granule.granuleSize, 17909733);

    const hdfFile = granule.files.find((f) =>
      f.name === 'MOD09GQ.A2017224.h09v02.006.2017227165020.hdf');
    t.truthy(hdfFile);
    t.is(hdfFile.path, '/MODOPS/MODAPS/EDC/CUMULUS/FPROC/DATA');
    t.is(hdfFile.fileSize, 17865615);
    t.is(hdfFile.checksumType, 'CKSUM');
    t.is(hdfFile.checksumValue, 4208254019);

    const metFile = granule.files.find((f) =>
      f.name === 'MOD09GQ.A2017224.h09v02.006.2017227165020.hdf.met');
    t.truthy(metFile);
    t.is(metFile.path, '/MODOPS/MODAPS/EDC/CUMULUS/FPROC/DATA');
    t.is(metFile.fileSize, 44118);
  }
  catch (err) {
    if (err instanceof errors.RemoteResourceError || err.code === 'AllAccessDisabled') {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else t.fail(err);
  }
  finally {
    await recursivelyDeleteS3Bucket(t.context.payload.config.provider.host);
  }
});

test.serial('Parse a PDR without a granuleIdFilter in the config', async (t) => {
  // Create the collections contained in this PDR
  await Promise.all([
    t.context.collectionConfigStore.put(
      'MYG29_S1D_SIR', '006',
      { name: 'MYG29_S1D_SIR', granuleIdExtraction: '^(.*)\.tar.gz' }
    ),
    t.context.collectionConfigStore.put(
      'MYG29_N1D_SIR', '006',
      { name: 'MYG29_N1D_SIR', granuleIdExtraction: '^(.*)\.tar.gz' }
    )
  ]);

  // Set up the task config
  t.context.payload.config.provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: '127.0.0.1',
    username: 'testuser',
    password: 'testpass'
  };
  t.context.payload.config.useList = true;

  // Set up the task input
  t.context.payload.input.pdr.name = 'MODAPSops7.1234567.PDR';

  await validateInput(t, t.context.payload.input);
  await validateConfig(t, t.context.payload.config);

  let output;
  try {
    output = await parsePdr(t.context.payload);

    await validateOutput(t, output);

    t.deepEqual(output.pdr, t.context.payload.input.pdr);
    t.is(output.granules.length, 2);
    t.is(output.granulesCount, 2);
    t.is(output.filesCount, 2);
    t.is(output.totalSize, 3952643);
  }
  catch (err) {
    if (err instanceof errors.RemoteResourceError || err.code === 'AllAccessDisabled') {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else t.fail(err);
  }
});

test.serial('Empty FILE_ID valule in PDR, parse-pdr throws error', async (t) => {
  t.context.payload.config.provider = {
    id: 'MODAPS',
    protocol: 's3',
    host: randomString()
  };
  t.context.payload.input.pdr.path = '/pdrs';

  await validateInput(t, t.context.payload.input);
  await validateConfig(t, t.context.payload.config);

  await s3().createBucket({ Bucket: t.context.payload.config.provider.host }).promise();

  let output;
  try {
    await s3().putObject({
      Bucket: t.context.payload.config.provider.host,
      Key: `${t.context.payload.input.pdr.path}/${t.context.payload.input.pdr.name}`,
      Body: fs.createReadStream('../../packages/test-data/pdrs/MOD09GQ-without-file-id-value.PDR')
    }).promise();

    await t.throws(parsePdr(t.context.payload), "Failed to parse value ('') of FILE_ID", 'Value corresponding to FILE_ID key in the PDR is empty');
  }
  catch(err) {
    if (err instanceof errors.RemoteResourceError || err.code === 'AllAccessDisabled') {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else t.fail(err);
  }
})

test.serial('Missing FILE_ID in PDR, parse-pdr throws error', async (t) => {
  t.context.payload.config.provider = {
    id: 'MODAPS',
    protocol: 's3',
    host: randomString()
  };
  t.context.payload.input.pdr.path = '/pdrs';

  await validateInput(t, t.context.payload.input);
  await validateConfig(t, t.context.payload.config);

  await s3().createBucket({ Bucket: t.context.payload.config.provider.host }).promise();

  let output;
  try {
    await s3().putObject({
      Bucket: t.context.payload.config.provider.host,
      Key: `${t.context.payload.input.pdr.path}/${t.context.payload.input.pdr.name}`,
	  Body: fs.createReadStream('../../packages/test-data/pdrs/MOD09GQ-without-file-id.PDR')
    }).promise();

    await t.throws(parsePdr(t.context.payload), 'FILE_ID', 'FILE_ID Key is not present in the supplied PDR');
  }
  catch(err) {
    if (err instanceof errors.RemoteResourceError || err.code === 'AllAccessDisabled') {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else t.fail(err);
  }
})

test.serial('Parse a PDR with a granuleIdFilter in the config', async (t) => {
  // Create the collections contained in this PDR
  await Promise.all([
    t.context.collectionConfigStore.put(
      'MYG29_S1D_SIR', '006',
      { name: 'MYG29_S1D_SIR', granuleIdExtraction: '^(.*)\.tar.gz' }
    ),
    t.context.collectionConfigStore.put(
      'MYG29_N1D_SIR', '006',
      { name: 'MYG29_N1D_SIR', granuleIdExtraction: '^(.*)\.tar.gz' }
    )
  ]);

  // Set up the task config
  t.context.payload.config.provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: '127.0.0.1',
    username: 'testuser',
    password: 'testpass'
  };
  t.context.payload.config.useList = true;
  t.context.payload.config.granuleIdFilter = '^MYG29_S1D_SIR.A2012254.tiled.006.2018082201326\..*';

  // Set up the task input
  t.context.payload.input.pdr.name = 'MODAPSops7.1234567.PDR';

  await validateInput(t, t.context.payload.input);
  await validateConfig(t, t.context.payload.config);

  let output;
  try {
    output = await parsePdr(t.context.payload);

    await validateOutput(t, output);

    t.deepEqual(output.pdr, t.context.payload.input.pdr);
    t.is(output.granules.length, 1);
    t.is(output.granulesCount, 1);
    t.is(output.filesCount, 1);
    t.is(output.totalSize, 1503297);
  }
  catch (err) {
    if (err instanceof errors.RemoteResourceError || err.code === 'AllAccessDisabled') {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else t.fail(err);
  }
});
