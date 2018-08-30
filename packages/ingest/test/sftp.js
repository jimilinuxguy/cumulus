'use strict';

const fs = require('fs');
const sinon = require('sinon');
const proxyquire = require('proxyquire');
const test = require('ava');
const JSFtp = require('jsftp');
const { sftpMixin: TestSftpMixin } = require('../sftp');
const {
  checksumS3Objects, fileExists, recursivelyDeleteS3Bucket, s3
} = require('@cumulus/common/aws');
const {
  randomString
} = require('@cumulus/common/test-utils');

const privateKey = 'ssh_client_rsa_key';
const bucket = randomString();
const stackName = randomString();

process.env.internal = bucket;
process.env.stackName = stackName;

class MyTestDiscoveryClass {
  constructor(useList) {
    this.decrypted = true;
    this.host = '127.0.0.1';
    this.port = '2222';
    this.username = 'user';
    this.path = '/pdrs';
    this.provider = {
      encrypted: false,
      privateKey: privateKey
    };
    this.useList = useList;
  }
}

test.before(async () => {
  // let's copy the key to s3
  await s3().createBucket({ Bucket: bucket }).promise();

  const privKey = fs.readFileSync(`../test-data/keys/${privateKey}`, 'utf-8');

  await s3().putObject({
    Bucket: bucket,
    Key: `${stackName}/crypto/${privateKey}`,
    Body: privKey
  }).promise();
});

test.after.always(async () => {
  await Promise.all([
    recursivelyDeleteS3Bucket(bucket)
  ]);
});

test('connect and retrieve list of pdrs', async (t) => {

  const jsftpSpy = sinon.spy(JSFtp);
  const { sftpMixin } = proxyquire('../sftp', {
    jsftp: jsftpSpy
  });

  class MyTestSftpDiscoveryClass extends sftpMixin(MyTestDiscoveryClass) {}
  const myTestSftpDiscoveryClass = new MyTestSftpDiscoveryClass(true);
  const list = await myTestSftpDiscoveryClass.list();
  t.is(list.length > 0, true);
});

test('Download remote file to s3', async (t) => {
  class MyTestSftpDiscoveryClass extends TestSftpMixin(MyTestDiscoveryClass) {}
  const myTestSftpDiscoveryClass = new MyTestSftpDiscoveryClass(true);

  const key = randomString();
  await myTestSftpDiscoveryClass.sync(
    '/granules/MOD09GQ.A2017224.h27v08.006.2017227165029.hdf', bucket, key
  );
  t.truthy(fileExists(bucket, key));
  const sum = await checksumS3Objects('CKSUM', bucket, key);
  t.is(sum, 1435712144);
});
