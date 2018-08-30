'use strict';

const sinon = require('sinon');
const proxyquire = require('proxyquire');
const test = require('ava');
const JSFtp = require('jsftp');
const { ftpMixin: TestFtpMixin } = require('../ftp');
const {
  checksumS3Objects, fileExists, recursivelyDeleteS3Bucket, s3
} = require('@cumulus/common/aws');
const { randomString } = require('@cumulus/common/test-utils');

class MyTestDiscoveryClass {
  constructor(useList) {
    this.decrypted = true;
    this.host = '127.0.0.1';
    this.password = 'testpass';
    this.path = '/';
    this.provider = { encrypted: false };
    this.useList = useList;
    this.username = 'testuser';
  }
}

test('useList is present and true when assigned', async (t) => {
  const jsftpSpy = sinon.spy(JSFtp);
  const { ftpMixin } = proxyquire('../ftp', {
    jsftp: jsftpSpy
  });

  class MyTestFtpDiscoveryClass extends ftpMixin(MyTestDiscoveryClass) {}
  const myTestFtpDiscoveryClass = new MyTestFtpDiscoveryClass(true);

  await myTestFtpDiscoveryClass.list();

  t.is(jsftpSpy.callCount, 1);
  t.is(jsftpSpy.getCall(0).args[0].useList, true);
});

test('useList defaults to false when not assigned', async (t) => {
  const jsftpSpy = sinon.spy(JSFtp);
  const { ftpMixin } = proxyquire('../ftp', {
    jsftp: jsftpSpy
  });

  class MyTestFtpDiscoveryClass extends ftpMixin(MyTestDiscoveryClass) {}
  const myTestFtpDiscoveryClass = new MyTestFtpDiscoveryClass();

  await myTestFtpDiscoveryClass.list();

  t.is(jsftpSpy.callCount, 1);
  t.is(jsftpSpy.getCall(0).args[0].useList, false);
});

test('Download remote file to s3', async (t) => {
  class MyTestFtpDiscoveryClass extends TestFtpMixin(MyTestDiscoveryClass) {}
  const myTestFtpDiscoveryClass = new MyTestFtpDiscoveryClass();
  const bucket = randomString();
  const key = randomString();
  try {
    await s3().createBucket({ Bucket: bucket }).promise();
    await myTestFtpDiscoveryClass.sync(
      '/granules/MOD09GQ.A2017224.h27v08.006.2017227165029.hdf', bucket, key
    );
    t.truthy(fileExists(bucket, key));
    const sum = await checksumS3Objects('CKSUM', bucket, key);
    t.is(sum, 1435712144);
  }
  finally {
    await recursivelyDeleteS3Bucket(bucket);
  }
});
