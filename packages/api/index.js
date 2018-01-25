'use strict';

exports.token = require('./endpoints/token');
exports.collections = require('./endpoints/collections');
exports.granules = require('./endpoints/granules');
exports.logs = require('./endpoints/logs');
exports.pdrs = require('./endpoints/pdrs');
exports.providers = require('./endpoints/providers');
exports.rules = require('./endpoints/rules');
exports.workflows = require('./endpoints/workflows');
exports.executions = require('./endpoints/executions');
exports.executionStatus = require('./endpoints/execution-status');
exports.schemas = require('./endpoints/schemas');
exports.stats = require('./endpoints/stats');
exports.version = require('./endpoints/version');
exports.distribution = require('./endpoints/distribution');

exports.jobs = require('./lambdas/jobs');
exports.bootstrap = require('./lambdas/bootstrap');
exports.scheduler = require('./lambdas/sf-scheduler');
exports.starter = require('./lambdas/sf-starter');
exports.queue = require('./lambdas/queue');

const indexer = require('./es/indexer');
const broadcast = require('./lambdas/sf-sns-broadcast');

exports.sfStart = broadcast.start;
exports.sfEnd = broadcast.end;
exports.indexer = indexer.handler;
exports.logHandler = indexer.logHandler;
