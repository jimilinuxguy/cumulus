'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');

/**
 * Synchronously makes a temporary directory, smoothing over the differences between
 * mkdtempSync in node.js for various platforms and versions
 *
 * @param {string} name - A base name for the temp dir, to be uniquified for the final name
 * @returns {string} - The absolute path to the created dir
 */
exports.mkdtempSync = (name) => {
  const dirname = ['gitc', name, +new Date()].join('_');
  const abspath = path.join(os.tmpdir(), dirname);
  fs.mkdirSync(abspath, 0o700);
  return abspath;
};

/**
 * Generate and return an RFC4122 v4 UUID.
 * @return - An RFC44122 v4 UUID.
 */
exports.uuid = require('uuid/v4');

/**
 * Does nothing.  Used where a callback is required but not used.
 *
 * @returns {undefined} undefined
 */
exports.noop = () => {}; // eslint-disable-line lodash/prefer-noop
