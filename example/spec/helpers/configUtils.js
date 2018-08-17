'use strict';

const fs = require('fs-extra');
const yaml = require('js-yaml');

const { extend } = require('lodash');

/**
 * Copy a configuration file to a backup location
 *
 * @param {string} configurationYmlFilepath - configuration file path
 * @param {string} configurationYmlBackupFilepath - backup configuration file path
 * @returns {undefined} none
 */
function backupConfigYml(configurationYmlFilepath, configurationYmlBackupFilepath) {
  fs.copyFileSync(configurationYmlFilepath, configurationYmlBackupFilepath);
}

/**
 * Copy a configuration file back from the backup location. Delete
 * the backup configuration file
 *
 * @returns {undefined} none
 * @param {string} configurationYmlFilepath - configuration file path
 * @param {string} configurationYmlBackupFilename - backup configuration file path
 */
function restoreConfigYml(configurationYmlFilepath, configurationYmlBackupFilename) {
  fs.copyFileSync(configurationYmlBackupFilename, configurationYmlFilepath);
  fs.unlinkSync(configurationYmlBackupFilename);
}

/**
 * Load a configuration yml file
 *
 * @param {string} workflowConfigFile - workflow yml file,defaults to './workflows.yml'
 * @returns {Object} - JS Object representation of yml file
 */
function loadYmlConfigFile(workflowConfigFile) {
  return yaml.safeLoad(fs.readFileSync(workflowConfigFile, 'utf8'));
}

/**
 * Convert config JS to yml
 *
 * @param {Object} configJs - configuration as a JS object
 * @param {string} filepath - file path to save to
 * @returns {undefined} None
 */
function saveYmlConfigFile(configJs, filepath) {
  const configYaml = yaml.safeDump(configJs);
  fs.writeFileSync(filepath, configYaml);
}

/**
 * Returns configuration object for entire configuration yml, or the top-level node
 * specified
 *
 * @param {string} configFilepath -config file path
 * @param {string} nodeName - workflow name
 * @returns {Object} return the workflow configuration
 */
function getConfigObject(configFilepath, nodeName) {
  const config = loadYmlConfigFile(configFilepath);
  if (nodeName) return config[nodeName];
  return config;
}

/**
 * Updates configuration file node with values specified in configJson*
 * @param {string} configFilePath -config file path
 * @param {string} nodeName - top-level-node name
 * @param {Object} configJson - JSON object to append/overwrite target key's values with
 * @returns {Object} return the workflow configuration
 */

function updateConfigObject(configFilePath, nodeName, configJson) {
  const config = loadYmlConfigFile(configFilePath);
  extend(config[nodeName], configJson);
  saveYmlConfigFile(config, configFilePath);
}

module.exports = {
  getConfigObject,
  backupConfigYml,
  restoreConfigYml,
  loadYmlConfigFile,
  saveYmlConfigFile,
  updateConfigObject
};
