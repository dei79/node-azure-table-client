var azure               = require("azure-storage");

function AzureTableClientDefaultBackend() {
    var self = this;

    var azureTableService = undefined;

    self.connect = function(storageAccountKey, storageAccountSecret, storageAccountEndpointSuffix) {
        azureTableService = new azure.TableService(storageAccountKey, storageAccountSecret, undefined, undefined, storageAccountEndpointSuffix);
    }

    self.createTableIfNotExists = function(tableName, callback) {
        azureTableService.createTableIfNotExists(tableName, callback)
    }

    self.executeBatch = function(tableName, batch, callback) {
        azureTableService.executeBatch(tableName, batch, callback)
    }

    self.queryEntities = function(tableName, tableQuery, currentToken, optionsOrCallback, callback) {
        azureTableService.queryEntities(tableName, tableQuery, currentToken, optionsOrCallback, callback);
    }
};


exports.AzureTableClientDefaultBackend = AzureTableClientDefaultBackend;