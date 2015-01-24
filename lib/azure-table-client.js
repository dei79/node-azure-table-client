var azure               = require("azure-storage");
var q                   = require('q');

var storeMode = {
    insert: 0,
    merge:  1
};

/*
 * Contains the configuration to the storage account which can be used
 * to store the data
 */
var azureTableService = undefined;

/*
 * Checks if the given object is null or undefined
 */
function isNullOrUndefined(obj) {
    return obj === null || obj === undefined;
}

/*
 * Checks if the given key marked as a system key which means nobody is using
 * them during model conversion.
 */
function isSystemKey(key) {
    return (key === 'PartitionKey' || key === 'RowKey' || key === 'Timestamp' || key === '.metadata' || key === 'QueryMapping');
}

function isTableName(key) {
    return (key === 'TableName');
}
/*
 * The AzureModel is the shim class the framework generates when
 * someone builds a new model according the definitions and the
 * default value.
 */
function AzureModel(definition, definitionObject, defaults) {
    var self = this;

    // generate our system functions and take over teh reference
    // from the definition
    self.PartitionKey   = definition.PartitionKey;
    self.RowKey         = definition.RowKey;

    // Generate the properties in the object according the
    // object definition.
    var keys = Object.keys(definition);
    keys.forEach(function(key) {

        // check system keys
        if (isTableName(key)) {
            return;
        }

        // check if we have a default
        if (defaults[key] !== undefined) {
            self[key] = defaults[key];
        // check if the property at all is missing
        } else if (self[key] === undefined) {
            self[key] = undefined;
        }
    });

    /*
     * This method inserts or merges the model into the storage. All
     * properties which are undefined will not be part of the save/merge
     * array
     */
    self.insert = function() {
        return definitionObject.store([self], storeMode.insert);
    };

    self.merge = function() {
        return definitionObject.store([self], storeMode.merge);
    }
}

/*
 * The AzureModelDefinition is the helper which is indirectly used
 * to define the models as a simple DSL.
 */
function AzureModelDefinition(definition) {
    var self = this;

    // converts a value from the azure entity in our current type. This is necessary
    // because the Azure SDK has some issues with handling double in interger format, ...
    function convertEntityValueToValidType(targetType, currentValue ) {

        if (targetType === Number) {
            if (typeof currentValue == 'string') {
                return parseFloat(currentValue);
            } else {
                return currentValue;
            }
        } else {
            return currentValue;
        }

    }

    // Just a thin wrapper to make it easier to test this function
    self.azureCalls = {
        createTableIfNotExists: function (tableName, callback) {
            azureTableService.createTableIfNotExists(tableName, callback)
        },

        executeBatch: function (tableName, batch, callback) {
            azureTableService.executeBatch(tableName, batch, callback)
        },

        queryEntities: function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
            azureTableService.queryEntities(tableName, tableQuery, currentToken, optionsOrCallback, callback);
        }
    };

    self.build = function(defaults) {
        return new AzureModel(definition, self, isNullOrUndefined(defaults) ? {} : defaults);
    };

    self.store = function(models, mode) {
        var deferred = q.defer();

        var operationBatches = [];
        var currentOperationsBatch = undefined;

        // calculate the table name
        var tableName = definition.TableName();

        models.forEach(function(model) {

            // calculate the partition key
            var partitionKey = model.PartitionKey(model);

            // calculate the row key
            var rowKey = model.RowKey(model);

            // build the generator
            var entGen = azure.TableUtilities.entityGenerator;

            // generate the task
            var task = {
                PartitionKey: entGen.String(partitionKey),
                RowKey: entGen.String(rowKey)
            };

            // add the dynamic properties to the task
            var keys = Object.keys(definition);
            keys.forEach(function(key) {

                // filter out the system keys
                if (isSystemKey(key)) {
                    return;
                }

                // filter out not defined properties
                if (model[key] === undefined) {
                    return;
                }

                // convert into the right entity type
                var keyType = definition[key];

                if (keyType === Number) {
                    task[key] = entGen.Double(model[key]);
                } else if (keyType == Boolean) {
                    task[key] = entGen.Boolean(model[key]);
                } else {
                    task[key] = entGen.String(model[key]);
                }
            });

            // check if we need to create a batch
            if (currentOperationsBatch === undefined) {
                currentOperationsBatch = new azure.TableBatch();
                operationBatches.push(currentOperationsBatch);
            }

            // add the operation to the batch
            if (mode == storeMode.insert) {
                currentOperationsBatch.insertOrReplaceEntity(task);
            } else {
                currentOperationsBatch.insertOrMergeEntity(task);
            }

            // check if we need to switch to a new batch
            if (currentOperationsBatch.operations.length == 100) {
                currentOperationsBatch = undefined;
            }
        });

        // ensure the table exists
        self.azureCalls.createTableIfNotExists(tableName, function(error, result, response) {

            // check if we was able to create the table
            if (!isNullOrUndefined(error)) {
                deferred.reject(error);
                return;
            }

            // go through all batches and perform operation
            var promisses = [];
            operationBatches.forEach(function(batch) {
                var defer = q.defer();

                self.azureCalls.executeBatch(tableName, batch,  function (error, result, response) {

                    if (isNullOrUndefined(error)) {
                        defer.resolve();
                    } else {
                        defer.reject(error);
                    }
                });

                promisses.push(defer.promise);
            });

            // wait for all web calls and close the promises
            q.allSettled(promisses).then(function(results) {

                var rejected = undefined;

                results.forEach(function(result) {
                    if (result.state !== 'fulfilled') {
                        rejected = result.value;
                        return;
                    }
                });

                if (rejected === undefined) {
                    deferred.resolve();
                } else {
                    deferred.reject(rejected);
                }
            })

        });

        // return the promise
        return deferred.promise;
    };

    self.insert = function(models) {
        return self.store(models, storeMode.insert)
    };

    self.merge = function(models) {
        return self.store(models, storeMode.merge)
    };

    self.query = function(partitionKey, rowKey) {
        var deferred = q.defer();

        var query = null;

        // build the query
        if (partitionKey && rowKey) {
            query = new azure.TableQuery().where('PartitionKey eq ? and RowKey eq ?', partitionKey, rowKey);
        } else if (partitionKey && !rowKey) {
            query = new azure.TableQuery().where('PartitionKey eq ?', partitionKey);
        } else if (!partitionKey && rowKey) {
            query = new azure.TableQuery().where('RowKey eq ?', rowKey);
        } else {
            query = new azure.TableQuery();
        }

        // calculate the table name
        var tableName = definition.TableName();

        // query the data
        self.azureCalls.queryEntities(tableName, query, null, function(error, result, response) {

            if (error !== null) {
                deferred.reject(error);
            } else {

                // convert the entries
                var resultObjects = [];
                result.entries.forEach(function(entry) {

                    var defaultsObject = {};

                    // generate the defaults object
                    Object.keys(entry).forEach(function(key) {

                        // filter out the system keys
                        if (isSystemKey(key)) {
                            return;
                        }

                        // add to defaults
                        defaultsObject[key] = convertEntityValueToValidType(definition[key], entry[key]._);
                    });

                    // go through the query mapping if exists
                    if (definition.QueryMapping !== undefined) {

                        Object.keys(definition.QueryMapping).forEach(function(mappingKey) {
                            var sourceKey = definition.QueryMapping[mappingKey];
                            defaultsObject[mappingKey] = convertEntityValueToValidType(definition[mappingKey], entry[sourceKey]._);
                        })
                    }

                    // build a new model
                    resultObjects.push(self.build(defaultsObject))
                });

                deferred.resolve(resultObjects);
            }
        });

        return deferred.promise;
    }
}

/*
 * Exporting the model definition
 */
exports.define = function(definition) {
    return new AzureModelDefinition(definition);
};

/*
 * Exporting the config operations
 */
exports.config = function(storageAccountKey, storageAccountSecret) {
    azureTableService = azure.createTableService(storageAccountKey, storageAccountSecret);
};