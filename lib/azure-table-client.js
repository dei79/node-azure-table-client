var azure               = require("azure-storage");
var q                   = require('q');

var storeMode = {
    insert: 0,
    merge:  1,
    insertExclusive: 2,
    mergeExclusive: 3
};

// expose all possible storage providers
exports.AzureTableClientDefaultBackend  = require("./azure-table-client-default-backend.js").AzureTableClientDefaultBackend;
exports.AzureTableClientMockBackend  = require("./azure-table-client-mock-backend.js").AzureTableClientMockBackend;

var AzureTableClientHelpers = require('./azure-table-client-helpers.js').AzureTableClientHelpers;

function AzureTableClient() {
    var selfTableClient = this;

    /*
     * DEfines the data type we are supporting with special conversion
     */
    selfTableClient.DataTypes = {
        DateTime: 'datetime',
        Int32: 'int32',
        Int64: 'int64',
        Boolean: 'boolean',
        String: 'string',
        Guid: 'guid',
        Double:'double',
    }

    /*
     * Loads the standard storage provider which is able to communicate to azure table store.
     */
    var activeStorageProvider = new exports.AzureTableClientDefaultBackend();

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
        self.PartitionKey = definition.PartitionKey;
        self.RowKey = definition.RowKey;

        // Generate the properties in the object according the
        // object definition.
        var keys = Object.keys(definition);
        keys.forEach(function (key) {

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
        self.insert = function (exclusive) {
            return definitionObject.store([self], exclusive ? storeMode.insertExclusive : storeMode.insert);
        };

        self.merge = function (exclusive) {
            return definitionObject.store([self], exclusive ? storeMode.mergeExclusive : storeMode.merge);
        };

        /*
         * Allows to delete a singel instance
         */
        self.delete = function () {
            return definitionObject.delete([self]);
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
        function convertEntityValueToValidType(targetType, currentValue) {

            if (targetType === Number || targetType === selfTableClient.DataTypes.Int32 || targetType === selfTableClient.DataTypes.Int64 || targetType === selfTableClient.DataTypes.Double) {
                if (typeof currentValue == 'string') {
                    return parseFloat(currentValue);
                } else {
                    return currentValue;
                }
            } else if (targetType === Array || targetType === Object) {
                if (currentValue === null || currentValue === undefined) {
                    return [];
                } else {
                    return JSON.parse(currentValue);
                }
            } else {
                return currentValue;
            }

        }

        function waitWithPromise(mseconds) {
            var defered = q.defer();

            setTimeout(function() {
                defered.resolve();
            }, mseconds);

            return defered.promise;
        }

        function getRandomInt(max) {
            return Math.floor(Math.random() * Math.floor(max));
        }

        function storeBatchNative(tableName, batch) {
            var defer = q.defer();

            self.azureCalls.executeBatch(tableName, batch, function (error, result, response) {

                if (isNullOrUndefined(error)) {
                    return defer.resolve();
                } else {
                    return defer.reject(error);
                }
            });

            return defer.promise;
        }

        function storeBatchIncludingBackoffStrategy(tableName, batch, run) {

            // ensure we have a valid value for run
            if (!run) { run = 0; }

            // execute our wait operation just in case
            var waitPromise = q.resolve();
            if (run > 0) {
                var waitMultiple = getRandomInt(5);
                waitPromise = waitWithPromise((waitMultiple * run) * 1000);
            }

            // wait
            return waitPromise.then(function() {

                // execute
                return storeBatchNative(tableName, batch).catch(function(error) {

                    // in case we have an error and the error messae is storage is bussy
                    // StorageError: The server is busy
                    if (error.message && error.message.indexOf("busy") != -1 && run <= 10) {
                        return storeBatchIncludingBackoffStrategy(tableName, batch, run + 1);
                    } else if (error.message && error.message.indexOf("busy") != -1 && run > 10) {
                        return q.reject(new Error("StorageError: The server is busy, retry count > 10, giving up!"));
                    } else {
                        return q.reject(error);
                    }
                });
            });
        }

        function mapResultObjects(result, maxCount, optionalDefaultValues) {

            // reduce the amount of elements if needed
            if (maxCount > 0 && result.entries.length > maxCount) {
                result.entries = result.entries.slice(0, maxCount);
            }

            // convert the entries
            var resultObjects = [];
            result.entries.forEach(function (entry) {

                var defaultsObject = {};

                // define the default values
                if (optionalDefaultValues !== null && optionalDefaultValues !== undefined) {
                    Object.keys(optionalDefaultValues).forEach(function (defaultValueKey) {
                        defaultsObject[defaultValueKey] = optionalDefaultValues[defaultValueKey];
                    })
                }

                // generate the defaults object
                Object.keys(entry).forEach(function (key) {

                    // filter out the system keys
                    if (isSystemKey(key)) {
                        return;
                    }

                    // add to defaults
                    defaultsObject[key] = convertEntityValueToValidType(definition[key], entry[key]._);
                });

                // go through the query mapping if exists
                if (definition.QueryMapping !== undefined) {

                    Object.keys(definition.QueryMapping).forEach(function (mappingKey) {
                        var sourceKey = definition.QueryMapping[mappingKey];
                        defaultsObject[mappingKey] = convertEntityValueToValidType(definition[mappingKey], entry[sourceKey]._);
                    })
                }

                // build a new model
                resultObjects.push(self.build(defaultsObject))
            });

            return resultObjects;
        }

        function buildQueryObject(partitionKey, rowKey) {

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

            return query;
        };


        // Just a thin wrapper to make it easier to test this function
        self.azureCalls = {
            createTableIfNotExists: function (tableName, callback) {
                activeStorageProvider.createTableIfNotExists(tableName, callback);
            },

            executeBatch: function (tableName, batch, callback) {
                activeStorageProvider.executeBatch(tableName, batch, callback);

            },

            queryEntities: function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
                activeStorageProvider.queryEntities(tableName, tableQuery, currentToken, optionsOrCallback, callback);
            }
        };

        self.build = function (defaults) {
            return new AzureModel(definition, self, isNullOrUndefined(defaults) ? {} : defaults);
        };


        function popNextBatch(models, mode) {

            // build the working batch
            var workingBatch = new azure.TableBatch();

            // pop 100 entries or all
            while(models.length > 0 && workingBatch.operations.length < 100) {

                // pop an element
                var model = models.shift();

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
                keys.forEach(function (key) {

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
                    } else if (keyType === Boolean) {
                        task[key] = entGen.Boolean(model[key]);
                    } else if (keyType === selfTableClient.DataTypes.DateTime) {
                        task[key] = entGen.DateTime(model[key]);
                    } else if (keyType === selfTableClient.DataTypes.Int32) {
                        task[key] = entGen.Int32(model[key]);
                    } else if (keyType === selfTableClient.DataTypes.Int64) {
                        task[key] = entGen.Int64(model[key]);
                    } else if (keyType === selfTableClient.DataTypes.Boolean) {
                        task[key] = entGen.Boolean(model[key]);
                    } else if (keyType === selfTableClient.DataTypes.String) {
                        task[key] = entGen.String(model[key]);
                    } else if (keyType === selfTableClient.DataTypes.Guid) {
                        task[key] = entGen.Guid(model[key]);
                    } else if (keyType === selfTableClient.DataTypes.Double) {
                        task[key] = entGen.Double(model[key]);
                    } else if (keyType === Array || keyType === Object) {
                        task[key] = entGen.String(JSON.stringify(model[key]));
                    } else {
                        task[key] = entGen.String(model[key]);
                    }
                });

                // check if we need to create a batch
                if (workingBatch === undefined) {
                    workingBatch = new azure.TableBatch();
                }

                // add the operation to the batch
                if (mode == storeMode.insert) {
                    workingBatch.insertOrReplaceEntity(task);
                } else if (mode == storeMode.insertExclusive) {
                    workingBatch.insertEntity(task);
                } else if (mode == storeMode.mergeExclusive) {
                    workingBatch.mergeEntity(task);
                } else {
                    workingBatch.insertOrMergeEntity(task);
                }
            }

            // return the work
            return q.resolve(workingBatch);
        }

        function writeBatch(tableName, writingBatch) {

            // try to write
            return storeBatchIncludingBackoffStrategy(tableName, writingBatch).catch(function(error) {

                if (error && error.message && error.message.indexOf('table specified does not exist') != -1) {

                    var deferred = q.defer();

                    self.azureCalls.createTableIfNotExists(tableName, function (tableCreationError, result, response) {

                        // check if we was able to create the table
                        if (!isNullOrUndefined(tableCreationError)) { return deferred.reject(tableCreationError); }

                        storeBatchIncludingBackoffStrategy(tableName, writingBatch).then(function() {
                            deferred.resolve();
                        }).catch(function(error) {
                            deferred.reject(error);
                        })
                    });

                    return deferred.promise;

                } else {
                    return q.reject(error);
                }
            });
        }

        function storeInternal(models, mode, chunkId, cbProgress, previousStoreOperation) {

            // check that we have a call progress
            if (!cbProgress) { cbProgress = function() {}; }

            // ensure we have a previous opp
            if (!previousStoreOperation) { previousStoreOperation = q.resolve(); }

            // wait for the previous operation
            return previousStoreOperation.then(function() {

                // generate our work copy of the models array
                var workingCopyOfModels = models.slice(0);

                // call progress
                cbProgress(chunkId, workingCopyOfModels);

                // just in case our copy model is empty
                if (workingCopyOfModels.length == 0) { return q.resolve(); }

                // pop the next page
                return popNextBatch(workingCopyOfModels, mode).then(function (nextWritingBatch) {

                    // calculate the table name
                    var tableName = definition.TableName();

                    // start the writing process
                    var writeOperationPromise = writeBatch(tableName, nextWritingBatch);

                    // do it again
                    return storeInternal(workingCopyOfModels, mode, chunkId, cbProgress, writeOperationPromise);
                });
            });
        };

        self.store = function (models, mode, cbProgress) {

            // split to get more paralism
            var paralismFactor = models.length > 100 ?  Math.min(Math.floor(models.length / 100), 4) : 1;
            var chunkSize = Math.floor(models.length / paralismFactor) + 1;

            // kick off the operations
            var operations = [];
            var chunkId = 0;
            for(var i = 0; i < models.length; i += chunkSize) {

                var chunk = models.slice(i, i + chunkSize);
                operations.push(storeInternal(chunk, mode, chunkId, cbProgress));
                chunkId++;
            }

            return q.allSettled(operations).then(function (results) {

                var rejected = undefined;

                results.forEach(function (result) {
                    if (result.state !== 'fulfilled') {
                        rejected = result.value ? result.value : result.reason;
                        return;
                    }
                });

                if (rejected === undefined) {
                    return q.resolve();
                } else {
                    return q.reject(rejected);
                }
            });
        };

        self.insert = function (models, exclusive, cbProgress) {
            return self.store(models, exclusive ? storeMode.insertExclusive : storeMode.insert, cbProgress)
        };

        self.merge = function (models, exclusive, cbProgress) {
            return self.store(models, exclusive ? storeMode.mergeExclusive : storeMode.merge, cbProgress)
        };

        self.query = function (partitionKey, rowKey, optionalDefaultValues, options) {

            // build the query
            var query = buildQueryObject(partitionKey, rowKey);

            // execute
            return self.queryNative(query, optionalDefaultValues, options)
        };

        self.querySingle = function (partitionKey, rowKey, optionalDefaultValues) {
            var defered = q.defer();

            // load the profile
            self.query(partitionKey, rowKey, optionalDefaultValues).then(function (results) {

                // check if the result was empty
                if (results.length < 1) {
                    defered.reject(new Error('Entity not found'));
                } else {
                    defered.resolve(results[0]);
                }

            }).catch(function (error) {
                defered.reject(error);
            });

            return defered.promise;
        };

        self.queryPaged = function(partitionKey, rowKey, optionalDefaultValues, options) {

            // build the query
            var query = buildQueryObject(partitionKey, rowKey);

            // query
            return self.queryNativePaged(query, optionalDefaultValues, options);
        };

        self.queryEntitiesRecursive = function (tableName, query, maxCount, token, callback) {
            var currentResult = [];
            self.azureCalls.queryEntities(tableName, query, token, function (error, result, response) {
                if (error !== null) {
                    callback(error, result, response);
                    return;
                } else if (result.continuationToken !== null) {
                    currentResult = result.entries;

                    // check if our currentResult contains enough elements
                    if (maxCount > 0 && currentResult.length >= maxCount) {
                        callback(error, {entries: currentResult}, response);
                        return;
                    } else {
                        self.queryEntitiesRecursive(tableName, query, maxCount, result.continuationToken, function (error, result, response) {
                            callback(error, {entries: currentResult.concat(result.entries)}, response);
                            return;
                        })
                    }
                } else {
                    callback(error, result, response);
                }
            })
        };


        self.queryPage = function (tableName, query, token) {
            var defer = q.defer();

            self.azureCalls.queryEntities(tableName, query, token, function (error, result, response) {
                if (error !== null) {
                    return defer.reject(error);
                } else {
                    return defer.resolve({entries: result.entries, token: result.continuationToken});
                }
            });

            return defer.promise;
        };

        self.queryNative = function (query, optionalDefaultValues, options) {
            var deferred = q.defer();

            if (query === undefined || query === null) {
                query = new azure.TableQuery();
            }

            // add the counter
            var maxCount = 0;
            if (options && options.top) {
                if (options.top <= 1000) {
                    query = query.top(options.top);
                }
                maxCount = options.top;
            }

            // calculate the table name
            var tableName = definition.TableName();

            // query the data
            self.queryEntitiesRecursive(tableName, query, maxCount, null, function (error, result, response) {

                if (error !== null) {
                    deferred.reject(error);
                } else {
                    var resultObjects = mapResultObjects(result, maxCount, optionalDefaultValues);
                    deferred.resolve(resultObjects);
                }
            });

            return deferred.promise;
        };

        self.queryPageByPage = function(tableName, query, token, notifyDeferred, maxCount, optionalDefaultValues) {

            // query the data
            return self.queryPage(tableName, query, token).then(function(result) {

                // map the result
                var resultObjects = mapResultObjects(result, maxCount, optionalDefaultValues);

                // notify
                notifyDeferred.notify(resultObjects);

                // check if we need the next page
                if (result.token) {

                    return self.queryPageByPage(tableName, query, result.token, notifyDeferred);
                } else {
                    return q.resolve();
                }
            });
        };

        self.queryNativePaged = function (query, optionalDefaultValues, options) {
            // ensure we have options
            options = options ? options : {};

            // get the right defferer
            var deferred = q.defer();

            if (query === undefined || query === null) {
                query = new azure.TableQuery();
            }

            // add the counter
            var maxCount = 0;
            if (options.top) {
                if (options.top <= 1000) {
                    query = query.top(options.top);
                }
                maxCount = options.top;
            }

            // set the intial notify deferrer
            if (!options.deferred) {
                options.deferred = deferred;
            }

            // calculate the table name
            var tableName = definition.TableName();

            // query the data
            self.queryPageByPage(tableName, query, undefined, deferred, maxCount, optionalDefaultValues).then(function() {
                return deferred.resolve();
            }).catch(function(e) {
                return deferred.reject(e);
            });

            return deferred.promise;
        };

        self.delete = function (models) {

            // check of the models list is empty
            if (models === null || models === undefined || models.length === 0) {
                return q.resolve();
            }

            // calculate the table name
            var tableName = definition.TableName();

            // generate the batch
            var deleteOperationBatches = [];

            // current batch
            var deleteOperationBatch = undefined;

            // visit every model and add the remove operation
            // ot our batch
            var taskCounter = 0;

            models.forEach(function (model) {

                // create the batch operation
                if (!deleteOperationBatch) {
                    deleteOperationBatch = new azure.TableBatch();
                    deleteOperationBatches.push(deleteOperationBatch);
                }

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

                taskCounter++;
                deleteOperationBatch.deleteEntity(task);

                if (taskCounter == 100) {
                    taskCounter = 0;
                    deleteOperationBatch = undefined;
                }
            });

            // execute the remove operation
            var deletePromises = [];
            deleteOperationBatches.forEach(function(deleteBatch) {
                deletePromises.push(storeBatchNative(tableName, deleteBatch));
            });

            // wait
            return q.allSettled(deletePromises);
        };

        self.deleteMultiplePartitions = function (models) {

            // split the models into different partitions
            var partitionModelMap = {};
            models.forEach(function (model) {

                if (!partitionModelMap[model.PartitionKey(model)]) {
                    partitionModelMap[model.PartitionKey(model)] = [];
                }

                partitionModelMap[model.PartitionKey(model)].push(model);
            })

            // delete per partition
            var deletePromisses = [];
            Object.keys(partitionModelMap).forEach(function (parititonKey) {
                deletePromisses.push(self.delete(partitionModelMap[parititonKey]));
            })

            // wait until all finished
            return q.allSettled(deletePromisses);
        };

        self.deleteByPartitionKey = function (partitionKey) {

            // retrieve the whole partition
            return self.query(partitionKey).then(function (elements) {

                // define a dummy model for partition removal
                var DummyDeleteModel = new AzureModelDefinition({
                    P: String, R: String,
                    PartitionKey: function (model) {
                        return model.P;
                    },
                    RowKey: function (model) {
                        return model.R;
                    },
                    TableName: function () {
                        return definition.TableName();
                    }
                });

                // build the remove models
                var removeModels = elements.map(function (element) {
                    return DummyDeleteModel.build({P: partitionKey, R: element.RowKey(element)});
                });

                // check
                if (removeModels.length == 0) {
                    return q.resolve();
                } else {
                    // delete it
                    return DummyDeleteModel.delete(removeModels);
                }
            });
        }

        self.create = function () {
            var deferred = q.defer();

            // calculate the table name
            var tableName = definition.TableName();

            self.azureCalls.createTableIfNotExists(tableName, function (error, result, response) {
                if (error === null) {
                    deferred.resolve();
                } else {
                    deferred.reject(error);
                }
            })

            return deferred.promise;
        }
    }

    /*
     * Exporting the model definition
     */
    selfTableClient.define = function (definition) {
        return new AzureModelDefinition(definition);
    };

    /*
     * Exporting the config operations
     */
    selfTableClient.config = function (storageAccountKey, storageAccountSecret, storageAccountEndpointSuffix) {
        activeStorageProvider.connect(storageAccountKey, storageAccountSecret, storageAccountEndpointSuffix)
    };

    /*
     * Allows to override the storage provider
     */
    selfTableClient.setStorageProvider = function (storageProvider) {
        activeStorageProvider = storageProvider;
    }

    /*
     * A couple helper which makes it easier to deal with the table store
     */
    selfTableClient.helpers = new AzureTableClientHelpers(selfTableClient, activeStorageProvider);
}

exports.AzureTableClient = AzureTableClient

