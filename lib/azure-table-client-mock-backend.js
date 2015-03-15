var q = require('q');

exports.cache = {};
exports.cacheSingleKey = {};

exports.insert = function(models) {
    var defered = q.defer();

    models.forEach(function(model) {

        // insert the model under a partition+rowkey combination
        exports.cacheSingleKey[model.TableName() + "#" + model.PartitionKey(model) + "#" + model.RowKey(model)] = model;

        // insert the model under a partition
        if ( exports.cache[model.TableName() + "#" + model.PartitionKey(model)] === undefined) {
            exports.cache[model.TableName() + "#" + model.PartitionKey(model)] = {};
        }
        exports.cache[model.TableName() + "#" + model.PartitionKey(model)][model.RowKey(model)] = model;


        // insert the model under a rowkey
        if (exports.cache[model.TableName() + "##" + model.RowKey(model)] == undefined) {
            exports.cache[model.TableName() + "##" + model.RowKey(model)] = {};
        }
        exports.cache[model.TableName() + "##" + model.RowKey(model)][model.PartitionKey(model)] = model;
    });

    defered.resolve();

    return defered.promise;
};

exports.merge = function(models) {

    var modelsToInsert = [];

    models.forEach(function(model) {

        var existingModel = exports.cacheSingleKey[model.TableName() + "#" + model.PartitionKey(model) + "#" + model.RowKey(model)];
        if (existingModel === undefined) {
            modelsToInsert.push(model);
        } else {

            model.modelKeys().forEach(function(key) {

                if (model[key] !== undefined) {
                    existingModel[key] = model[key];
                }
            })
        }
    });

    return exports.insert(modelsToInsert);
};

exports.delete = function(models) {
    var defered = q.defer();

    models.forEach(function(model) {
        delete exports.cacheSingleKey[model.TableName() + "#" + model.PartitionKey(model) + "#" + model.RowKey(model)];
        delete exports.cache[model.TableName() + "#" + model.PartitionKey(model)];
        delete exports.cache[model.TableName() + "##" + model.RowKey(model)];
    });

    defered.resolve();

    return defered.promise;
};

exports.query = function(tablename, partitionKey, rowKey) {
    var defered = q.defer();

    var result = [];

    if ( partitionKey === null && rowKey === null) {
        var objKeys = Object.keys(exports.cacheSingleKey);
        objKeys.forEach(function(elementKey) {
           result.push(exports.cacheSingleKey[elementKey]);
        });
    } else {

        // build the search key
        var searchKey = tablename + "#";
        if (partitionKey !== undefined && partitionKey !== null) {
            searchKey += partitionKey;
        }

        if (rowKey !== undefined && rowKey !== null) {
            searchKey += "#";
            searchKey += rowKey;
        }

        // query the elements for the search queu
        var resultElements = exports.cache[searchKey];
        var objKeys = Object.keys(resultElements);
        objKeys.forEach(function (elementKey) {
            result.push(resultElements[elementKey]);
        });
    }

    defered.resolve(result);

    return defered.promise;
};

exports.reset = function() {
    exports.cache = {};
    exports.cacheSingleKey = {};
};

