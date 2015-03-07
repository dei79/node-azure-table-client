var q = require('q');

exports.cache = {};

exports.insert = function(models) {
    var defered = q.defer();

    models.forEach(function(model) {

        // insert the model under a partition+rowkey combination
        exports.cache[model.TableName() + "#" + model.PartitionKey(model) + "#" + model.RowKey(model)] = model;

        // insert the model under a partition
        exports.cache[model.TableName() + "#" + model.PartitionKey(model)] = model;

        // insert the model under a rowkey
        exports.cache[model.TableName() + "##" + model.RowKey(model)] = model;
    });

    defered.resolve();

    return defered.promise;
};

exports.merge = function(models) {

    var modelsToInsert = [];

    models.forEach(function(model) {

        var existingModel = exports.cache[model.TableName() + "#" + model.PartitionKey(model) + "#" + model.RowKey(model)];
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
    defered.resolve();
    return defered.promise;
};

exports.query = function(partitionKey, rowKey) {
    var defered = q.defer();
    defered.resolve();
    return defered.promise;
};

exports.reset = function() {
    exports.cache = {};
};

