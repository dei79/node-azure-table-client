var q = require('q');

function AzureTableClientHelpers(azureTableClient, activeStorageProvider) {
    var self = this;

    self.createTable = function(tableName) {

        var deferred = q.defer();

        activeStorageProvider.createTableIfNotExists(tableName, function(error) {
            if (error === null) {
                deferred.resolve();
            } else if (error.message.indexOf('Try operation later') !== -1) {
                setTimeout(function() {
                    self.createTable(tableName).then(function() {
                        deferred.resolve();
                    }).catch(function(error) {
                        deferred.reject(error);
                    })
                }, 1000);
            } else {
                deferred.reject(error);
            }
        })

        return deferred.promise;
    }

    // Groups a list of models by partitionkey
    self.groupModelsByPartitionKey = function(models) {

        var groupedData = {};

        models.forEach(function(model) {

            var pk = model.PartitionKey(model);

            var group = groupedData[pk];
            if (!group) { groupedData[pk] = []; group = groupedData[pk]; }

            group.push(model);
        });

        return groupedData;
    }

    self.dynamicTableDefinition = function(modelObject, partitionKeyPropertyName, rowKeyPropertyName, tableName, typeMapperCallback) {

        // build the definition
        var definition = {
            PartitionKey: function(model) {
                return model[partitionKeyPropertyName];
            },
            RowKey: function(model) {
                return model[rowKeyPropertyName];
            },
            TableName: function() {
                return tableName;
            }
        };

        Object.keys(modelObject).forEach(function(k) {

            // ignore this properties
            if (k == 'PartitionKey' || k == 'RowKey') { return; }

            // allow dynamic type mapping
            if ( typeMapperCallback ) {
                definition[k] = typeMapperCallback(k);
            } else {
                definition[k] = String;
            }
        });

        // define a virtual entity
        return azureTableClient.define(definition);
    }

}


exports.AzureTableClientHelpers = AzureTableClientHelpers;
