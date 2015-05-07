var azure               = require("azure-storage");

function AzureTableClientMockBackend() {
    var self = this;

    var mockStorage = {};

    self.reset = function() {
        mockStorage = {};
    }

    self.get = function() {
        return mockStorage;
    }

    self.connect = function(storageAccountKey, storageAccountSecret) {
        self.reset();
    }

    self.createTableIfNotExists = function(tableName, callback) {

        if (mockStorage[tableName] == undefined) {
            mockStorage[tableName] = {};
        }

        callback(null, null, null);
    }

    self.executeBatch = function(tableName, batch, callback) {

        self.createTableIfNotExists(tableName, function() {

            try {
                batch.operations.forEach(function (operation) {

                    // get the partition key
                    var partitionkey = operation.entity.PartitionKey._;

                    // get the row key
                    var rowkey = operation.entity.RowKey._;

                    // store the whole entity
                    if (mockStorage[tableName][partitionkey] === undefined) {
                        mockStorage[tableName][partitionkey] = {};
                    }

                    mockStorage[tableName][partitionkey][rowkey] = operation.entity;

                    /*
                     { PartitionKey: { _: 'userId01', '$': 'Edm.String' },
                     RowKey: { _: 'liveId01', '$': 'Edm.String' },
                     FirstName: { _: 'Arthur', '$': 'Edm.String' },
                     LastName: { _: 'Admin', '$': 'Edm.String' },
                     LiveId: { _: 'liveId01', '$': 'Edm.String' },
                     UserId: { _: 'userId01', '$': 'Edm.String' },
                     Contact: { _: 'aa@example.com', '$': 'Edm.String' },
                     LoginCount: { _: 0, '$': 'Edm.Double' } }
                     */

                    // type: 'INSERT_OR_MERGE',
                })

                callback(null, null, null);
            } catch(error) {
                callback(error, null, null);
            }
        })
    }

    self.queryEntities = function(tableName, tableQuery, currentToken, optionsOrCallback, callback) {

        // get the properties from tablequery
        var queryElements = {}

        tableQuery._where.forEach(function(queryElement) {
            var queryElementArr = queryElement.split(' eq ');
            queryElements[queryElementArr[0]] = queryElementArr[1].replace(/'/g, '');
        })

        // check if we have the table
        var result = [];
        if (mockStorage[tableName] !== undefined ) {

            // query by partition key
            result = mockStorage[tableName][queryElements.PartitionKey];

            // query by rowkey if neeed
            if (queryElements.RowKey !== undefined) {
                result = [ result[queryElements.RowKey] ]
            } else {

                var resultArr = [];
                Object.keys(result).forEach(function(key) {
                    resultArr.push(result[key]);
                })

                result = resultArr;
            }
        }

        if (callback === undefined) {
            optionsOrCallback(null, { entries: result, continuationToken: null }, null);
        } else {
            callback(null, {entries: result, continuationToken: null}, null);
        }
    }
};


module.exports = exports = new AzureTableClientMockBackend();