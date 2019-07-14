var credentials = require('../.credentials.json');
var azureTables = require('../lib/azure-table-client.js');

var tableCredentials = credentials['demo01'];

var azureTableClient = new azureTables.AzureTableClient();
azureTableClient.config(tableCredentials.key, tableCredentials.secret, tableCredentials.suffix);

var Person = azureTableClient.define({
    FirstName: String,
    LastName: String,
    Partition: String,
    Counter: Number,
    PartitionKey: function (model) {
        return model.Partition;
    },
    RowKey: function (model) {
        return 'P' + model.Counter;
    },
    TableName: function () {
        return "PersonsMass6";
    }
});

var overAllPersonsCounter = 112317;
// var overAllPersonsCounter = 500;
// var overAllPersonsCounter = 12317;
console.log("Generating " + overAllPersonsCounter + " persons...");

var persons = [];
for(var i = 0; i < overAllPersonsCounter; i++)
{
    persons.push(Person.build({
        FirstName: "DefaultFirstName",
        LastName: "DefaultLastName",
        Partition: "MassInsert",
        Counter: i
    }));
}

var progressHandler = function(threadId, pendingElements) {
    console.log("T: " + threadId + "Items pending: " + pendingElements.length);
};

// Merging persons into storage
var startTime = new Date();
console.log("Storing " + overAllPersonsCounter + " persons...");
Person.merge(persons, false, progressHandler).then(function() {
    console.log("Store Procedure done");
    console.log("Started: " + startTime);
    console.log("Finished: " + new Date());
    process.exit(0);
}).catch(function(e) {
    console.log("Store Procedure failed");
    console.log(e);
    process.exit(1);
});
