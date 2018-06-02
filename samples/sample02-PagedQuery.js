var credentials = require('../.credentials.json');
var azureTables = require('../lib/azure-table-client.js');

var tableCredentials = credentials['demo01'];

var azureTableClient = new azureTables.AzureTableClient();
azureTableClient.config(tableCredentials.key, tableCredentials.secret, tableCredentials.suffix);

var util = require('util');

var Person = azureTableClient.define({
    FirstName: String,
    LastName: String,
    UniqueIdentifier: String,
    PartitionKey: function (model) {
        return 'PERSON';
    },
    RowKey: function (model) {
        return model.UniqueIdentifier;
    },
    TableName: function () {
        return "Persons";
    }
});

function dumpMemory() {
    var workingSet = process.memoryUsage();
    console.log(workingSet.heapUsed + "," + workingSet.heapTotal);
}

function triggerGC() {
    if (global && global.gc) {
        global.gc();
    }
}

dumpMemory();

// build 2500 models
var itemCount = 25000;
var persons = [];

for(var i = 0; i < itemCount; i++) {
    persons.push(Person.build({FirstName: "DefaultFirstName", LastName: "DefaultLastName", UniqueIdentifier: "PP" + i}));
}

// create the 100 models
dumpMemory();
console.log("Creating " + itemCount + " items...");
Person.insert(persons).then(function() {

    // ensure memory can be freed
    persons = undefined;
    dumpMemory();

    triggerGC();

    // query paged
    console.log("Loading Paged...");
    return Person.queryPaged('PERSON', undefined, undefined, undefined).progress(function(pageResult) {
            triggerGC();
            dumpMemory();
            console.log("Next Page with size " + pageResult.length);
    }).then(function() {

        dumpMemory();

        // query recursive
        console.log("Loading recursive...");
        return Person.query('PERSON').then(function(result) {
            console.log("Loaded recursive " + result.length);
            dumpMemory();

            // delete
            console.log("Delete persons...");
            return Person.delete(persons).then(function () {
                dumpMemory();
                console.log("DONE");
                process.exit(0);
            })
        });
    });

}).catch(function(e) {
    console.log("ERROR");
    console.log(e);
    process.exit(1);
});