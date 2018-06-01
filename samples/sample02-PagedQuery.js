var credentials = require('../.credentials.json');
var azureTables = require('../lib/azure-table-client.js');

var tableCredentials = credentials['demo01'];

var azureTableClient = new azureTables.AzureTableClient();
azureTableClient.config(tableCredentials.key, tableCredentials.secret, tableCredentials.suffix);

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

// build 2500 models
var persons = [];

for(var i = 0; i < 2500; i++) {
    persons.push(Person.build({FirstName: "DefaultFirstName", LastName: "DefaultLastName", UniqueIdentifier: "PP" + i}));
}

// create the 100 models
console.log("Creating 2500 items...");
Person.insert(persons).then(function() {

    // query paged
    console.log("Loading Paged...");
    return Person.queryPaged('PERSON').progress(function(pageResult) {
            console.log("Next Page with size " + pageResult.length);
    }).then(function() {

        // query recursive
        console.log("Loading recursive...");
        return Person.query('PERSON').then(function(result) {
            console.log("Loaded recursive " + result.length);

            // delete
            console.log("Delete persons...");
            return Person.delete(persons).then(function () {
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