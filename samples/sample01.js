var credentials = require('../.credentials.json');
var azureTables = require('../lib/azure-table-client.js');

var tableCredentials = credentials['de'];

var azureTableClient = new azureTables.AzureTableClient();
azureTableClient.config(tableCredentials.key, tableCredentials.secret, tableCredentials.suffix);

var Person = azureTableClient.define({
    FirstName: String,
    LastName: String,
    UniqueIdentifier: String,
    PartitionKey: function (model) {
        return model.UniqueIdentifier;
    },
    RowKey: function (model) {
        return model.LastName;
    },
    TableName: function () {
        return "Persons";
    }
});

var person1 = Person.build({FirstName: "DefaultFirstName", LastName: "DefaultLastName", UniqueIdentifier: "P3"});
person1.insert().then(function () {

    return Person.query(undefined, undefined, undefined, {top: 2}).then(function(results) {
        console.log(results);
        process.exit(0);
    });

}).catch(function(e) {
    console.log(e);
    process.exit(1);
});