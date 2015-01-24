# node-azure-table-client
A simple to use client for Azure Table Store which allows model definitions (comparable to an ORM) and correct type conversion.

## Configure
The table client needs to be configured before using it the first time. The configuration can be performed as follows:

```javascript
var azureTables = require('azure-table-client');
azureTables.config(<<YOURACCOUNTKEY>>, <<YOURACCOUNTSECRET>>);
```

## Define Models 
Defining models in the azure table client is as simple as in different ORM implementations for node. Of course a simple table store like Azure Table has no hard relations this means this clients does not support any implementation of relations as well. If relations are required in some projects the business logic above this client needs to implement this. 

A first simple model can be defined as follows: 

```javascript 
var Person = azureTables.define({
  FirstName: String,
  LastName: String,
  UniqueIdentifier: String,
  PartitionKey: function(model) {
    return model.UnqieIdentifier;
  },
  RowKey: function(model) {
    return model.LastName;
  }
```

There are two special fields, the PartitionKey and the RowKey which are used in Azure Tables to identify a single record. During the model defintion a function needs to be defined which can calculate the values during operations against the storage services. When ever the system calls the Azure Storage Services it's recalculating the partition and row key.

## Query Mapping

TODO


