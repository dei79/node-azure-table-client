# node-azure-table-client
A simple to use client for Azure Table Store which allows model definitions (comparable to an ORM) and correct type conversion. The componet comes with a coupel useful features:

* Using simple javascript objects
* String type conversion of objects during serialization 
* Insert, Merge & Query operations
* Batch Operations for multiple hundred or thousand records
* Validation against the defined weak schema

## Breaking Changes
**Starting with version 0.4.0 we are exposing a table client class which needs to be used to define, create, query and delete
entities. This makes it possible to use the module with separate table stores in a single process.**

## Install 
Just install the component via npm

```
npm install azure-table-client --save
```

## Configure
The table client needs to be configured before using it the first time. The configuration can be performed as follows:

```javascript
var azureTables = require('azure-table-client');

var azureTableClient = new azureTables.AzureTableClient();
azureTableClient.config(<<YOURACCOUNTKEY>>, <<YOURACCOUNTSECRET>>);
```

## Define Models 
Defining models in the azure table client is as simple as in different ORM implementations for node. Of course a simple table store like Azure Table has no hard relations this means this clients does not support any implementation of relations as well. If relations are required in some projects the business logic above this client needs to implement this. 

A first simple model can be defined as follows: 

```javascript 
var Person = azureTableClient.define({
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

## Build & store a new model-instance
A new model instance is just a simple presentation of the model in the memory. It does not need to be stored in the Azure Storage Services. It's normally the first step to creat models and is used often when objects will be received:

```javascript
var person1 = Person.build({FirstName: "DefaultFirstName", LastName: "DefaultLastName"});
```

Every built model instance can be used as every normal javascript object which means the property can be changed as usual:

```javascript
person1.FirstName = "Kevin";
```

The model instance offers a couple of instance specific method to update or create to model in the Azure Storage Services. Azure has to major options to make something persistent. The first one is called "insertOrReplace" which means the object will be created in the store as it is in the memory and an existing one will be fully replaced. This can cause data loss if the built object does not contain all properties:

```javascript
person1.insert();
```

An other option is the "insertOrMerge" operation. Executing this operation Azure Storage will let the all non existing properties untouched and just overrides the existing one in this schema: 

```javascript
person1.merge();
```

Both operations will create a new instance if no instance exists in the storage services. 

## Query models 

TODO

## Batch creation or update

TODO 

## Query Mapping

TODO


