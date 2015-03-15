var azuremodel = require('../lib/azure-table-client.js');
var mockBackend = require('../lib/azure-table-client-mock-backend.js');

describe('AzureTableClientMockBackend', function() {

    beforeEach(function () {

        // reset the backend stuff
        mockBackend.reset();

        // configure the correct backend to the azuremodels
        azuremodel.setBackend(mockBackend);

        // some helper
        AccountTableName = "AccountsCollection";

        // define a simple contract model
        Account = azuremodel.define({
            AccountId: String,
            Name: String,
            Demo1: String,
            Demo2: String,
            PartitionKey: function (model) {
                return model.AccountId;
            },
            RowKey: function (model) {
                return model.Name;
            },
            TableName: function () {
                return AccountTableName;
            }
        });
    });

    describe('insert', function () {

        it('add the models into the mock cache', function () {
            // add a couple values to the cache
            return mockBackend.insert([
                Account.build({AccountId: "123", Name: "Heinz"}),
                Account.build({AccountId: "124", Name: "Egon"})
            ]).finally(function() {

                // we expect 6 entries because 3 for every element
                Object.keys(mockBackend.cache).length.should.be.equal(4);
                Object.keys(mockBackend.cacheSingleKey).length.should.be.equal(2);

                Object.keys(mockBackend.cacheSingleKey)[0].should.be.equal('AccountsCollection#123#Heinz');
                Object.keys(mockBackend.cache)[0].should.be.equal('AccountsCollection#123');
                Object.keys(mockBackend.cache)[1].should.be.equal('AccountsCollection##Heinz');

                Object.keys(mockBackend.cacheSingleKey)[1].should.be.equal('AccountsCollection#124#Egon');
                Object.keys(mockBackend.cache)[2].should.be.equal('AccountsCollection#124');
                Object.keys(mockBackend.cache)[3].should.be.equal('AccountsCollection##Egon');
            }).should.be.fulfilled;
        });

        it('add the models with the same primary key', function() {
            // add a couple values to the cache
            return mockBackend.insert([
                Account.build({AccountId: "123", Name: "Heinz"}),
                Account.build({AccountId: "123", Name: "Egon"})
            ]).finally(function() {

                // we expect 5 entries because of the same partition key
                Object.keys(mockBackend.cache).length.should.be.equal(3);
                Object.keys(mockBackend.cacheSingleKey).length.should.be.equal(2);

                Object.keys(mockBackend.cache)[0].should.be.equal('AccountsCollection#123');
                Object.keys(mockBackend.cache[Object.keys(mockBackend.cache)[0]])[0].should.be.equal('Heinz');
                Object.keys(mockBackend.cache[Object.keys(mockBackend.cache)[0]])[1].should.be.equal('Egon');
            })
        })
    });

    describe('merge', function () {

        it('insert not existing item', function () {
            // add a couple values to the cache
            mockBackend.merge([
                Account.build({AccountId: "123", Name: "Heinz"})
            ]).should.be.fulfilled;

            // we expect 6 entries because 3 for every element
            Object.keys(mockBackend.cache).length.should.be.equal(2);
            Object.keys(mockBackend.cacheSingleKey).length.should.be.equal(1);

            Object.keys(mockBackend.cacheSingleKey)[0].should.be.equal('AccountsCollection#123#Heinz');
            Object.keys(mockBackend.cache)[0].should.be.equal('AccountsCollection#123');
            Object.keys(mockBackend.cache)[1].should.be.equal('AccountsCollection##Heinz');
        });

        it('merge an existing item', function () {
            // insert
            mockBackend.insert([
                Account.build({AccountId: "123", Name: "Heinz", Demo1: "Demo1Value"})
            ]).should.be.fulfilled;

            // check
            Object.keys(mockBackend.cache).length.should.be.equal(2);
            Object.keys(mockBackend.cacheSingleKey).length.should.be.equal(1);
            mockBackend.cacheSingleKey['AccountsCollection#123#Heinz'].Demo1.should.be.equal('Demo1Value');
            (mockBackend.cacheSingleKey['AccountsCollection#123#Heinz'].Demo2 === undefined).should.be.true;

            // merge
            mockBackend.merge([
                Account.build({AccountId: "123", Name: "Heinz", Demo2: "Demo2Value"})
            ]).should.be.fulfilled;

            // check
            Object.keys(mockBackend.cache).length.should.be.equal(2);
            Object.keys(mockBackend.cacheSingleKey).length.should.be.equal(1);
            mockBackend.cacheSingleKey['AccountsCollection#123#Heinz'].Demo1.should.be.equal('Demo1Value');
            mockBackend.cacheSingleKey['AccountsCollection#123#Heinz'].Demo2.should.be.equal('Demo2Value');
        })

    });

    describe('delete', function () {

        it('remove an item', function () {

            var model = Account.build({AccountId: "123", Name: "Heinz", Demo1: "Demo1Value"});

            // add the model
            return mockBackend.insert([model]).finally(function() {

                // check
                Object.keys(mockBackend.cacheSingleKey).length.should.be.equal(1);
                Object.keys(mockBackend.cache).length.should.be.equal(2);
                mockBackend.cacheSingleKey['AccountsCollection#123#Heinz'].Demo1.should.be.equal('Demo1Value');

                // delete the model
                return mockBackend.delete([model]).finally(function() {

                    // check
                    Object.keys(mockBackend.cache).length.should.be.equal(0);

                }).should.be.fulfilled;

            }).should.be.fulfilled;
        })
    });

    describe('query', function () {

        beforeEach(function() {

            // add default models
            var models = [
                Account.build({AccountId: "456", Name: "Heinz", Demo1: "Demo1Value"}),
                Account.build({AccountId: "456", Name: "Egon", Demo1: "Demo2Value"}),
                Account.build({AccountId: "123", Name: "Emil", Demo1: "Demo2Value"}),
                Account.build({AccountId: "123", Name: "Egon", Demo1: "Demo2Value"})
            ];

            // add the model
            return mockBackend.insert(models).finally(function() {
                Object.keys(mockBackend.cache).length.should.be.equal(5);
                Object.keys(mockBackend.cacheSingleKey).length.should.be.equal(4);
            });
        });

        it('returns all items', function () {

            // query all
            return mockBackend.query(AccountTableName, null, null).then(function(models) {

                // check
                models.length.should.be.equal(4);

                // check the models
                models[0].AccountId.should.be.equal('456');
                models[1].AccountId.should.be.equal('456');
                models[2].AccountId.should.be.equal('123');
                models[3].AccountId.should.be.equal('123');

            }).should.be.fulfilled;
        });

        it('return only the items with a specific primary key', function() {
            // query all
            return mockBackend.query(AccountTableName, '456', null).then(function(models) {

                // check
                models.length.should.be.equal(2);

                // check the models
                models[0].AccountId.should.be.equal('456');
                models[1].AccountId.should.be.equal('456');

            }).should.be.fulfilled;
        });

        it('return only the items with a specific row key', function() {
            // query all
            return mockBackend.query(AccountTableName, null, 'Egon').then(function(models) {

                // check
                models.length.should.be.equal(2);

                // check the models
                models[0].AccountId.should.be.equal('123');
                models[1].AccountId.should.be.equal('456');

            }).should.be.fulfilled;
        })

    });

});
