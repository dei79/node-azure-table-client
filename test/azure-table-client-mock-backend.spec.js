var azuremodel = require('../lib/azure-table-client.js');
var mockBackend = require('../lib/azure-table-client-mock-backend.js');

describe('AzureTableClientMockBackend', function() {

    beforeEach(function () {

        // reset the backend stuff
        mockBackend.reset();

        // configure the correct backend to the azuremodels
        azuremodel.setBackend(mockBackend);

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
                return "AccountsCollection";
            }
        });
    });

    describe('insert', function () {

        it('add the models into the mock cache', function () {
            // add a couple values to the cache
            mockBackend.insert([
                Account.build({AccountId: "123", Name: "Heinz"}),
                Account.build({AccountId: "124", Name: "Egon"})
            ]).should.be.fulfilled;

            // we expect 6 entries because 3 for every element
            Object.keys(mockBackend.cache).length.should.be.equal(6);

            Object.keys(mockBackend.cache)[0].should.be.equal('AccountsCollection#123#Heinz');
            Object.keys(mockBackend.cache)[1].should.be.equal('AccountsCollection#123');
            Object.keys(mockBackend.cache)[2].should.be.equal('AccountsCollection##Heinz');

            Object.keys(mockBackend.cache)[3].should.be.equal('AccountsCollection#124#Egon');
            Object.keys(mockBackend.cache)[4].should.be.equal('AccountsCollection#124');
            Object.keys(mockBackend.cache)[5].should.be.equal('AccountsCollection##Egon');
        })

    });

    describe('merge', function () {

        it('insert not existing item', function () {
            // add a couple values to the cache
            mockBackend.merge([
                Account.build({AccountId: "123", Name: "Heinz"})
            ]).should.be.fulfilled;

            // we expect 6 entries because 3 for every element
            Object.keys(mockBackend.cache).length.should.be.equal(3);

            Object.keys(mockBackend.cache)[0].should.be.equal('AccountsCollection#123#Heinz');
            Object.keys(mockBackend.cache)[1].should.be.equal('AccountsCollection#123');
            Object.keys(mockBackend.cache)[2].should.be.equal('AccountsCollection##Heinz');
        });

        it('merge an existing item', function () {
            // insert
            mockBackend.insert([
                Account.build({AccountId: "123", Name: "Heinz", Demo1: "Demo1Value"})
            ]).should.be.fulfilled;

            // check
            Object.keys(mockBackend.cache).length.should.be.equal(3);
            mockBackend.cache['AccountsCollection#123#Heinz'].Demo1.should.be.equal('Demo1Value');
            (mockBackend.cache['AccountsCollection#123#Heinz'].Demo2 === undefined).should.be.true;

            // merge
            mockBackend.merge([
                Account.build({AccountId: "123", Name: "Heinz", Demo2: "Demo2Value"})
            ]).should.be.fulfilled;

            // check
            Object.keys(mockBackend.cache).length.should.be.equal(3);
            mockBackend.cache['AccountsCollection#123#Heinz'].Demo1.should.be.equal('Demo1Value');
            mockBackend.cache['AccountsCollection#123#Heinz'].Demo2.should.be.equal('Demo2Value');
        })

    });

    describe('delete', function () {

        it('remove an item', function () {
            mockBackend.delete([]).should.be.fulfilled;
        })
    });

    describe('query', function () {

        it('returns items', function () {
            mockBackend.query(null,null).should.be.fulfilled;
        });

    });

});
