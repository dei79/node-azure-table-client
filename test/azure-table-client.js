var azuremodel = require('../lib/azure-table-client.js');

describe('AzureTableClient', function() {
    var Account = undefined;

    beforeEach(function () {

        // config the azure table store with a fake account
        azuremodel.config("fakeAccount", "XDsrm1KPRn5LOFquMkFK013VAM37JErnsAc2t0MWMscbvasZi61hJdsCWbkVb8DCF3q7riPjPsdqx2wlmC5dcQ==");

        // define a simple contract model
        Account = azuremodel.define({
            AccountId: String,
            Name: String,
            Contact: String,
            WantsReport: Boolean,
            WantsAlerts: Boolean,
            Limits: Number,
            PartitionKey: function (model) {
                return model.AccountId;
            },
            RowKey: function (model) {
                return model.Contact;
            },
            TableName: function () {
                return "AccountsCollection";
            }
        });

    });

    describe('config', function () {

        it('throw exception when using invalid key', function () {
            (function () {
                azuremodel.config("myaccount", "invalidsecret");
            }).should.throw('The provided account key invalidsecret is not a valid base64 string.');
        });

        it('finishes without exception when passing valid key', function () {
            (function () {
                azuremodel.config("myaccount", "XDsrm1KPRn5LOFquMkFK013VAM37JErnsAc2t0MWMscbvasZi61hJdsCWbkVb8DCF3q7riPjPsdqx2wlmC5dcQ==");
            }).should.not.throw();
        })
    });

    describe('define', function () {

        it('generates a standard Contract model', function () {
            var account = Account.build();
            (account.AccountId === undefined).should.be.ok;
            (account.Name === undefined).should.be.ok;
            (account.Contact === undefined).should.be.ok;
            (account.WantsReport === undefined).should.be.ok;
            (account.Limits === undefined).should.be.ok;
        });

        it('generates a Contract model with default values', function () {
            var account = Account.build({AccountId: "123456", Name: "abcd", Limits: 0.4});
            account.AccountId.should.be.equal('123456');
            account.Name.should.be.equal('abcd');
            (account.Contact === undefined).should.be.ok;
            (account.WantsReport === undefined).should.be.ok;
            account.Limits.should.be.equal(0.4);
        });

    });

    describe('insert', function() {

        it('calls PartitionKey function when saving', function () {
            var account = Account.build({
                AccountId: "123456",
                Name: "abcd",
                Limits: 0.4,
                PartitionKey: function (model) {
                    throw new Error('PartitionKeyFake')
                }
            });
            (function () {
                account.insert()
            }).should.throw('PartitionKeyFake');
        });

        it('calls RowKey function when saving', function () {
            var account = Account.build({
                AccountId: "123456",
                Name: "abcd",
                Limits: 0.4,
                PartitionKey: function (model) {
                    return model.AccountId;
                },
                RowKey: function (model) {
                    throw new Error('RowKeyFake')
                }
            });
            (function () {
                account.insert()
            }).should.throw('RowKeyFake');
        });

        it('calls TableName function when saving', function () {
            var simpleModelDefintion = azuremodel.define({
                AccountId: String, PartitionKey: function (model) {
                    return model.AccountId;
                }, RowKey: function (model) {
                    return model.AccountId;
                }, TableName: function () {
                    throw new Error('TableNameFake')
                }
            });
            var simpleModel = simpleModelDefintion.build({AccountId: "abcd"});
            (function () {
                simpleModel.insert()
            }).should.throw('TableNameFake');
        });

        it('rejects the call when table was not able to create', function () {
            sinon.stub(Account.azureCalls, "createTableIfNotExists", function (tableName, callback) {
                callback(new Error("Invalid"), null, null);
            });
            var account = Account.build({
                AccountId: "123456",
                Name: "abcd",
                Contact: "test@example.com",
                Limits: 0.4
            });
            return account.insert().should.be.rejected;
        });

        it('inserts the elements with missing value', function () {
            sinon.stub(Account.azureCalls, "createTableIfNotExists", function (tableName, callback) {
                callback(null, null, null);
            });
            sinon.stub(Account.azureCalls, "executeBatch", function (tableName, batch, callback) {

                // check the tablename
                tableName.should.be.equal("AccountsCollection");

                // check the single batch element we created
                batch.operations.length.should.be.eql(1);

                batch.operations.forEach(function (operation) {

                    operation.type.should.be.equal('INSERT_OR_REPLACE');
                    operation.entity.PartitionKey._.should.be.equal('123456');
                    operation.entity.PartitionKey.$.should.be.equal('Edm.String');
                    operation.entity.RowKey._.should.be.equal('test@example.com');
                    operation.entity.RowKey.$.should.be.equal('Edm.String');
                    operation.entity.AccountId._.should.be.equal('123456');
                    operation.entity.AccountId.$.should.be.equal('Edm.String');
                    operation.entity.Name._.should.be.equal('abcd');
                    operation.entity.Name.$.should.be.equal('Edm.String');
                    operation.entity.Contact._.should.be.equal('test@example.com');
                    operation.entity.Contact.$.should.be.equal('Edm.String');
                    operation.entity.Limits._.should.be.equal(0.4);
                    operation.entity.Limits.$.should.be.equal('Edm.Double');
                    operation.entity.WantsAlerts._.should.be.equal(true);
                    operation.entity.WantsAlerts.$.should.be.equal('Edm.Boolean');

                    Object.keys(operation.entity).length.should.be.eql(7);
                });

                callback(null, null, null);
            });

            var account = Account.build({
                AccountId: "123456",
                Name: "abcd",
                Contact: "test@example.com",
                Limits: 0.4,
                WantsAlerts: true
            });

            return account.insert().should.be.fulfilled;
        })
    });

    describe('query', function() {

        it("builds the correct query for primary key and rowkey", function() {

            sinon.stub(Account.azureCalls, "queryEntities", function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
                tableQuery._where[0].should.equal('PartitionKey eq \'123456\' and RowKey eq \'abcd\'');
                optionsOrCallback(null, { entries: []}, null);
            });

            return Account.query("123456", "abcd").should.be.fulfilled;
        });

        it("builds the correct query for primary key", function() {

            sinon.stub(Account.azureCalls, "queryEntities", function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
                tableQuery._fields.length.should.eql(0);
                (tableQuery._top === null).should.be.ok;
                tableQuery._where[0].should.equal('PartitionKey eq \'123456\'');
                optionsOrCallback(null, { entries: []}, null);
            });

            return Account.query("123456", null).should.be.fulfilled;
        });

        it("builds the correct query for row key", function() {

            sinon.stub(Account.azureCalls, "queryEntities", function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
                tableQuery._fields.length.should.eql(0);
                (tableQuery._top === null).should.be.ok;
                tableQuery._where[0].should.equal('RowKey eq \'abcd\'');
                optionsOrCallback(null, { entries: []}, null);
            });

            return Account.query(null, 'abcd').should.be.fulfilled;
        });

        it("builds the correct query for all", function() {

            sinon.stub(Account.azureCalls, "queryEntities", function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
                tableQuery._fields.length.should.eql(0);
                (tableQuery._top === null).should.be.ok;
                tableQuery._where.length.should.be.eql(0);
                optionsOrCallback(null, { entries: []}, null);
            });

            return Account.query(null, null).should.be.fulfilled;
        });

        it("returns valid default objects", function() {

            sinon.stub(Account.azureCalls, "queryEntities", function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
                var results = {
                    entries: [
                        {
                            PartitionKey:   { '$': 'Edm.String', _: 'Dirk' },
                            RowKey:         { '$': 'Edm.String', _: 'Eisenberg' },
                            Timestamp:      { '$': 'Edm.DateTime', _: new Date() },
                            AccountId:      { _: '34567' },
                            Name:           { _: 'Dirk Eisenberg' },
                            Contact:        { _: 'info@example.com' },
                            WantsAlerts:    { '$': 'Edm.Boolean', _: true },
                            Limits:         { '$': 'Edm.Double', _: 0.4 },
                            '.metadata':    { etag: 'W/"datetime\'2015-01-24T11:37:28.6472064Z\'"'}
                        }
                    ],
                    continuationToken: null };

                optionsOrCallback(null, results, null);
            });

            return Account.query().then(function(results) {
                results.length.should.eql(1);
                results[0].AccountId.should.equal('34567');
                results[0].Name.should.equal('Dirk Eisenberg');
                results[0].Contact.should.equal('info@example.com');
                results[0].Limits.should.eql(0.4);
                results[0].WantsAlerts.should.eql(true);
                (results[0].WantsReport === undefined).should.be.ok;

            }).should.be.fulfilled;
        });

        it("returns valid default objects with double as string", function() {

            sinon.stub(Account.azureCalls, "queryEntities", function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
                var results = {
                    entries: [
                        {
                            PartitionKey:   { '$': 'Edm.String', _: 'Dirk' },
                            RowKey:         { '$': 'Edm.String', _: 'Eisenberg' },
                            Timestamp:      { '$': 'Edm.DateTime', _: new Date() },
                            AccountId:      { _: '34567' },
                            Name:           { _: 'Dirk Eisenberg' },
                            Contact:        { _: 'info@example.com' },
                            Limits:         { '$': 'Edm.String', _: '0.0' },
                            '.metadata':    { etag: 'W/"datetime\'2015-01-24T11:37:28.6472064Z\'"'}
                        }
                    ],
                    continuationToken: null };

                optionsOrCallback(null, results, null);
            });

            return Account.query().then(function(results) {
                results.length.should.eql(1);
                results[0].Limits.should.eql(0.0);
            }).should.be.fulfilled;
        });

        it("returns valid default objects with mapped properties", function() {

            var ModelWithQueryMapping = azuremodel.define({
                Value1: String,
                Value2: String,
                PartitionKey:  function(model) {
                    return model.Value1;
                },
                RowKey: function(model) {
                    return model.Value2;
                },
                TableName: function () {
                    return "DemoTable";
                },
                QueryMapping: {
                    Value1:     'PartitionKey',
                    Value2:     'RowKey'
                }
            });

            sinon.stub(ModelWithQueryMapping.azureCalls, "queryEntities", function (tableName, tableQuery, currentToken, optionsOrCallback, callback) {
                var results = {
                    entries: [
                        {
                            PartitionKey:   { '$': 'Edm.String', _: 'TV01' },
                            RowKey:         { '$': 'Edm.String', _: 'TV02' },
                            Timestamp:      { '$': 'Edm.DateTime', _: new Date() },
                            '.metadata':    { etag: 'W/"datetime\'2015-01-24T11:37:28.6472064Z\'"'}
                        }
                    ],
                    continuationToken: null };

                optionsOrCallback(null, results, null);
            });

            return ModelWithQueryMapping.query().then(function(results) {
                results.length.should.eql(1);
                results[0].Value1.should.equal('TV01');
                results[0].Value2.should.equal('TV02');
            }).should.be.fulfilled;
        })

    });
});