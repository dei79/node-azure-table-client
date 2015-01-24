// configure chai
var chai = require("chai");
chai.use(require("chai-as-promised"));

// define a global should
global.should = require('chai').should();

global.sinon  = require("sinon");
require('sinon-as-promised');