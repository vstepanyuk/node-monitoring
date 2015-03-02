var request = require('request'),
    _       = require('lodash'),
    fs      = require('fs'),
    Influx  = require('influx'),
    async   = require('async'),
    format  = require('string-template')
;

// Extend core string class
String.prototype.format = function (values) {
    return format(this, values);
};

var config = JSON.parse(fs.readFileSync(__dirname + "/config.json"));
var client = Influx(config.storage);

var plugins = [];

_.each(config['plugins'], function (options, pluginName) {
    plugins.push({name: pluginName, options: options});
});

async.map(plugins, function (pluginData) {
    (new (require('./plugins/' + pluginData.name + '.js'))(pluginData.options, client)).run();
});