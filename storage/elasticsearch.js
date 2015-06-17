var AbstractStorage = require('./abstract')
  , util            = require('util')
  , elasticsearch   = require('elasticsearch')
  , _               = require('lodash')
  , async           = require('async')
  , uniqid          = require('uniqid')
  ;

module.exports = ElasticSearchStorage = function ElasticSearchStorage (options) {
    AbstractStorage.call(this, options);
    this.client = new elasticsearch.Client(_.omit(options, 'index'));
};

util.inherits(ElasticSearchStorage, AbstractStorage);

ElasticSearchStorage.prototype.write = function(data, callback) {
    if (this.debug) {
        console.error('Storage %s data:', this.name);
        console.error(data);
    }

    if (!(data instanceof Array)) {
        data = [data];
    }

    var indexOptions = this.options.index,
        now          = new Date();

    var q = async.queue(function (task, callback) {
        var document = {
            index: indexOptions.name,
            type : indexOptions.type,
            id   : uniqid(),
            body : {
                name     : task.name,
                tags     : task.tags,
                value    : task.value,
                timestamp: now
            },
            timestamp: now
        };

        this.client.create(document, callback);
    }.bind(this), 5);

    q.drain = callback.bind(this);

    data.forEach(function (item) {
        q.push(item);
    });
};