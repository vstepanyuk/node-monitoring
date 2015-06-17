var async = require('async')
  , path = require('path')
  ;

/**
 * Abstract plugin
 * @param {Object} options  Plugin options
 * @param {Array}  storages Storages instances
 */
module.exports = AbstractPlugin = function AbstractPlugin (options, storages) {
    this.options  = options;
    this.storages = storages;
    this.name     = path.parse(module.parent.id)['name'];
    this.debug    = process.env.NODE_DEBUG && (new RegExp('\bplugin_' + module.id  + '\b')).test(process.env.NODE_DEBUG);
};

/**
 * Flush all data to storages
 * @param  {Array|String}   data     Data to flush
 * @param  {Function}       callback Callback function
 */
AbstractPlugin.prototype.flush = function (data, callback) {
    var q = async.queue(function (task, callback) {
        task.storage.write(task.data, callback);
    }, 5);

    q.drain = callback.bind(this);
    this.storages.forEach(function (storage) {
        q.push({storage: storage, data: data});
    });
};