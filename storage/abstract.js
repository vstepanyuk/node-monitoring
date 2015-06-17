var path = require('path');

module.exports = Abstract = function Abstract(options) {
    this.options = options;
    this.name    = path.parse(module.parent.id)['name'];
    this.debug   = !!process.env.NODE_DEBUG && (new RegExp('\\bstorage_' + this.name  + '\\b')).test(process.env.NODE_DEBUG);
};

Abstract.prototype.write = function (data, callback) {
    callback(new Error(this.name + " 'write' method doesn't implemented!"));
};