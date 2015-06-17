var request        = require('request')
  , _              = require('lodash')
  , fs             = require('fs')
  , async          = require('async')
  , util           = require('util')
  , AbstractPlugin = require('./abstract')
;

/**
 * Constructor
 * @param {object} options Plugin options
 * @param {Influx} storage Storages instances
 */
module.exports = Plugin = function Plugin (options, storages) {
    AbstractPlugin.call(this, options, storages);
};

util.inherits(Plugin, AbstractPlugin);

Plugin.prototype.escape = function(name) {
    return encodeURIComponent(name.replace(/\./g, '_').replace(/\|/g, '.'));
};

/**
 * Write statistic into storage
 * @param  {String}   statName Statistic name
 * @param  {String}   host     Rabbitmq host
 * @param  {Array}    series   Series data
 * @param  {Function} callback
 */
Plugin.prototype.writeStats = function(statName, host, series, callback) {
    var self = this,
        tmpSeries = {};

    var tmp = [];
    _.each(series, function (data) {
        var tags = _.extend({}, data.tags, {type: statName, host: host});
        _.each(data.metrics, function (value, name) {
            tmp.push({
                name: name,
                value: value,
                tags: tags
            });
        });
    }, this);

    this.flush(tmp, callback);
};

/**
 * Get statistic info
 * @param  {string}   host     Rabbitmq host
 * @param  {string}   path     Path
 * @param  {Function} callback
 */
Plugin.prototype.getInfo = function (host, path, callback) {
    var options = this.options[host];
    var url = 'http://{host}:{port}/api/'.format(options);

    request(url + path, {
        auth: {
            user: options.username,
            pass: options.password,
            sendImmediately: true
        }
    }, _.bind(function (err, res, body) {
        if (err) {
            callback.call(this, err);
        } else {
            try {
                callback.call(this, err, JSON.parse(body));
            } catch (error) {
                callback.call(this, error);
            }
        }
    }, this));
};

/**
 * Get value
 * @param  {object} obj          Original object
 * @param  {string} path         Value path separated by dot
 * @param  {mixed}  defaultValue Default value
 * @return {mixed}               Value for path
 */
Plugin.prototype.value = function (obj, path, defaultValue) {
    var paths = path.split('.');

    _.each(paths, function(path) {
        if (obj[path] !== undefined) {
            obj = obj[path];
        } else {
            obj = null;
            return false;
        }
    });

    return obj || defaultValue;
};

/**
 * Get values
 * @param  {object} obj Original values
 * @return {object}     Transformed values
 */
Plugin.prototype.values = function (obj) {
    var tmp = {};
    for (var i = 1; i < arguments.length; i++) {
        var from, to;
        if (_.isString(arguments[i])) {
            from = to = arguments[i];
            tmp[to] = this.value(obj, from, 0);
        } else {
            from = _.values(arguments[i]);
            to   = _.keys(arguments[i]);

            _.each(from, _.bind(function (_from, index) {
                tmp[to[index]] = this.value(obj, _from, 0);
            }, this));
        }
    }

    return tmp;
};

/**
 * Send nodes statistic
 * @param  {string}   host     Rabbitmq host
 * @param  {Function} callback
 */
Plugin.prototype.sendNodesStats = function (host, callback) {
    var self = this;
    this.getInfo(host, 'nodes', function (err, json) {
        if (err) {
            callback(err);
            return;
        }

        var series = [];
        _.each(json || [], function (node) {
            var values = self.values(node, 'mem_used', 'sockets_used', 'disk_free', 'mem_limit', 'sockets_total');

            tmpValues = {
                mem: Math.round(100 * parseFloat(values.mem_used) / parseFloat(values.mem_limit)),
                sockets: Math.round(100 * parseFloat(values.sockets_used) / parseFloat(values.sockets_total)),
                disk_free: values.disk_free
            };

            series.push({
                tags: {name: node.name},
                metrics: tmpValues
            });
        });

        self.writeStats('node', host, series, callback);
    });
};

/**
 * Send virtual hosts statistic
 * @param  {string}   host     Rabbitmq host
 * @param  {Function} callback
 */
Plugin.prototype.sendVhostsStats = function (host, callback) {
    var self = this;

    this.getInfo(host, 'vhosts', function (err, json) {
        if (err) {
            callback(err);
            return;
        }

        var series = [];
        _.each(json || [], function (vhost) {
            series.push({
                tags: {vhost: vhost['name']},
                metrics: self.values(
                    vhost, 'messages', 'messages_ready', {'messages_unack': 'messages_unacknowledged'}
                )
            });
        });

        self.writeStats('vhost', host, series, callback);
    });
};

/**
 * Send exchanges statistic
 * @param  {string}   host     Rabbitmq host
 * @param  {Function} callback
 */
Plugin.prototype.sendExchangesStats = function (host, callback) {
    // TODO: Refactor old implementation
    var self = this;
    this.getInfo(host, 'exchanges', function (err, json) {
        if (err) {
            callback(err);
            return;
        }

        var series = {};
        _.each(json || [], function (exchange) {
            if (false == /^amq\./.test(exchange.name) && exchange.name.length) {
                series[exchange['vhost'] + '|exchange|' + exchange.name] = [
                    self.values(
                        exchange, {'publish_in' : 'message_stats.publish_in'},
                        {'publish_out': 'message_stats.publish_out'},
                        {'publish_in_rate': 'message_stats.publish_in_details.rate'},
                        {'publish_out_rate': 'message_stats.publish_out_details.rate'}
                    )
                ];
            }
        });

        self.writeStats('vhost', host, series, callback);
    });
};

/**
 * Send queues statistic
 * @param  {string}   host     Rabbitmq host
 * @param  {Function} callback
 */
Plugin.prototype.sendQueuesStats = function (host, callback) {
    var self = this;
    this.getInfo(host, 'queues', function (err, json) {
        if (err) {
            callback(err);
            return;
        }

        var series = [];
        _.each(json || [], function (queue) {
            series.push({
                tags: {
                    vhost: queue['vhost'],
                    queue: queue.name
                },
                metrics: self.values(
                    queue, 'messages', 'messages_ready', {'messages_unack': 'messages_unacknowledged'},
                    'consumers'
                )
            });
        });

        self.writeStats('queue', host, series, callback);
    });
};

/**
 * Run plugin
 */
Plugin.prototype.run = function () {
    var self = this;
    process.stdout.write((new Date()) + "\r");
    async.map(_.keys(this.options), _.bind(function (host, callback) {
        async.waterfall([
            _.bind(this.sendNodesStats, this, host),
            _.bind(this.sendVhostsStats, this, host),
            // Refactoring
            // _.bind(this.sendExchangesStats, this, host),
            _.bind(this.sendQueuesStats, this, host)
        ], function (err) {
            if (err) {
                console.error(err.stack);
                process.exit(1);
            }
            callback();
        });
    }, this), function () {
        setTimeout(function() {
            self.run();
        }, 5000);
    });
};