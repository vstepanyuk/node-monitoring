var request = require('request'),
    _       = require('lodash'),
    fs      = require('fs'),
    async   = require('async')
;

/**
 * Constructor
 * @param {object} options Plugin options
 * @param {Influx} storage Storage instance
 */
module.exports = Plugin = function Plugin (options, storage) {
    this.name = "rabbitmq";
    this.options = options;
    this.storage = storage;
};

/**
 * Write statistic into storage
 * @param  {string}   statName Statistic name
 * @param  {string}   host     Rabbitmq host
 * @param  {array}    series   Series data
 * @param  {Function} callback
 */
Plugin.prototype.writeStats = function(statName, host, series, callback) {
    var self = this,
        now = new Date(),
        tmpSeries = {};

    _.each(series, function (points, name) {
        name = '{plugin}|{host}|{name}|{subname}'.format({plugin: self.name, host: host, name: statName, subname: name});
        tmpSeries[name] = _.each(points, function(point) {
            point['date'] = now;
        });
    });

    this.storage.writeSeries(tmpSeries, {}, _.bind(function(err) {
        callback.call(this, err);
    }, this));
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

        var series = {};
        _.each(json || [], function (node) {
            series[node.name] = [self.values(node, 'mem_used', 'fd_used', 'proc_used')];
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

        var series = {};
        _.each(json || [], function (vhost) {
            series[vhost['name']] = [
                self.values(
                    vhost, 'messages', 'messages_ready', {'messages_unack': 'messages_unacknowledged'}, 
                    {'messages_rate': 'messages_details.rate'}, {'messages_ready_rate': 'messages_ready_details.rate'},
                    {'messages_unack_rate': 'messages_unacknowledged_details.rate'}
                )
            ];
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

        var series = {};
        _.each(json || [], function (queue) {
            series[queue['vhost'] + '|queue|' + queue.name] = [
                self.values(
                    queue, 'messages', 'messages_ready', {'messages_unack': 'messages_unacknowledged'},
                    {'messages_rate': 'messages_details.rate'},
                    {'messages_ready_rate': 'messages_ready_details.rate'},
                    {'messages_unack_rate': 'messages_unacknowledged_details.rate'},
                    'consumers',
                    {'ack' : 'message_stats.ack'}, 
                    {'ack_rate' : 'message_stats.ack_details.rate'}, 
                    {'deliver': 'message_stats.deliver'},
                    {'deliver_rate' : 'message_stats.deliver_details.rate'}, 
                    {'get': 'message_stats.get'}, 
                    {'get_rate' : 'message_stats.get_details.rate'}, 
                    {'publish': 'message_stats.publish'},
                    {'publish_rate' : 'message_stats.publish_details.rate'}
                )
            ];
        });

        self.writeStats('vhost', host, series, callback);
    });
};

/**
 * Run plugin
 */
Plugin.prototype.run = function () {
    var self = this;
    async.map(_.keys(this.options), _.bind(function (host, callback) {
        async.waterfall([
            _.bind(this.sendNodesStats, this, host),
            _.bind(this.sendVhostsStats, this, host),
            _.bind(this.sendExchangesStats, this, host),
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
        }, 500);
    });
};