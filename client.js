"use strict";

const Promise = require('bluebird');
const { clone, map, partialRight, pick, toArray, values, keys, chunk, flatten } = require('lodash');
const mysql = require('mysql');
const DateTime = require('luxon').DateTime;

Promise.promisifyAll(require("mysql/lib/Connection").prototype);
Promise.promisifyAll(require("mysql/lib/Pool").prototype);


function twoDigits(d) {
    if (0 <= d && d < 10) return "0" + d.toString();
    if (-10 < d && d < 0) return "-0" + (-1 * d).toString();
    return d.toString();
}

function toMysqlFormat(date) {
    return date.getUTCFullYear() + "-" + twoDigits(1 + date.getUTCMonth()) + "-" +
        twoDigits(date.getUTCDate()) + " " + twoDigits(date.getUTCHours()) + ":" +
        twoDigits(date.getUTCMinutes()) + ":" + twoDigits(date.getUTCSeconds());
}

var initialMysqlConfig = {
    queryFormat: function (query, values) {
        if (!values) return query;
        var match = query.match(/\:(\w+)\(([\w\:\s,]+)\)/);
        while (match) {
            const key = match[1];
            if (values.hasOwnProperty(key) && Array.isArray(values[key])) {
                values = clone(values);
                const coluns = match[2].replace(/[\s:]+/g, '').split(',');
                let value = values[key];
                const finalValue = map(value, partialRight(pick, coluns));
                values[key] = map(finalValue, toArray);
            }
            query = query.slice(0, match.index + match[1].length + 1) + query.slice(match.index + match[0].length);
            match = query.match(/\:(\w+)\(([\w\:\s,]+)\)/);
        }
        return query.replace(/\:(\w+)/g, function (txt, key) {
            if (values.hasOwnProperty(key)) {
                let value = values[key];
                if (value && value instanceof Function) {
                    throw new Error("Error parsing value. Cannot be a function. Key:" + key + "\nQuery:" + query);
                }
                if (value && value instanceof DateTime) {
                    value = toMysqlFormat(new Date(value));
                } else if (value && value instanceof Date) {
                    value = toMysqlFormat(value);
                }
                return this.escape(value);
            }
            return 'NULL';
        }.bind(this));
    }
};

const clients = {};

class MySqlDbClient {
    constructor(config) {
        this.mysqlConfig = Object.assign({}, initialMysqlConfig, config);
        this.connectionPool = mysql.createPool(this.mysqlConfig);

        this.openTransactions = {};

        this.connectionPool.on('release', (connection) => {
            if (connection.transactionId && this.openTransactions[connection.transactionId]) {
                delete this.openTransactions[connection.transactionId];
            }
        });
    }

    getConnectionAsync(transaction) {
        if (transaction) {
            return Promise.resolve(transaction);
        } else {
            return Promise.resolve(this);
        }
    }

    queryAsync(query, params) {
        return this.connectionPool.queryAsync(query, params);
    }

    async queryDirtyAsync(query, params) {
        query = `SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
        ${query}
        SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;`;

        return (await this.queryAsync(query, params))[1];
    }

    async queryBatchAsync(query, params, batchSize) {
        var values = values(params);
        if (values && Array.isArray(values[0]) && batchSize && values[0].length > batchSize) {
            const key = keys(params)[0];

            const results = await Promise.all(
                map(chunk(values[0], batchSize), function (batch) {
                    const empty = {};
                    empty[key] = batch;
                    return this.queryAsync(query, empty);
                }.bind(this))
            );
            return flatten(results);
        } else {
            return this.queryAsync(query, params);
        }
    }

    registerTransaction(transaction) {
        if (this.openTransactions[transaction.id])
            throw new Error("Transaction with id: ${transaction.id} already registered");
        this.openTransactions[transaction.id] = transaction;

        return transaction;
    }

    getConnectionForTransactionAsync() {
        return this.connectionPool.getConnectionAsync();
    }
}

/**
 * Returns a mysql client based on the configs passed
 * @param config
 * @returns {MySqlDbClient}
 */
module.exports = function (config) {
    var alias = config.host + (config.database || '');
    var instance = clients[alias];
    if (!instance) {
        clients[alias] = new MySqlDbClient(config);
    }

    return clients[alias];
};
