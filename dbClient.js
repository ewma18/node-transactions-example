const Promise = require('bluebird');
const mysql = require('mysql');

Promise.promisifyAll(require("mysql/lib/Connection").prototype);
Promise.promisifyAll(require("mysql/lib/Pool").prototype);

const initialMysqlConfig = {
	queryFormat(query, values) {
		/**
		 * This function parses the query, replacing any parameter in the form of
		 *  ':{param}' in the query by the appropriated escaped value of 'values' argument.
		 * ex: select * from Table where id = :id, will replace the ':id' identifier by the 
		 * escaped id property of the 'values' object.
		 */

		if (!values) return query;
		return query.replace(/\:(\w+)/g, (txt, key) => {
			if (values.hasOwnProperty(key)) {
				let value = values[key];
				if (value && value instanceof Function) {
					throw new Error(
						'Error parsing value. Cannot be a function. Key:' + key + '\nQuery:' + query
					);
				}
				return this.escape(value);
			}
			return 'NULL';
		});
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
 * This is useful when we need to connect to more than one
 * database in the same api/system.
 * 
 * @param config
 * @returns {MySqlDbClient}
 */
module.exports = function getDBClient (config) {
    var alias = config.host + (config.database || '');
    var instance = clients[alias];
    if (!instance) {
        clients[alias] = new MySqlDbClient(config);
    }

    return clients[alias];
};
