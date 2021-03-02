const uuidv4 = require('uuid/v4');
const MySqlDbClient = require('./mySqlDbClient.js');

class Transaction {
	constructor () {
		this.id = uuidv4();
	}
	
	beginTransactionAsync () {	
		return MySqlDbClient.getConnectionForTransactionAsync()
			.then((connection)=> {
				this.connection = connection;
				this.connection.transactionId = this.id;
				return this.connection.beginTransactionAsync();
			}).then(() => {
				MySqlDbClient.registerTransaction(this);
				return this;
			});
	}
	queryAsync(query, params) {
		return this.connection.queryAsync(query, params);
	}
	
	commitAsync(){
		return this.connection.commitAsync()
			.then(() => {
				this.connection.release();
			}).catch((err)=> {
				console.error(`Error commiting transaction ${this.id}`, err);
				
				return this.rollbackAsync();
			});
	}
	
	rollbackAsync() {
		console.log(`Rolling back transaction ${this.id}...`);
		return this.connection.rollbackAsync().finally(()=> {
			console.log(`Rollback of transaction ${this.id}... OK`);
			console.log(`Releasing connection of transaction ${this.id}...`);
			this.connection.release();
		});
	}
}

module.exports = Transaction;