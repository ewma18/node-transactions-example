"use strict";

const uuidv4 = require('uuid/v4');
const { clone } = require('lodash');

class Transaction {
	constructor(config, mainTransaction) {
		if (mainTransaction) {
			let newTransation = clone(mainTransaction);
			newTransation.isSlave = true;
			return newTransation;
		} else {
			this.id = uuidv4();
			this.isOpen = false;
			this.isCommited = false;
			this.hasErrors = false;

			this.client = require('./client')(config);
		}
	}

	beginTransactionAsync() {
		if (this.isSlave) {
			console.log(`This is a slave transaction of ${this.id}, please make sure to begin transaction the main transaction;`);
			return this;
		}
		return this.client.getConnectionForTransactionAsync()
			.then((connection) => {
				this.connection = connection;
				this.connection.transactionId = this.id;
				return this.connection.beginTransactionAsync();
			}).then(() => {
				this.isOpen = true;
				this.client.registerTransaction(this);
				return this;
			});
	}

	queryAsync(query, params) {
		return this.connection.queryAsync(query, params);
	}

	commitAsync() {
		if (this.isSlave) {
			console.log(`This is a slave transaction of ${this.id}, please make sure to commit the main transaction;`);
			return this;
		}
		return this.connection.commitAsync()
			.then(() => {
				this.connection.release();
				this.isOpen = false;
				this.isCommited = true;
			}).catch((err) => {
				console.error(`Error commiting transaction ${this.id}`, err);

				return this.rollbackAsync();
			});
	}

	rollbackAsync() {
		if (this.isSlave) {
			console.log(`This is a slave transaction of ${this.id}, please make sure to rollback the main transaction;`);
			return this;
		}
		console.log(`Rolling back transaction ${this.id}...`);
		this.hasErrors = true;
		this.isOpen = false;
		this.isCommited = false;
		return this.connection.rollbackAsync().then(() => {
			console.log(`Rollback of transaction ${this.id}... OK`);
		}).finally(() => {
			console.log(`Releasing connection of transaction ${this.id}...`);
			this.connection.release();
		});
	}
}

module.exports = Transaction;