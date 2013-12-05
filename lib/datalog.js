var util = require('util');
var EventEmitter = require('events').EventEmitter;
var sqlite3 = require('sqlite3').verbose();

var DataLog = function(options) {
	EventEmitter.call(this);
	this.db = new sqlite3.Database(options.db || 'log.db');
	this._init();
};

util.inherits(DataLog, EventEmitter);

/**
 * Add an entry to the data log.
 */
DataLog.prototype.addEntry = function(path, data) {
	var value = data.value;
	var unit = data.unit || null;
	var timestamp = data.timestamp || new Date();
	this.insertStatement.run(path, value, unit, timestamp);
};

/**
 * Close the database.
 */
DataLog.prototype.close = function() {
	this.insertStatement.finalize();
	this.selectStatement.finalize();
	this.countStatement.finalize();
	this.db.close();
};

/**
 * Initialise the database.
 */
DataLog.prototype._init = function() {
	var self = this;
	
	// setup database
	this.db.serialize(function() {
		self.db.run("CREATE TABLE IF NOT EXISTS data_log (id INTEGER PRIMARY KEY ASC, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, tz TEXT DEFAULT 'UTC', path TEXT, unit TEXT, value TEXT)");
	});
	self.db.run("CREATE INDEX IF NOT EXISTS index_path ON data_log (path)");
	self.db.run("CREATE INDEX IF NOT EXISTS index_time ON data_log (timestamp)");
	self.db.run("CREATE INDEX IF NOT EXISTS index_path_time ON data_log (path, timestamp)");
	
	// create prepared statements
	self.insertStatement = self.db.prepare("INSERT INTO data_log (path, value, unit, timestamp) VALUES (?, ?, ?, ?)");
	self.selectStatement = self.db.prepare("SELECT * FROM data_log WHERE path = ? AND timestamp >= ? AND TIMESTAMP < ? LIMIT ?, ?");
	self.countStatement = self.db.prepare("SELECT COUNT(*) AS count FROM data_log WHERE path = ? AND timestamp >= ? AND TIMESTAMP < ?");
	
	self.emit("ready");
};

/**
 * 
 * @param {Object} path
 * @param {Object} from
 * @param {Object} to
 * @param {Object} callback
 * @param {Object} complete
 */
DataLog.prototype.getValueCount = function(path, from, to, callback) {
	//this.db.get("SELECT COUNT(*) FROM data_log WHERE path = ? AND timestamp >= ? AND TIMESTAMP < ?", path, from, to, callback);
	this.countStatement.each(path, from, to, callback);
};

/**
 * 
 * @param {Object} path
 * @param {Object} from
 * @param {Object} to
 * @param {Object} callback
 * @param {Object} complete
 */
DataLog.prototype.getValues = function(path, from, to, offset, max, callback, complete) {
	//var offset = 0;
	//var max = 500;
	//this.db.each("SELECT * FROM data_log WHERE path = ? AND timestamp >= ? AND TIMESTAMP < ? LIMIT " + offset + ", " + max, path, from, to, callback, complete);
	this.selectStatement.each(path, from, to, offset, max, callback, complete);
};

DataLog.prototype.dump = function() {
	this.db.each("SELECT id, timestamp, path, value, unit FROM data_log", function(err, row) {
		console.log(row.id + ": " + row.timestamp + " : " + row.path + " = " + row.value + " : " + row.unit);
	});
};


exports.DataLog = DataLog;
