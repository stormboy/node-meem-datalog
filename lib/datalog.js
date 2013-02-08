var util = require('util');
var EventEmitter = require('events').EventEmitter;
var sqlite3 = require('sqlite3').verbose();

var DataLog = function() {
	EventEmitter.call(this);

	this.db = new sqlite3.Database('log.db');
	this._init()
}

util.inherits(DataLog, EventEmitter);

DataLog.prototype.addEntry = function(path, data) {
	var value = data.value;
	var unit = data.unit || null;
	var timestamp = data.timestamp || new Date();
	this.insertStatement.run(path, value, unit, timestamp);
}

DataLog.prototype.close = function() {
	this.db.close();
}

DataLog.prototype._init = function() {
	var self = this;
	this.db.serialize(function() {
		self.db.run("CREATE TABLE IF NOT EXISTS data_log (id INTEGER PRIMARY KEY ASC, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, tz TEXT DEFAULT 'UTC', path TEXT, unit TEXT, value TEXT)");
	});
	self.db.run("CREATE INDEX IF NOT EXISTS index_path ON data_log (path)");
	self.db.run("CREATE INDEX IF NOT EXISTS index_time ON data_log (timestamp)");
	self.db.run("CREATE INDEX IF NOT EXISTS index_path_time ON data_log (path, timestamp)");
	self.insertStatement = self.db.prepare("INSERT INTO data_log (path, value, unit, timestamp) VALUES (?, ?, ?, ?)");
	//self.selectStatement = self.db.prepare("SELECT * FROM data_log WHERE path = ? AND timestamp >= ? AND TIMESTAMP < ?");
	self.emit("ready");
}

DataLog.prototype.getValues = function(path, start, end, callback, finished) {
	var self = this;
	this.db.each("SELECT * FROM data_log WHERE path = ? AND timestamp >= ? AND TIMESTAMP < ?", path, start, end, callback, finished);
	
	//self.selectStatement.each.apply(this.selectStatement, [path, start, end, callback]).finalize()
}
DataLog.prototype.dump = function() {
	this.db.each("SELECT id, timestamp, path, value, unit FROM data_log", function(err, row) {
		console.log(row.id + ": " + row.timestamp + " : " + row.path + " = " + row.value + " : " + row.unit);
	});
}


exports.DataLog = DataLog;
