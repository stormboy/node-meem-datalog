var util = require("util");
var meem = require("meem");
var DataLog = require("./datalog").DataLog;
var time = require('time')(Date);


var TRACE = true;
var DETAIL = false;

function parseQueryString(query) {
	var result = {};
	var vars = query.split('&');
    for (var i = 0; i < vars.length; i++) {
        var pair = vars[i].split('=');
        var name = decodeURIComponent(pair[0]);
        result[name] = decodeURIComponent(pair[1]);
    }
    return result;
}

/**
 * To query for data, publish a query to the data-query topic.  The payload of the message is the topic for the DataGimp to
 * publish results to.
 * 
 * e.g. 
 * mosquitto_pub -t "/data/log?/path=/house/meter/power/demand&from=2013-02-08T18:49:10.000Z" -m "/response/12345"
 * 
 * mosquitto_pub -t "/data/log?/path=/house/meter/power/demand&from=2013-02-08T18:49:10.000Z&to=2013-02-15T15:23:35.000Z" -m "/response/12345"
 */


var DataLogger = exports.DataLogger = function DataLogger(def) {
	meem.Meem.call(this, def, this._getProperties(def.properties), this._getFacets());
	
	// init database
	var settings = {
		db: this.getPropertyValue("db") || "log.db"
	};
	this.dataLog = new DataLog(settings);

	var self = this;
	
	/*
	this.on("connect", function(meemBus) {
		var monitorTopic = self.getPropertyValue("topic");
		meemBus._mqttClient.subscribe(monitorTopic);
		meemBus._mqttClient.on("message", function(topic, payload) {
			
		});
	});
	this.on("disconnect", function(meemBus) {
		var monitorTopic = self.getPropertyValue("topic");
		meemBus._mqttClient.unsubscribe(monitorTopic);
	});
	*/

	var mqttOptions = options.mqtt || {};
	var mqttHost = mqttOptions.host || "127.0.0.1";	// host for the MQTT server to connect to
	var mqttPort = mqttOptions.port || 1833;			// port for the MQTT server
	var clientId = "meem_" + crypto.randomBytes(8).toString('hex');

	this._mqttClient = mqtt.createClient(mqttPort, mqttHost, {
		keepalive: 10000,
		client : clientId
	});

	this._mqttClient.addListener('message', function(topic, payload) {
		// got data from subscribed topic
		if (TRACE) {
			console.log('received ' + topic + ' : ' + payload);
		}

		// don't log content-requests (containing '?')
		if (topic.indexOf("?") < 0) {
			try {
				var message = JSON.parse(payload);
				self._handleInput(topic, message);
			}
			catch (e) {
			}
		}
	});
	
	// subscribe to topic to log messages on
	var monitorTopic = self.getPropertyValue("topic");
	this._mqttClient.subscribe(monitorTopic);
	this.on("property", function(name, value, oldValue) {
		if (name == "topic") {
			if (oldValue) {
				self._mqttClient.unsubscribe(oldValue);
			}
			self._mqttClient.subscribe(value);
		}
	});
};
util.inherits(DataLogger, meem.Meem);

DataLogger.prototype._getProperties = function(config) {
	var properties = {
		topic: {
			description: "MQTT topic to monitor for log",
			type: String,
			value: "home/#"
		},
		db: {
			description: "path to database",
			type: String,
			value: "log.db"
		}
	};
	return properties;
};

/**
 * Define the facets for this Meem.
 */
DataLogger.prototype._getFacets = function() {
	var self = this;

	var handleDataIn = function(message) {
		//addEntry(message.topic, message.data);
	};

	var handleDataOutRequest = function(request) {
		self._handleContentRequest(request);
	};

	var facets = {
		dataIn: {
			type: "org.meemplex.DataLog", 
			direction: meem.Direction.IN, 
			description: "data to log",
			handleMessage: handleDataIn
		},
		dataOut: {
			type: "org.meemplex.DataLog", 
			direction: meem.Direction.OUT, 
			description: "data from the data logger",
			handleContentRequest: handleDataOutRequest
		}
	};
	return facets;
};

DataLogger.prototype._handleInput = function(path, message) {
	if (TRACE) {
		console.log('received ' + path + ' : ' + message);
	}

	try {
		//var message = JSON.parse(payload);
		if (message.timestamp) {
			message.timestamp = new time.Date(message.timestamp);
		}
		this.dataLog.addEntry(path, message);
	}
	catch (e) {
	}
};



DataLogger.prototype._handleContentRequest = function(request) {
	var self = this;
	var query = request.query;
	try {
		// get these query values from query
		var path  = query.path; 										// "/house/meter/power/demand";
		var start = query.from ? new Date(query.from) : new Date();		// new Date("2012-05-23T13:11:53.002Z");
		var end   = query.to ? new Date(query.to) : new Date();				// new Date("2013-05-23T13:11:53.002Z");
		var offset = query.offset ? query.offset : 0;				// offset in the database
		var max = query.max ? query.max : 500;						// max number of records to return 
		
		if (TRACE) {
			console.log("getting " + path + " values from " + start + " to " + end);
		}
		
		var data = { path : path, start : start, end : end, count : 0, offset : offset, values : [] };
	
		this.dataLog.db.serialize(function() {
			self.dataLog.getValueCount(path, start, end, function(err, row) {
				console.log("count: " + JSON.stringify(err) + " " + JSON.stringify(row));
				data.count = row.count;
			});
			self.dataLog.getValues(path, start, end, offset, max, function(err, row) {
				// var d = { timestamp : row.timestamp, value : row.value, unit : row.unit };
				var d = [row.timestamp, row.value, row.unit];
				data.values.push(d);
			}, function() {
				if (TRACE && DETAIL) {
					console.log("sending content: " + JSON.stringify(data));
				}
				request.respond(JSON.stringify(data));		// send response data
			});
		});
	}
	catch (e) {
		console.log("DataLogger: exception when satisfying query: " + e);
	}
};