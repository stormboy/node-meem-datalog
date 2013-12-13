var DataLog = require("../lib/datalog").DataLog,
	time = require('time')(Date),
	mqtt = require('mqtt'),
   	crypto = require('crypto');

var TRACE = true;
var DETAIL = true;

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

var DataGimp = function(settings) {
	this.TOPIC_query = settings.queryTopic;
	this.TOPIC_data = settings.dataTopics[0];
	this.dataLog = new DataLog(settings);
	
	this._init({port : settings.mqttPort, host : settings.mqttHost});
};


DataGimp.prototype._init = function(options) {
	if (TRACE) {
		console.log("initialising");
	}
	var self = this;
	
	// connect to MQTT service
	var clientId = "datalog_" + crypto.randomBytes(8).toString('hex');		// create a random client ID for MQTT
	self.mqttClient = mqtt.createClient(options.port, options.host, {
			keepalive: 60,
			client: clientId
		});

	// add handlers to MQTT client
	self.mqttClient.on('connect', function() {
		if (TRACE) {
			console.log('MQTT sessionOpened');
		}
		self._subscribe();	// subscribe to control and request topics
	});
	self.mqttClient.on('close', function() {
		console.log('MQTT close');
	});
	self.mqttClient.on('error', function(e) {
		console.log('MQTT error: ' + e);
	});
	self.mqttClient.on('message', function(topic, payload) {
		// got data from subscribed topic
		// check if message is a request for current value, send response
		var i = topic.indexOf("?");
		if (i > 0) {
			self._handleContentRequest(topic, payload);
		}
		else {
			self._handleInput(topic, payload);
		}
	});

};

DataGimp.prototype._subscribe = function() {
	if (this.mqttClient) {
		this.mqttClient.subscribe(this.TOPIC_data);		// topic for incoming data to store
		this.mqttClient.subscribe(this.TOPIC_query+"?/#");	// topic for data queries
	}
};

DataGimp.prototype._handleContentRequest = function(topic, payload) {
	var self = this;
	var i = topic.indexOf("?");
	var requestTopic = topic.slice(0, i);
	var queryString = topic.slice(i+2);
	var query = parseQueryString(queryString);
	
	var responseTopic = payload;
	if (TRACE) {
		console.log("requestTopic: " + requestTopic + "; query: " + JSON.stringify(query) + "; responseTopic: " + responseTopic);
	}
	
	if (requestTopic == this.TOPIC_query) {
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
					self.mqttClient.publish(responseTopic, JSON.stringify(data));
				});
			});
		}
		catch (e) {
			console.log("exception when satisfying query: " + e);
		}
	}
};

DataGimp.prototype._handleInput = function(topic, payload) {
	if (TRACE) {
		console.log('received ' + topic + ' : ' + payload);
	}

	var path = topic;
	var msg = JSON.parse(payload);
	if (msg.timestamp) {
		msg.timestamp = new time.Date(msg.timestamp);
	}
	this.dataLog.addEntry(path, msg);
};


module.exports = DataGimp;
