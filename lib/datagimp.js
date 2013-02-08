var DataLog = require("./datalog").DataLog,
	time = require('time')(Date),
	mqtt = require('mqttjs'),
   	crypto = require('crypto');

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

var DataGimp = function(settings) {
	this.TOPIC_query = settings.queryTopic;
	this.TOPIC_data = settings.dataTopics[0];
	this.dataLog = new DataLog();
	
	this._init({port : settings.mqttPort, host : settings.mqttHost});
}


DataGimp.prototype._init = function(options) {
	var self = this	
	// connect to MQTT service
	
	mqtt.createClient(options.port, options.host, function(err, client) {
		self.mqttClient = client;

		// add handlers to MQTT client
		self.mqttClient.on('connack', function(packet) {
			if (packet.returnCode === 0) {
				if (TRACE) {
					console.log('MQTT sessionOpened');
				}
				self._subscribe();	// subscribe to control and request topics
				self._startPing();
			}
		});
		self.mqttClient.on('close', function() {
			console.log('MQTT close');
		});
		self.mqttClient.on('error', function(e) {
			console.log('MQTT error: ' + e);
		});
		self.mqttClient.on('publish', function(packet) {
			// got data from subscribed topic
			if (TRACE) {
				console.log('received ' + packet.topic + ' : ' + packet.payload);
			}

			// check if message is a request for current value, send response
			var i = packet.topic.indexOf("?");
			if (i > 0) {
				self._handleContentRequest(packet);
			}
			else {
				self._handleInput(packet);
			}
		});

        // connect to MQTT service
		crypto.randomBytes(24, function(ex, buf) {		// create a random client ID for MQTT
			var clientId = buf.toString('hex');
			self.mqttClient.connect({
				keepalive: 60,
				client: clientId
			});
		});

	});
}

DataGimp.prototype._subscribe = function() {
	if (this.mqttClient) {
		this.mqttClient.subscribe({topic: this.TOPIC_data});		// topic for incoming data to store
		this.mqttClient.subscribe({topic: this.TOPIC_query+"?/#"});	// topic for data queries
	}
}

DataGimp.prototype._handleContentRequest = function(packet) {
	var self = this;
	var i = packet.topic.indexOf("?");
	var requestTopic = packet.topic.slice(0, i);
	var queryString = packet.topic.slice(i+2);
	var query = parseQueryString(queryString);
	
	var responseTopic = packet.payload;
	if (TRACE) {
		console.log("requestTopic: " + requestTopic + "; query: " + JSON.stringify(query) + "; responseTopic: " + responseTopic);
	}
	
	if (requestTopic == this.TOPIC_query) {
		try {
			// get these query values from query
			var path  = query.path; 										// "/house/meter/power/demand";
			var start = query.from ? new Date(query.from) : new Date();		// new Date("2012-05-23T13:11:53.002Z");
			var end   = query.to ? new Date(query.to) : new Date();				// new Date("2013-05-23T13:11:53.002Z");
			
			if (TRACE) {
				console.log("getting " + path + " values from " + start + " to " + end);
			}
			
			var data = { path : path, start : start, end : end, values : [] };
			
			this.dataLog.getValues(path, start, end, function(err, row) {
				// var d = { timestamp : row.timestamp, value : row.value, unit : row.unit };
				var d = [row.timestamp, row.value, row.unit];
				data.values.push(d);
			}, function() {
				if (TRACE && DETAIL) {
					console.log("sending content: " + JSON.stringify(data));
				}
				self.mqttClient.publish({topic: responseTopic, payload: JSON.stringify(data)});
			});
		}
		catch (e) {
			console.log("exception when satisfying query: " + e);
		}
	}
}

DataGimp.prototype._handleInput = function(packet) {
	var path = packet.topic;
	var msg = JSON.parse(packet.payload);
	if (msg.timestamp) {
		msg.timestamp = new time.Date(msg.timestamp);
	}
	this.dataLog.addEntry(path, msg);
}

DataGimp.prototype._startPing = function() {
    if (this.pingTimer) {
        clearTimeout(this.pingTimer);
    }
    var self = this;
    this.pingTimer = setTimeout(function() {
        self._ping();
    }, 60000);        // make sure we ping the server 
}

DataGimp.prototype._stopPing = function() {
    if (this.pingTimer) {
        clearTimeout(this.pingTimer);
    }
}

DataGimp.prototype._ping = function() {
	var self = this;
    if (self.mqttClient) {
        if (TRACE && DETAIL) {
            console.log("pinging MQTT server");
        }
        self.mqttClient.pingreq();
        self.pingTimer = setTimeout(function() {
            self._ping();
        }, 60000);
    }
}


exports.DataGimp = DataGimp;
