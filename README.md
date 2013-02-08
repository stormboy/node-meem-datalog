node-meem-datalog
=================

A data store for meem events on MQTT

    npm install sqlite
    npm install mqttjs
    npm install time
    
To query for data, publish a query to the data-query topic.  The payload of the message is the topic for the DataGimp to
publish results to.
 
e.g. 
    mosquitto_pub -t "/data/log?/path=/house/meter/power/demand&from=2013-02-08T18:49:10.000Z" -m "/response/12345"
 
    mosquitto_pub -t "/data/log?/path=/house/meter/power/demand&from=2013-02-08T18:49:10.000Z&to=2013-02-15T15:23:35.000Z" -m "/response/12345"
