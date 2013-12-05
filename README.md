node-meem-datalog
=================

A data store for meem events on MQTT

    npm install
    
To query for data, publish a query to the data-query topic.  The payload of the message is the topic for the DataGimp to
publish results to.
 
Example query providing a "from" time:

        mosquitto_pub -t "/data/log?/path=/house/meter/power/demand&from=2013-02-08T18:49:10.000Z" -m "/response/12345"

Example query with a "from" time, a record "offset" and "max" records parameters:

        mosquitto_pub -h 192.168.0.23 -t "/data/log?/path=/house/meter/energy/in&from=2013-02-08T14:49:10.000Z&offset=400&max=200" -m "/response/12345"

Example query with a "from" time and a "to" time:

        mosquitto_pub -t "/data/log?/path=/house/meter/power/demand&from=2013-02-08T18:49:10.000Z&to=2013-02-15T15:23:35.000Z" -m "/response/12345"

Example response:

        /response/12345 {"path":"/house/meter/energy/in","start":"2013-02-08T14:49:10.000Z","end":"2013-02-10T07:50:19.703Z","count":9,"offset":0,"values":[[1360335187000,"23672800","Wh"],[1360335428000,"23672899","Wh"],[1360335668000,"23673000","Wh"],[1360335908000,"23673000","Wh"],[1360336148000,"23673100","Wh"],[1360336628000,"23673200","Wh"],[1360481670000,"23682300","Wh"],[1360481910000,"23682400","Wh"],[1360482151000,"23682400","Wh"]]}
