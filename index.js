const kafka = require("kafka-node");
var async = require('async');
const uuid = require("uuid");

const client = new kafka.Client("localhost:2181", "my-client-id", {
    sessionTimeout: 300,
    spinDelay: 100,
    retries: 2
});

const producer1 = new kafka.HighLevelProducer(client);
/*producer.createTopics(['t1'], function (err, data) {
	console.log(data);
});*/
producer1.on("ready", function() {
    console.log("Kafka Producer1 is connected and ready.");
});

const producer2 = new kafka.HighLevelProducer(client);
producer2.on("ready", function() {
    console.log("Kafka Producer2 is connected and ready.");
});

setInterval(() => {
	producer1.send([{
        topic: "OtherEvents",
        messages: 'OtherEvents - ' + new Date().toString(),
        //attributes: 1 /* Use GZip compression for the payload */
    },{
        topic: "BulkLoad",
        messages: 'BulkLoad - ' + new Date().toString(),
        key: 'BL-Key'
        //attributes: 1 /* Use GZip compression for the payload */
    }], (a) => {
    	console.log(a);
    });
    producer2.send([{
        topic: "TRUCKEVENTS",
        messages: 'TRUCKEVENTS - - - ' + new Date().toString(),
        //attributes: 1 /* Use GZip compression for the payload */
    }], (a) => {
    	console.log(a);
    });
}, 5000)

//---------------------------------------

var consumerOptions1 = {
  host: '127.0.0.1:2181',
  kafkaHost: 'localhost:9092',
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};
var consumerGroup1 = new kafka.ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions1), ['OtherEvents', 'BulkLoad']);
consumerGroup1.on('message', (message) => {
	console.log(message);
	return message;
});

var consumerOptions2 = {
  host: '127.0.0.1:2181',
  kafkaHost: 'localhost:9092',
  groupId: 'ExampleTestGroup 2',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};
var consumerGroup2 = new kafka.ConsumerGroup(Object.assign({id: 'consumer2'}, consumerOptions2), ['TRUCKEVENTS']);
consumerGroup2.on('message', (message) => {
	console.log(message);
	return message;
});

process.once('SIGINT', function () {
	let count = 0;

    async.each([consumerGroup1, consumerGroup2], function (consumer, callback) {
		consumer.close(true, () => {
			count++;
			if(count == 2){
				process.exit();
			}
		});
	});
});