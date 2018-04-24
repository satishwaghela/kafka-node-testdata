const kafka = require("kafka-node");
var async = require('async');
const uuid = require("uuid");

const zookeeperHost = 'localhost:2181';
const kafkaHost = 'localhost:9092';

const topics = ['topic1', 'topic2', 'topic3'];

const client = new kafka.Client(zookeeperHost, "my-client-id", {
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
        topic: topics[0],
        messages: topics[0] + ' - ' + new Date().toString(),
        //attributes: 1 /* Use GZip compression for the payload */
    },{
        topic: topics[1],
        messages: topics[1] + ' - ' + new Date().toString(),
        key: topics[1] + '-Key'
        //attributes: 1 /* Use GZip compression for the payload */
    }], (a) => {
    	console.log(a);
    });
    producer2.send([{
        topic: topics[2],
        messages: topics[2] + ' - - - ' + new Date().toString(),
        //attributes: 1 /* Use GZip compression for the payload */
    }], (a) => {
    	console.log(a);
    });
}, 50000)

//---------------------------------------

var consumerOptions1 = {
  host: zookeeperHost,
  kafkaHost: kafkaHost,
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};
var consumerGroup1 = new kafka.ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions1), [topics[0], topics[1]]);
consumerGroup1.on('message', (message) => {
	console.log(message);
	return message;
});

var consumerOptions2 = {
  host: zookeeperHost,
  kafkaHost: kafkaHost,
  groupId: 'ExampleTestGroup2',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};
var consumerGroup2 = new kafka.ConsumerGroup(Object.assign({id: 'consumer2'}, consumerOptions2), [topics[2]]);
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