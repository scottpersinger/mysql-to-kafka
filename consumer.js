var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    knex = require('knex'),
	glob = require("glob"),
	path = require("path"),
    client = new kafka.Client();

var config = require('./config.js');
for (var key in config.databases) {
	config[key] = knex({client:'pg', connection:config.databases[key], debug:true});
}

var handlers = {};
var topics = [];

glob("./scripts/*.js", function(er, files) {
	console.log(files);
	files.forEach(function(f) {
		var mod = require(f);
		if (typeof(mod) == "object") {
			for (var topic in mod) {
				topics.push({topic: topic, partition:0});
				handlers[topic] = mod[topic];
			}
		} else {
			var topic = path.basename(f, '.js');
			topics.push({topic: topic, partition:0});
			handlers[topic] = mod;
		}
	});

	console.log("Topics ", topics);

	var consumer = new Consumer(client, topics);

	consumer.on('message', function(message) {
		try {
			handlers[message.topic](config, JSON.parse(message.value));
		} catch (e) {
			console.log(e);
		}
	});
});


