/* jshint esnext: true */
require('events').EventEmitter.prototype._maxListeners = 0;
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-c/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-c/lib/email_lib.js');
var functions = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-c/lib/func_lib.js');
var fs = require("fs");
var express = require("express");
var http = require('http');
var https = require('https');
var amqp = require('amqplib/callback_api');
var options = {
    key: fs.readFileSync(config.security.key),
    cert: fs.readFileSync(config.security.cert)
};
var app = express();
var bodyParser = require("body-parser");
var cors = require("cors");
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.raw({ limit: '50mb' }));
app.use(bodyParser.text({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: false }));
app.use(express.static("./public"));
app.use(cors());

app.use(function(req, res, next) {
    next();
});

var server = https.createServer(options, app).listen(config.port.group_e2e_c_port, function() {
    email.sendNewApiGroupE2ECIsUpEmail();
});



/**
 *   RabbitMQ connection object
 */
var amqpConn = null;


/**
 *  Subscribe api-group-e2e-c to topic to receive messages
 */
function subscribeToGroupE2EC(topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiGroupE2EC.*';
            var toipcName = `apiGroupE2EC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.assertQueue(toipcName, { exclusive: false, auto_delete: true }, function(err, q) {
                ch.bindQueue(q.queue, exchange, toipcName);
                ch.consume(q.queue, function(msg) {
                    // check if status ok or error
                    var message = JSON.parse(msg.content.toString());
                     if (toipcName === `apiGroupE2EC.${config.rabbitmq.topics.deleteGroupOneTimePublicKeysByUUIDQ}`){
                        // Check message status, if ok, means that api-group-e2e-q deleted one time keys correctly
                        // If error, means something went wrong, re-send delete one time keys, also start counting re-send times
                        // If re-send times count to high, stop and wait certain amount of time before re-sending again
                    } else if (toipcName === `apiGroupE2EC.${config.rabbitmq.topics.newGroupE2EKeysQ}`){
                        // Check message status, if ok, means that api-group-e2e-q received and processed new group one time keys correctly
                        // If error, means something went wrong, re-send delete one time keys, also start counting re-send times
                        // If re-send times count to high, stop and wait certain amount of time before re-sending again
                    }  else if (toipcName === `apiGroupE2EC.${config.rabbitmq.topics.deleteGroupOneTimePublicKeysByUUIDGC}`){
                        // Group chat message has been received and processed correctly, delete one time key used
                        // delete group key and send delete message to the api-group-e2e-q
                        functions.deleteGroupOneTimeKeysByUUIDS(message.uuids, amqpConn, config.rabbitmq.topics.deleteGroupOneTimePublicKeysByUUIDC);
                    } 
                }, { noAck: true });
            });
        });
    }
}

/**
 *  Connect to RabbitMQ
 */
function connectToRabbitMQ() {
    amqp.connect(config.rabbitmq.url, function(err, conn) {
        if (err) {
            console.error("[AMQP]", err.message);
            return setTimeout(connectToRabbitMQ, 1000);
        }
        conn.on("error", function(err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err.message);
            }
        });
        conn.on("close", function() {
            console.error("[AMQP] reconnecting");
            return setTimeout(connectToRabbitMQ, 1000);
        });
        console.log("[AMQP] connected");
        amqpConn = conn;

        // Subscribe to all the topics
        subscribeToGroupE2EC(config.rabbitmq.topics.deleteGroupOneTimePublicKeysByUUIDC);
        subscribeToGroupE2EC(config.rabbitmq.topics.deleteGroupOneTimePublicKeysByUUIDGC);
        subscribeToGroupE2EC(config.rabbitmq.topics.newGroupE2EKeysQ);
    });
}

connectToRabbitMQ();


/**
 *  POST upload group public one time keys
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
app.post("/uploadGroupPublicKeys", function(req, res) {
    functions.uploadGroupPublicKeys(req, res, amqpConn, config.rabbitmq.topics.newGroupE2EKeys);
});