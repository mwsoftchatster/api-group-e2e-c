/* jshint esnext: true */
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-c/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-c/lib/email_lib.js');


/**
 *  Setup the pool of connections to the db so that every connection can be reused upon it's release
 *
 */
var mysql = require('mysql');
var Sequelize = require('sequelize');
const sequelize = new Sequelize(config.db.name, config.db.user_name, config.db.password, {
    host: config.db.host,
    dialect: config.db.dialect,
    port: config.db.port,
    operatorsAliases: config.db.operatorsAliases,
    pool: {
      max: config.db.pool.max,
      min: config.db.pool.min,
      acquire: config.db.pool.acquire,
      idle: config.db.pool.idle
    }
});

/**
 *  Publishes message on api-group-e2e-q topic with newly generated one time keys batch
 */
function publishOnGroupE2EQ(amqpConn, message, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiGroupE2EQ.*';
            var toipcName = `apiGroupE2EQ.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, toipcName, new Buffer(message));
        });
    }else {
        // log and send error
        email.sendApiE2ECErrorEmail('API Group E2E C publishOnGroupE2EQ AMPQ connection was null');
    }
}

/**
 *  Publishes message on api-group-chat-c topic with newly generated one time keys batch
 */
function publishOnGroupChatC(amqpConn, message, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiGroupChatC.*';
            var toipcName = `apiGroupChatC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, toipcName, new Buffer(message));
        });
    }else {
        // log and send error
        email.sendApiGroupE2ECErrorEmail('API Group Group E2E C publishOnChatC AMPQ connection was null');
    }
}


/**
 * Model of group_one_time_pre_key_pair table
 * 
 */
const OneTimeGroupPreKey = sequelize.define('group_one_time_pre_key_pair', {
    user_id: { 
        type: Sequelize.INTEGER,
        allowNull: false
    },
    group_id: { 
        type: Sequelize.STRING,
        allowNull: false
    },
    group_one_time_pre_key_pair_pbk: {type: Sequelize.BLOB('long'), allowNull: false},
    group_one_time_pre_key_pair_uuid: {type: Sequelize.STRING, allowNull: false}
}, {
  freezeTableName: true, // Model tableName will be the same as the model name
  timestamps: false,
  underscored: true
});


/**
 *  Saves public one time keys into db
 * 
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.uploadGroupPublicKeys = function(req, res, amqpConn, topic){
    var oneTimeGroupPreKeyPairPbks = JSON.parse(req.query.oneTimeGroupPreKeyPairPbks);

    OneTimeGroupPreKey.bulkCreate(oneTimeGroupPreKeyPairPbks.oneTimeGroupPreKeyPairPbks, { fields: ['user_id','group_id', 'group_one_time_pre_key_pair_pbk', 'group_one_time_pre_key_pair_uuid'] }).then(() => {
        publishOnGroupE2EQ(amqpConn, req.query.oneTimeGroupPreKeyPairPbks, topic);
        res.json("success");
    }).error(function(err){
        email.sendApiGroupE2ECErrorEmail(err);
        res.json("error");
    });
};

/**
 * Deletes all public group one time keys used for one message
 * 
 * (uuids string): Comma separated list of uuids of the public group one time keys to be deleted
 * (ampqConn Object): RabbitMQ connection object used to send messages via messaging queue
 * (topic string): Topic on which successful processing verification message is sent
 */
module.exports.deleteGroupOneTimeKeysByUUIDS = function (uuids, amqpConn, topic) {
    sequelize.query('CALL DeleteGroupOneTimePublicKeysByUUID(?)',
    { replacements: [ uuids ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            var response = {
                status: config.rabbitmq.statuses.ok
            };
            publishOnGroupChatC(amqpConn, JSON.stringify(response), topic);
            publishOnGroupE2EQ(amqpConn, uuids, config.rabbitmq.topics.deleteOneTimePublicKeysByUUID);
    }).error(function(err){
        email.sendApiGroupE2ECErrorEmail(err);
        var response = {
            status: config.rabbitmq.statuses.error
        };
        publishOnGroupChatC(amqpConn, JSON.stringify(response), topic);
    });
}