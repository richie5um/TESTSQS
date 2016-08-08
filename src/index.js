'use strict';

var AWS = require('aws-sdk');
var FS = require('fs');
var Promise = require('bluebird');

var awsCredentialsPath = './aws.credentials.json';
var awsConfigPath = './aws.config.json';

AWS.config.loadFromPath(awsCredentialsPath);
var sqs = new AWS.SQS();

var awsConfig = JSON.parse(FS.readFileSync(awsConfigPath));

sendMessage({ key: 'seed', value: 1 });
listener();

function receiveMessage() {
    return new Promise(function (resolve, reject) {
        sqs.receiveMessage({
            QueueUrl: awsConfig.sqsQueueUrl,
            MaxNumberOfMessages: 10,
            VisibilityTimeout: 60,
            WaitTimeSeconds: 20
        }, function (err, data) {
            if (err) {
                return reject(err);
            }
            return resolve(data);
        });
    });
}

function sendMessage(body) {
    return new Promise(function (resolve, reject) {
        sqs.sendMessage({
            QueueUrl: awsConfig.sqsQueueUrl,
            MessageBody: JSON.stringify(body)
        }, function (err, data) {
            if (err) {
                return reject(err);
            }
            return resolve(data);
        });
    });
}

function deleteMessage(message) {
    return new Promise(function (resolve, reject) {
        sqs.deleteMessage({
            QueueUrl: awsConfig.sqsQueueUrl,
            ReceiptHandle: message.ReceiptHandle
        }, function (err, data) {
            if (err) {
                return reject(err);
            }
            return resolve(data);
        });
    });
};

function processMessage(message) {
    return new Promise(function (resolve, reject) {
        var body = JSON.parse(message.Body);
        body.value = body.value + 1;
        return resolve(body);
    });
};

function listener() {
    receiveMessage().then(function (data) {
        return Promise.each(data.Messages, function (message) {
            var origMessage = message;
            return processMessage(message)
                .then(function (body) {
                    console.log({ old: origMessage.Body, new: JSON.stringify(body) });
                    return body;
                })
                .then(function (body) {
                    return sendMessage(body)
                })
                .finally(function () {
                    return deleteMessage(origMessage);
                });
            })
            .finally(function () {
                listener();
            });
    });
};