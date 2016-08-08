'use strict';

var _ = require('lodash');
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

function receiveMessages(options) {
    var defaultOptions = {
        MaxNumberOfMessages: 10,
        VisibilityTimeout: 60,
        WaitTimeSeconds: 20
    };

    options = _.merge(defaultOptions, options);

    return new Promise(function (resolve, reject) {
        sqs.receiveMessage({
            QueueUrl: awsConfig.sqsQueueUrl,
            MaxNumberOfMessages: options.MaxNumberOfMessages,
            VisibilityTimeout: options.VisibilityTimeout,
            WaitTimeSeconds: options.WaitTimeSeconds
        }, function (err, data) {
            if (err) {
                return reject(err);
            }
            console.log("Received:", data.Messages.length);
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
        console.log("Deleting: ", message.ReceiptHandle);
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
    receiveMessages({ MaxNumberOfMessages: 3 }).then(function (data) {
        return Promise.all(data.Messages.map(function (message) {
            var origMessage = message;
            return processMessage(message).then(function (body) {
                console.log({ old: origMessage.Body, new: JSON.stringify(body) });
                return body;
            }).then(function (body) {
                return sendMessage(body)
            }).catch(function (err) {
                console.log(err);
            }).finally(function () {
                return deleteMessage(origMessage);
            });
        })).finally(function () {
            listener();
        }).catch(function (err) {
            console.log(err);
        });
    });
};