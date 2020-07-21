const aws = require("aws-sdk");
const { v4: uuidv4 } = require('uuid');

function chunk(arr, chunk_size) {
    var R = [];
    for (var i=0,len=arr.length; i<len; i+=chunk_size)
        R.push(arr.slice(i,i+chunk_size));
    return R;
}


class Queue {
  constructor(option) {
    this.queueOption = {
      QueueUrl: option.QueueUrl,
      MaxNumberOfMessages: option.MaxNumberOfMessages || 10,
      VisibilityTimeout: option.VisibilityTimeout,
      WaitTimeSeconds: option.WaitTimeSeconds || 20,
    };
    this.awsOption = {
      apiVersion: option.apiVersion,
      region: option.region,
    }
    this.sqs = new aws.SQS(this.awsOption);
  }
  async send(messages) {
    let res = await Promise.all(chunk(messages, 10).map(messagesChunk => 
      this.sqs.sendMessageBatch({
        QueueUrl: this.queueOption.QueueUrl,
        Entries: messagesChunk.map(m => ({
          Id: uuidv4(),
          MessageBody: m
        })),
      }).promise()))
    return res;
  }
  async receive(queueSize=10) {
    let messages = [];
    while(true) {
      let res = await this.sqs.receiveMessage({
        QueueUrl: this.queueOption.QueueUrl,
        MaxNumberOfMessages: Math.min(this.queueOption.MaxNumberOfMessages, queueSize - messages.length),
        VisibilityTimeout: this.queueOption.VisibilityTimeout,
        WaitTimeSeconds: Math.min(this.queueOption.WaitTimeSeconds),
      }).promise();
      if(!res.Messages || res.Messages.length == 0)
        break;
      for(let msg of res.Messages){
        try{
          messages.push(msg);
        } catch(e) {
          console.log(`Fail to decode message '${msg.Body}', just skip it`);
        }
      }
      if(messages.length >= queueSize)
        break;
    }
    return messages;
  }
  async delete(messages) {
    let res = await Promise.all(chunk(messages, 10).map(messagesChunk =>
      this.sqs.deleteMessageBatch({
        QueueUrl: this.queueOption.QueueUrl,
        Entries: messagesChunk.map(m => ({
          Id: m.MessageId,
          ReceiptHandle: m.ReceiptHandle ,
        })),
      }).promise()));
    return res;
  }
  async heartbeat(messages) {
    if(this.queueVisibilityTimeout == null)
      this.queueVisibilityTimeout = (await this.sqs.getQueueAttributes({
        QueueUrl: this.queueOption.QueueUrl,
        AttributeNames: ['VisibilityTimeout'],
      }).promise()).Attributes.VisibilityTimeout;
    let res = await Promise.all(chunk(messages, 10).map(messagesChunk =>
      this.sqs.changeMessageVisibilityBatch({
        QueueUrl: this.queueOption.QueueUrl,
        Entries: messagesChunk.map(m => ({
          Id: m.MessageId,
          ReceiptHandle: m.ReceiptHandle,
          VisibilityTimeout: this.queueOption.VisibilityTimeout || this.queueVisibilityTimeout,
        })),
      }).promise()));
    return res;
  }
}

module.exports = Queue;
