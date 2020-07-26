//import aws from 'aws-sdk';
import SQS from 'aws-sdk/clients/sqs';
import { v4 as uuidv4 } from 'uuid';

function chunk<T>(arr: T[], chunk_size: number): T[][] {
    var R = [];
    for (var i=0,len=arr.length; i<len; i+=chunk_size)
        R.push(arr.slice(i,i+chunk_size));
    return R;
}

export interface Option {
  QueueUrl: string,
  MaxNumberOfMessages?: number,
  VisibilityTimeout?: number,
  WaitTimeSeconds?: number,
}

export default class Queue {
  option: Option;
  sqs: SQS;
  queueVisibilityTimeout?: number | string;
  constructor(option: Option, awsOption: SQS.Types.ClientConfiguration) {
    this.option = {
      QueueUrl: option.QueueUrl,
      MaxNumberOfMessages: option.MaxNumberOfMessages || 10,
      VisibilityTimeout: option.VisibilityTimeout,
      WaitTimeSeconds: option.WaitTimeSeconds || 20,
    };
    this.sqs = new SQS(awsOption);
  }
  static async createQueue(
    name: string, 
    awsOption: SQS.Types.ClientConfiguration | undefined = undefined,
    attributes: { [key: string]: string } | undefined = undefined, 
    tags: {[key: string]: string } | undefined = undefined, 
    option: Omit<Option, 'QueueUrl'> = {},)
  {
    let sqs = new SQS(awsOption);
    await sqs.createQueue({
      QueueName: name,
      Attributes: attributes,
      tags
    }).promise();
    let url = (await sqs.getQueueUrl({
      QueueName: name,
    }).promise()).QueueUrl;
    return new Queue(Object.assign({ QueueUrl: url }, option), awsOption);
  }
  async deleteQueue() {
    await this.sqs.deleteQueue({
      QueueUrl: this.option.QueueUrl
    }).promise();
  }
  async send(messages: string[]) {
    let res = await Promise.all(chunk(messages, 10).map((messagesChunk: string[]) => 
      this.sqs.sendMessageBatch({
        QueueUrl: this.option.QueueUrl,
        Entries: messagesChunk.map((m:string) => ({
          Id: uuidv4(),
          MessageBody: m
        })),
      }).promise()))
    return res;
  }
  async receive(queueSize=10, concurrency=1): Promise<SQS.MessageList> {
    let messages: SQS.MessageList = [];
    let res = await this.sqs.receiveMessage({
      QueueUrl: this.option.QueueUrl,
      MaxNumberOfMessages: Math.min(this.option.MaxNumberOfMessages, queueSize - messages.length),
      VisibilityTimeout: this.option.VisibilityTimeout,
      WaitTimeSeconds: Math.min(this.option.WaitTimeSeconds),
    }).promise();
    if(!res.Messages || res.Messages.length == 0)
      return messages;
    for(let msg of res.Messages){
      messages.push(msg);
    }
    let shouldBreak = false
    while(!shouldBreak) {
      let reses = await Promise.all([...Array(concurrency).keys()].map(_ => this.sqs.receiveMessage({
        QueueUrl: this.option.QueueUrl,
        MaxNumberOfMessages: Math.min(this.option.MaxNumberOfMessages, queueSize - messages.length),
        VisibilityTimeout: this.option.VisibilityTimeout,
        WaitTimeSeconds: Math.min(this.option.WaitTimeSeconds),
      }).promise()));
      for(let res of reses){
        if(!res.Messages || res.Messages.length == 0){
          shouldBreak = true;
        } else {
          for(let msg of res.Messages){ 
            messages.push(msg);
          }
        }
      }
      if(messages.length >= queueSize)
        break;
    }
    return messages;
  }
  async delete(messages: SQS.MessageList) {
    let res = await Promise.all(chunk(messages, 10).map(messagesChunk =>
      this.sqs.deleteMessageBatch({
        QueueUrl: this.option.QueueUrl,
        Entries: messagesChunk.map(m => ({
          Id: m.MessageId,
          ReceiptHandle: m.ReceiptHandle ,
        })) as SQS.DeleteMessageBatchRequestEntryList,
      }).promise()));
    return res;
  }
  async heartbeat(messages: SQS.MessageList) {
    if(this.queueVisibilityTimeout == null)
      this.queueVisibilityTimeout = (await this.sqs.getQueueAttributes({
        QueueUrl: this.option.QueueUrl,
        AttributeNames: ['VisibilityTimeout'],
      }).promise()).Attributes?.VisibilityTimeout;
    let res = await Promise.all(chunk(messages, 10).map(messagesChunk =>
      this.sqs.changeMessageVisibilityBatch({
        QueueUrl: this.option.QueueUrl,
        Entries: messagesChunk.map(m => ({
          Id: m.MessageId,
          ReceiptHandle: m.ReceiptHandle,
          VisibilityTimeout: this.option.VisibilityTimeout || this.queueVisibilityTimeout,
        })) as SQS.ChangeMessageVisibilityBatchRequestEntryList,
      }).promise()));
    return res;
  }
}
