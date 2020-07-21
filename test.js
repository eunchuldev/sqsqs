const sinon = require("sinon");
function chunk(arr, chunk_size) {
    var R = [];
    for (var i=0,len=arr.length; i<len; i+=chunk_size)
        R.push(arr.slice(i,i+chunk_size));
    return R;
}

describe('send', () => {
  const aws = require("aws-sdk");
  const Queue = require("./index.js");
  const QueueUrl = 'https://dummy-queue';
  let sqs;

  beforeEach(() => {
    sqs = { sendMessageBatch: () => {} };
    sinon.stub(sqs, "sendMessageBatch").returns({
      promise: () => (Promise.resolve({
        Failed: [],
        Successful: [],
      }))
    });
    sinon.stub(aws, 'SQS').returns(sqs);
  });

  afterEach(() => {
    aws.SQS.restore();
    sqs.sendMessageBatch.restore();
  });

  it('sends messages length less than 10', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    const messages = [
      JSON.stringify({a: "test1"}),
      JSON.stringify({b: "test2"}),
    ];
    const expectedParams = {
      Entries: messages.map(m => ({
        MessageBody: m,
        Id: sinon.match.any,
      })),
      QueueUrl,
    };
    let res = await queue.send(messages);
    sinon.assert.calledOnce(aws.SQS);
    sinon.assert.calledWith(aws.SQS, {region: "ap-northeast-2", apiVersion: "apiversion"});
    sinon.assert.calledOnce(sqs.sendMessageBatch);
    sinon.assert.calledWith(sqs.sendMessageBatch, expectedParams);
  });
  it('sends messages length larger than 10', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    const messages = [...Array(30).keys()].map(i => (
      {i: i}
    ));
    const expectedParams = chunk(messages, 10).map(messages => ({
      Entries: messages.map(m => ({
        MessageBody: m,
        Id: sinon.match.any,
      })),
      QueueUrl,
    }));
    let res = await queue.send(messages);
    sinon.assert.calledOnce(aws.SQS);
    sinon.assert.calledWith(aws.SQS, {region: "ap-northeast-2", apiVersion: "apiversion"});
    sinon.assert.calledThrice(sqs.sendMessageBatch);
    sinon.assert.calledWith(sqs.sendMessageBatch.firstCall, expectedParams[0]);
    sinon.assert.calledWith(sqs.sendMessageBatch.secondCall, expectedParams[1]);
    sinon.assert.calledWith(sqs.sendMessageBatch.thirdCall, expectedParams[2]);
  });
});

describe('receive', () => {
  const aws = require("aws-sdk");
  const Queue = require("./index.js");
  const QueueUrl = 'https://dummy-queue';
  let sqs;
  let messages = [];

  beforeEach(() => {
    sqs = { 
      receiveMessage: () => {}, 
      deleteMessageBatch: () => {}, 
      changeMessageVisibilityBatch: () => {}, 
      getQueueAttributes: () => {},
    };
    sinon.stub(sqs, "receiveMessage").returns({
      promise: () => (Promise.resolve({
        Messages: messages.splice(0, 10),
      }))
    });
    sinon.stub(sqs, "deleteMessageBatch").returns({
      promise: () => (Promise.resolve({
        Failed: [],
        Successful: [],
      }))
    });
    sinon.stub(sqs, "changeMessageVisibilityBatch").returns({
      promise: () => (Promise.resolve({
        Failed: [],
        Successful: [],
      }))
    });
    sinon.stub(sqs, "getQueueAttributes").returns({
      promise: () => (Promise.resolve({
        Attributes: {
          VisibilityTimeout: 600,
        },
      }))
    });
    sinon.stub(aws, 'SQS').returns(sqs);
  });

  afterEach(() => {
    aws.SQS.restore();
    sqs.receiveMessage.restore();
    sqs.deleteMessageBatch.restore();
    sqs.changeMessageVisibilityBatch.restore();
    sqs.getQueueAttributes.restore();
    messages = [];
  });

  it('receive messages length less than 10', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    messages = [...Array(5).keys()].map(i => (
      {Body: JSON.stringify({i: i})}
    ));
    const expectedOutput = messages.map(m => ({
        Body: m.Body,
    }));
    const expectedParams = {
      QueueUrl,
      MaxNumberOfMessages: 10,
      VisibilityTimeout: sinon.match.any,
      WaitTimeSeconds: 20
    }
    let res = await queue.receive(50);
    sinon.assert.calledOnce(aws.SQS);
    sinon.assert.calledWith(aws.SQS, {region: "ap-northeast-2", apiVersion: "apiversion"});
    sinon.assert.callCount(sqs.receiveMessage, 2);
    sinon.assert.calledWith(sqs.receiveMessage, expectedParams);
    sinon.assert.match(res, expectedOutput);
  });

  it('receive messages length larger than 10, but less than queueSize', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    messages = [...Array(30).keys()].map(i => (
      {Body: JSON.stringify({i: i})}
    ));
    const expectedOutput = messages.map(m => ({
        Body: m.Body,
    }));
    const expectedParams = {
      QueueUrl,
      MaxNumberOfMessages: 10,
      VisibilityTimeout: sinon.match.any,
      WaitTimeSeconds: 20
    }
    let res = await queue.receive(50);
    sinon.assert.calledOnce(aws.SQS);
    sinon.assert.calledWith(aws.SQS, {region: "ap-northeast-2", apiVersion: "apiversion"});
    sinon.assert.callCount(sqs.receiveMessage, 4);
    sinon.assert.calledWith(sqs.receiveMessage, expectedParams);
    sinon.assert.match(res, expectedOutput);
  });

  it('receive messages length larger than queueSize', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    messages = [...Array(100).keys()].map(i => (
      {Body: JSON.stringify({i: i})}
    ));
    const expectedOutput = messages.slice(0, 50).map(m => ({
        Body: m.Body,
    }));
    const expectedParams = {
      QueueUrl,
      MaxNumberOfMessages: 10,
      VisibilityTimeout: sinon.match.any,
      WaitTimeSeconds: 20
    }
    let res = await queue.receive(50);
    sinon.assert.calledOnce(aws.SQS);
    sinon.assert.calledWith(aws.SQS, {region: "ap-northeast-2", apiVersion: "apiversion"});
    sinon.assert.callCount(sqs.receiveMessage, 5);
    sinon.assert.calledWith(sqs.receiveMessage, expectedParams);
    sinon.assert.match(res, expectedOutput);
  });

  it('delete messages length larger than 10', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    let messages = [...Array(27).keys()].map(i => (
      {
        MessageId: i,
        ReceiptHandle: "r" + i,
      }
    ));
    const expectedParams = chunk(messages, 10).map(messages => ({
      Entries: messages.map(m => ({
        Id: m.MessageId,
        ReceiptHandle: m.ReceiptHandle,
      })),
      QueueUrl,
    }));
    let res = await queue.delete(messages);
    sinon.assert.calledOnce(aws.SQS);
    sinon.assert.calledWith(aws.SQS, {region: "ap-northeast-2", apiVersion: "apiversion"});
    sinon.assert.callCount(sqs.deleteMessageBatch, 3);

    sinon.assert.calledWith(sqs.deleteMessageBatch.firstCall, expectedParams[0]);
    sinon.assert.calledWith(sqs.deleteMessageBatch.secondCall, expectedParams[1]);
    sinon.assert.calledWith(sqs.deleteMessageBatch.thirdCall, expectedParams[2]);
  });

  it('extend message visibility timeout(heartbeat)', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion",
      VisibilityTimeout: 1000,
    });
    let messages = [...Array(27).keys()].map(i => (
      {
        MessageId: i,
        ReceiptHandle: "r" + i,
      }
    ));
    const expectedParams = chunk(messages, 10).map(messages => ({
      Entries: messages.map(m => ({
        Id: m.MessageId,
        ReceiptHandle: m.ReceiptHandle,
        VisibilityTimeout: 1000,
      })),
      QueueUrl,
    }));
    let res = await queue.heartbeat(messages);
    sinon.assert.calledOnce(aws.SQS);
    sinon.assert.calledWith(aws.SQS, {region: "ap-northeast-2", apiVersion: "apiversion"});
    sinon.assert.callCount(sqs.changeMessageVisibilityBatch, 3);
    sinon.assert.callCount(sqs.getQueueAttributes, 1);

    sinon.assert.calledWith(sqs.getQueueAttributes.firstCall, { AttributeNames: ["VisibilityTimeout"], QueueUrl: "https://dummy-queue" });
    sinon.assert.calledWith(sqs.changeMessageVisibilityBatch.firstCall, expectedParams[0]);
    sinon.assert.calledWith(sqs.changeMessageVisibilityBatch.secondCall, expectedParams[1]);
    sinon.assert.calledWith(sqs.changeMessageVisibilityBatch.thirdCall, expectedParams[2]);
  });
  it('extend default message visibility timeout(heartbeat)', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion",
    });
    let messages = [...Array(27).keys()].map(i => (
      {
        MessageId: i,
        ReceiptHandle: "r" + i,
      }
    ));
    const expectedParams = chunk(messages, 10).map(messages => ({
      Entries: messages.map(m => ({
        Id: m.MessageId,
        ReceiptHandle: m.ReceiptHandle,
        VisibilityTimeout: 600,
      })),
      QueueUrl,
    }));
    let res = await queue.heartbeat(messages);
    sinon.assert.calledOnce(aws.SQS);
    sinon.assert.calledWith(aws.SQS, {region: "ap-northeast-2", apiVersion: "apiversion"});
    sinon.assert.callCount(sqs.changeMessageVisibilityBatch, 3);
    sinon.assert.callCount(sqs.getQueueAttributes, 1);

    sinon.assert.calledWith(sqs.getQueueAttributes.firstCall, { AttributeNames: ["VisibilityTimeout"], QueueUrl: "https://dummy-queue" });
    sinon.assert.calledWith(sqs.changeMessageVisibilityBatch.firstCall, expectedParams[0]);
    sinon.assert.calledWith(sqs.changeMessageVisibilityBatch.secondCall, expectedParams[1]);
    sinon.assert.calledWith(sqs.changeMessageVisibilityBatch.thirdCall, expectedParams[2]);
  })

});
