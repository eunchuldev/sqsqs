function chunk<T>(arr:T[], chunk_size:number): T[][] {
    var R = [];
    for (var i=0,len=arr.length; i<len; i+=chunk_size)
        R.push(arr.slice(i,i+chunk_size));
    return R;
}

import { mocked } from 'ts-jest/utils';
import SQS from 'aws-sdk/clients/sqs';

let messages: any[] = [];

const mockedSendMessageBatch = jest.fn(() => ({
  promise: () => Promise.resolve({
    Failed: [],
    Successful: [],
  })
}));
const mockedDeleteMessageBatch = jest.fn(() => ({
  promise: () => Promise.resolve({
    Failed: [],
    Successful: [],
  })
}));
const mockedReceiveMessage = jest.fn(() => ({
  promise: () => (Promise.resolve({
    Messages: messages.splice(0, 10),
  }))
}));
const mockedGetQueueAttributes = jest.fn(() => ({
  promise: () => (Promise.resolve({
    Attributes: {
      VisibilityTimeout: 600,
    },
  }))
}));
const mockedChangeMessageVisibilityBatch = jest.fn(() => ({
  promise: () => (Promise.resolve({
    Attributes: {
      VisibilityTimeout: 600,
    },
  }))
}));
jest.mock('aws-sdk/clients/sqs', () => {
  return jest.fn().mockImplementation(() => ({
    sendMessageBatch: mockedSendMessageBatch,
    receiveMessage: mockedReceiveMessage,
    deleteMessageBatch: mockedDeleteMessageBatch,
    getQueueAttributes: mockedGetQueueAttributes,
    changeMessageVisibilityBatch: mockedChangeMessageVisibilityBatch,
  }));
});
import Queue from '../src/index';

describe('send', () => {
  const MockedSQS = mocked(SQS, true);
  const aws = require("aws-sdk");
  const QueueUrl = 'https://dummy-queue';

  beforeEach(() => {
    MockedSQS.mockClear();
    mockedSendMessageBatch.mockClear();
  });

  afterEach(() => {
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
    const expectedParams = [{
      Entries: messages.map(m => ({
        MessageBody: m,
        Id: expect.any(String),
      })),
      QueueUrl,
    }];
    let res = await queue.send(messages);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});
    expect(mockedSendMessageBatch.mock.calls.length).toBe(1);
    expect(mockedSendMessageBatch.mock.calls[0]).toMatchObject(expectedParams);
  });
  it('sends messages length larger than 10', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    const messages = [...Array(30).keys()].map(i => JSON.stringify({i: i}));
    const expectedParams = chunk(messages, 10).map(messages => ([{
      Entries: messages.map(m => ({
        MessageBody: m,
        Id: expect.any(String),
      })),
      QueueUrl,
    }]));
    let res = await queue.send(messages);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});
    expect(mockedSendMessageBatch.mock.calls.length).toBe(3);
    expect(mockedSendMessageBatch.mock.calls).toMatchObject(expectedParams);
  });
});


describe('receive', () => {
  const MockedSQS = mocked(SQS, true);
  const aws = require("aws-sdk");
  const QueueUrl = 'https://dummy-queue';

  beforeEach(() => {
    messages = [];
    MockedSQS.mockClear();
    mockedReceiveMessage.mockClear();
    mockedDeleteMessageBatch.mockClear();
    mockedChangeMessageVisibilityBatch.mockClear();
    mockedGetQueueAttributes.mockClear();
  });

  afterEach(() => {
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
    const expectedParams = [{
      QueueUrl,
      MaxNumberOfMessages: 10,
      VisibilityTimeout: undefined as any,
      WaitTimeSeconds: 20
    }]
    let res = await queue.receive(50);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});
    expect(mockedReceiveMessage.mock.calls.length).toBe(2);
    expect(mockedReceiveMessage.mock.calls[0]).toMatchObject(expectedParams);
    expect(res).toMatchObject(expectedOutput);
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
    const expectedParams = [{
      QueueUrl,
      MaxNumberOfMessages: 10,
      VisibilityTimeout: undefined as number,
      WaitTimeSeconds: 20
    }]
    let res = await queue.receive(50);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});
    expect(mockedReceiveMessage.mock.calls.length).toBe(4);
    expect(mockedReceiveMessage.mock.calls[0]).toMatchObject(expectedParams);
    expect(res).toMatchObject(expectedOutput);
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
    const expectedParams = [{
      QueueUrl,
      MaxNumberOfMessages: 10,
      VisibilityTimeout: undefined as number,
      WaitTimeSeconds: 20
    }];
    let res = await queue.receive(50);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});
    expect(mockedReceiveMessage.mock.calls.length).toBe(5);
    expect(mockedReceiveMessage.mock.calls[0]).toMatchObject(expectedParams);
    expect(res).toMatchObject(expectedOutput);
  });

  it('receive messages length larger than queueSize parralel', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    messages = [...Array(100).keys()].map(i => (
      {Body: JSON.stringify({i: i})}
    ));
    const expectedOutput = messages.slice(0, 60).map(m => ({
        Body: m.Body,
    }));
    const expectedParams = [{
      QueueUrl,
      MaxNumberOfMessages: 10,
      VisibilityTimeout: undefined as number,
      WaitTimeSeconds: 20
    }];
    let res = await queue.receive(50, 5);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});
    expect(mockedReceiveMessage.mock.calls.length).toBe(6);
    expect(mockedReceiveMessage.mock.calls[0]).toMatchObject(expectedParams);
    expect(res).toMatchObject(expectedOutput);
  });

  it('delete messages length larger than 10', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion"
    });
    let messages = [...Array(27).keys()].map(i => (
      {
        MessageId: "" + i,
        ReceiptHandle: "r" + i,
      } as SQS.Message
    ));
    const expectedParams = chunk(messages, 10).map(messages => ([{
      Entries: messages.map(m => ({
        Id: m.MessageId,
        ReceiptHandle: m.ReceiptHandle,
      })),
      QueueUrl,
    }]));
    let res = await queue.delete(messages);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});

    expect(mockedDeleteMessageBatch.mock.calls.length).toBe(3);
    expect(mockedDeleteMessageBatch.mock.calls).toMatchObject(expectedParams);
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
        MessageId: "" + i,
        ReceiptHandle: "r" + i,
      } as SQS.Message
    ));
    const expectedParams = chunk(messages, 10).map(messages => ([{
      Entries: messages.map(m => ({
        Id: m.MessageId,
        ReceiptHandle: m.ReceiptHandle,
        VisibilityTimeout: 1000,
      })),
      QueueUrl,
    }]));
    let res = await queue.heartbeat(messages);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});

    expect(mockedChangeMessageVisibilityBatch.mock.calls.length).toBe(3);
    expect(mockedGetQueueAttributes.mock.calls.length).toBe(1);

    expect(mockedGetQueueAttributes.mock.calls[0]).toMatchObject([{ AttributeNames: ["VisibilityTimeout"], QueueUrl: "https://dummy-queue" }]);
    expect(mockedChangeMessageVisibilityBatch.mock.calls).toMatchObject(expectedParams);
  });
  it('extend default message visibility timeout(heartbeat)', async () => {
    let queue = new Queue({
      QueueUrl,
      region: "ap-northeast-2",
      apiVersion: "apiversion",
    });
    let messages = [...Array(27).keys()].map(i => (
      {
        MessageId: "" + i,
        ReceiptHandle: "r" + i,
      } as SQS.Message
    ));
    const expectedParams = chunk(messages, 10).map(messages => ([{
      Entries: messages.map(m => ({
        Id: m.MessageId,
        ReceiptHandle: m.ReceiptHandle,
        VisibilityTimeout: 600,
      })),
      QueueUrl,
    }]));
    let res = await queue.heartbeat(messages);
    expect(MockedSQS).toHaveBeenCalledTimes(1);
    expect(MockedSQS).toHaveBeenCalledWith({region: "ap-northeast-2", apiVersion: "apiversion"});

    expect(mockedChangeMessageVisibilityBatch.mock.calls.length).toBe(3);
    expect(mockedGetQueueAttributes.mock.calls.length).toBe(1);

    expect(mockedGetQueueAttributes.mock.calls[0]).toMatchObject([{ AttributeNames: ["VisibilityTimeout"], QueueUrl: "https://dummy-queue" }]);
    expect(mockedChangeMessageVisibilityBatch.mock.calls).toMatchObject(expectedParams);
  })

});
