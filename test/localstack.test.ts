import Queue from '../src/index';
describe('send', () => {
  it('create and delete queue', async () => {
    let queue = await Queue.createQueue('test-queue', {region: "ap-northeast-2", apiVersion: "apiversion", endpoint: 'http://localhost:4566'});
    expect(queue.option.QueueUrl.split('/').pop()).toEqual('test-queue');
    await queue.deleteQueue();
  });
});
