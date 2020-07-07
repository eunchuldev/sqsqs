# sqsqs
SQSQS(Simple Quick SQS wrapper)

# Usage
```
const Queue = require("sqsqs")

const data1 = "data1"
const data2 = "data2"

/* Send any number of string */
/* Warning: the order of messages are not kept in the batch list! */
await Queue.send([data1, data2])

/* Receive specific number of string */
/* it might wait until fetch queueSize messages or there are no remain messages in SQS queue */
let queueSize = 50
let received = await Queue.receive(queueSize)

/* ***
received = [{
  Body: "data1",
  ReceiptHandleId: ...,
  Id: ...
},{
  Body: "data2",
  ReceiptHandleId: ...,
  Id: ...
}
]
*** */

/* Delete Messages */
await Queue.delete(received);

```
