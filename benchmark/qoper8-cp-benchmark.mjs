import {QOper8} from 'qoper8-cp';
import path from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let benchmark = function(options) {
  let poolSize = +options.poolSize || 1;
  let maxMessages = +options.maxMessages || 5000;
  let blockLength = +options.blockLength || 100;
  let delay = +options.delay || 200;
  let maxQLength = +options.maxQLength || 20000;
  let logging = options.logging || false;

  let message = options.message;

  let qOptions = {
    logging: logging,
    maxQLength: maxQLength,
    workerInactivityLimit: 2,
    handlersByMessageType: new Map([
      ['benchmark', {
        module: 'benchmarkWorker.mjs',
        path: __dirname
      }]
    ]),
    poolSize: poolSize,
    exitOnStop: true
  };

  if (options.handlers) {
    for (let type in options.handlers) {
      qOptions.handlersByMessageType.set(type, options.handlers[type]);
    }
  }

  if (options.mgdbx) qOptions.mgdbx = options.mgdbx;

  let q = new QOper8(qOptions)

  let msgNo = 0;
  let batchNo = 0;
  let maxQueueLength = 0;
  let responseNo = 0;
  let startTime = Date.now();
  let messageCountByWorker = {};
  for (let id = 0; id < poolSize; id++) {
    messageCountByWorker[id] = 0;
  }

  function handleResponse(res, responseNo, workerId) {
    if (responseNo) {
      messageCountByWorker[workerId]++;
      if (responseNo === maxMessages) {
        let elapsed = (Date.now() - startTime) / 1000;
        let rate = maxMessages / elapsed;
        console.log('===========================');
        console.log(' ');
        console.log(responseNo.toLocaleString() + ' messages: ' + elapsed.toLocaleString() + ' sec');
        console.log('Processing rate: ' + rate.toLocaleString() + ' message/sec');
        for (let id = 0; id < poolSize; id++) {
          console.log('Child Process ' + id + ': ' + messageCountByWorker[id].toLocaleString() + ' messages handled');
        }
        console.log(' ');
        console.log('===========================');
        q.stop();
      }
    }
  };

  function addBlockOfMessages(blockLength, delay) {
    // add a block of messages to the queue
    batchNo++;
    setTimeout(function() {
      // Check what's already in the queue
      let queueLength = q.getQueueLength();
      if (queueLength > maxQueueLength) {
        console.log('Block no: ' + batchNo + ' (' + msgNo.toLocaleString() + '): queue length increased to ' + queueLength.toLocaleString());
        maxQueueLength = queueLength;
        delay++;
        console.log('delay increased to ' + delay);
      }
      if (queueLength === 0) {
        console.log('Block no: ' + batchNo + ' (' + msgNo.toLocaleString() + '): Queue exhausted');
        delay--;
        console.log('delay reduced to ' + delay);
      }
      // Now add another block
      for (let i = 0; i < blockLength; i++) {
        msgNo++;
        if (msgNo > maxMessages) break;
        let msg;
        if (message) {
          msg = {...message};
        }
        else {
          msg = {
            type: 'benchmark',
            messageNo: msgNo,
            time: Date.now()
          };
        }
        q.message(msg, function(res, workerId) {
          responseNo++;
          handleResponse(res, responseNo, workerId);
        });
      }
      // add another block of message to the queue
      if (msgNo < maxMessages) {
        addBlockOfMessages(blockLength, delay);
      }
      else {
        console.log('Completed sending messages');
      }  
    }, delay);
  };

  addBlockOfMessages(blockLength, delay);

};

export {benchmark};
