/*
 ----------------------------------------------------------------------------
 | QOper8-cp: Queue-based Node.js Child Process Pool Manager                 |
 |                                                                           |
 | Copyright (c) 2023 MGateway Ltd,                                          |
 | Redhill, Surrey UK.                                                       |
 | All rights reserved.                                                      |
 |                                                                           |
 | https://www.mgateway.com                                                  |
 | Email: rtweed@mgateway.com                                                |
 |                                                                           |
 |                                                                           |
 | Licensed under the Apache License, Version 2.0 (the "License");           |
 | you may not use this file except in compliance with the License.          |
 | You may obtain a copy of the License at                                   |
 |                                                                           |
 |     http://www.apache.org/licenses/LICENSE-2.0                            |
 |                                                                           |
 | Unless required by applicable law or agreed to in writing, software       |
 | distributed under the License is distributed on an "AS IS" BASIS,         |
 | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  |
 | See the License for the specific language governing permissions and       |
 |  limitations under the License.                                           |
 ----------------------------------------------------------------------------

29 September 2023

*/

import path from 'path';

process.stdin.resume();

process.on( 'SIGINT', function() {
  if (process && process.pid) {
    console.log('Child Process ' + process.pid + ' detected SIGINT (Ctrl-C) - ignored');
  }
});

let QWorker = class {
  constructor() {
    let logging = false;
    let listeners = new Map();
    let handlers = new Map();
    let startedAt = Date.now();
    let id = false;
    let initialised = false;
    let isActive = false;
    let toBeTerminated = false;
    let uuid = false;
    let delay = 60000;
    let inactivityLimit = 180000;
    let handlersByMessageType = new Map();
    let timer = false;
    let lastActivityAt = Date.now();
    let noOfMessages = 0;
    let q = this;
    let cwd = process.cwd();
    this.cwd = cwd;

    let shutdown = function() {
      // signal to master process that I'm to be shut down
      q.log('Worker ' + id + ' sending request to shut down');
      q.emit('stop');
      let obj = {
        qoper8: {
          shutdown: true
        }
      };
      if (timer) clearInterval(timer);
      process.send(obj);
      q.emit('shutdown_signal_sent');
      //setTimeout(function() {
      process.exit(0);
      //}, 1000);
    }

    let finished = function(res) {
      res = res || {};
      if (!res.qoper8) res.qoper8 = {};
      res.qoper8.finished = true;
      process.send(res);
      q.emit('finished', res);
      isActive = false;
      if (toBeTerminated) {
        shutdown();
      }
    }

    this.send = function(msg) {
      process.send(msg);
    };

    this.postMessage = function(msg) {
      process.send(msg);
    };


    let startTimer = function() {
      timer = setInterval(function() {
        let inactiveFor = Date.now() - lastActivityAt;
        q.log('Worker ' + id + ' inactive for ' + inactiveFor);
        q.log('Inactivity limit: ' + inactivityLimit);
        if (inactiveFor > inactivityLimit) {
          if (isActive) {
            // flag to be terminated when activity finished
            q.log('Worker ' + id + ' flagged for termination');
            toBeTerminated = true;
          }
          else {
            shutdown();
          }
        }
      }, delay);
    }

    this.upTime = function() {
      let sec = (new Date().getTime() - startedAt)/1000;
      let hrs = Math.floor(sec / 3600);
      sec %= 3600;
      let mins = Math.floor(sec / 60);
      if (mins < 10) mins = '0' + mins;
      sec = Math.floor(sec % 60);
      if (sec < 10) sec = '0' + sec;
      let days = Math.floor(hrs / 24);
      hrs %= 24;
      return days + ' days ' + hrs + ':' + mins + ':' + sec;
    }

    this.getMessageCount = function() {
      return noOfMessages;
    }

    this.on = function(type, callback) {
      if (!listeners.has(type)) {
        listeners.set(type, callback);
      }
    };

    this.off = function(type) {
      if (listeners.has(type)) {
        listeners.delete(type);
      }
    };

    this.emit = function(type, data) {
      if (listeners.has(type)) {
        let handler =  listeners.get(type);
        handler.call(q, data);
      }
    };

    this.log = function(message) {
      if (logging) {
        console.log(Date.now() + ': ' + message);
      }
    };

    this.onMessage = async function(obj) {

      lastActivityAt = Date.now();
      isActive = true;

      let error;

      if (obj.qoper8 && obj.qoper8.init && typeof obj.qoper8.id !== 'undefined') {
        if (initialised) {
          error = 'QOper8 Worker ' + id + ' has already been initialised';
          q.emit('error', error);
          return finished({
            error: error,
            originalMessage: obj
          });
        }

        if (obj.qoper8.onStartupModule) {
          let mod;
          try {
            let {onStartupModule} = await import(path.resolve(cwd, obj.qoper8.onStartupModule));
            mod = onStartupModule;
            q.log('onStartup Customisation module loaded: ' + path.resolve(cwd, obj.qoper8.onStartupModule));
          }
          catch(err) {
            error = 'Unable to load onStartup customisation module ' + obj.qoper8.onStartupModule;
            q.log(error);
            q.log(JSON.stringify(err, Object.getOwnPropertyNames(err)));
            q.emit('error', {
              error: error,
              caughtError: JSON.stringify(err, Object.getOwnPropertyNames(err))
            });
            return finished({
              error: error,
              caughtError: JSON.stringify(err, Object.getOwnPropertyNames(err)),
              originalMessage: obj,
              workerId: id
            });
          }

          // onStartup customisation module loaded: now invoke it

          try {
            if (mod.constructor.name === 'AsyncFunction') {
              await mod.call(q, obj.qoper8.onStartupArguments);
            }
            else {
              mod.call(q, obj.qoper8.onStartupArguments);
            }
          }
          catch(err) {
            error = 'Error running onStartup customisation module ' + obj.qoper8.onStartupModule;
            q.log(error);
            q.log(JSON.stringify(err, Object.getOwnPropertyNames(err)));
            q.emit('error', {
              error: error,
              caughtError: JSON.stringify(err, Object.getOwnPropertyNames(err))
            });
            return finished({
              error: error,
              caughtError: JSON.stringify(err, Object.getOwnPropertyNames(err)),
              originalMessage: obj,
              workerId: id
            });
          }
        }

        id = obj.qoper8.id;
        uuid = obj.qoper8.uuid;
        if (obj.qoper8.workerInactivityCheckInterval) delay = obj.qoper8.workerInactivityCheckInterval; 
        if (obj.qoper8.workerInactivityLimit) inactivityLimit = obj.qoper8.workerInactivityLimit; 
        if (obj.qoper8.handlersByMessageType) {
          handlersByMessageType = obj.qoper8.handlersByMessageType;
        }

        logging = obj.qoper8.logging;
        startTimer();
        q.log('new worker ' + id + ' started...');
        q.emit('started', {id: id});
        initialised = true;
        return finished({
          pid: process.pid,
          qoper8: {
            init: true
          }
        });
      }

      // all subsequent messages

      if (!initialised) {
        error = 'QOper8 Worker ' + id + ' has not been initialised';
        q.emit('error', error);
        return finished({
          error: error,
          originalMessage: obj
        });
      }

      if (!obj.qoper8 || !obj.qoper8.uuid) {
        error = 'Invalid message sent to QOper8 Worker ' + id;
        q.emit('error', error);
        return finished({
          error: error,
          originalMessage: obj
        });
      }

      if (obj.qoper8.uuid !== uuid) {
        error = 'Invalid UUID on message sent to QOper8 Worker ' + id;
        q.emit('error', error);
        return finished({
          error: error,
          originalMessage: obj
        });
      }

      let dispObj = {...obj};
      //let dispObj = JSON.parse(JSON.stringify(obj));
      delete obj.qoper8.uuid;
      delete dispObj.qoper8;
      q.log('Message received by worker ' + id + ': ' + JSON.stringify(dispObj, null, 2));
      q.emit('received', {message: dispObj});

      if (obj.type === 'qoper8_terminate') {
        shutdown();
        return;
      }

      if (obj.type === 'qoper8_getStats') {
        return finished(q.getStats());
      }

      if (!obj.type && !obj.handlerUrl) {
        error = 'No type or handler specified in message sent to worker ' + id;
        q.emit('error', error);
        return finished({
          error: error,
          originalMessage: dispObj
        });
      }

      if (obj.type && handlersByMessageType.has(obj.type)) {
        if (!handlers.has(obj.type)) {
          let handlerObj = handlersByMessageType.get(obj.type);
          if (handlerObj.text) {
            let handlerFn = new Function('message', 'finished', handlerObj.text);
            handlers.set(obj.type, handlerFn);
          }
          else if (handlerObj.module) {
            try {
              let modulePath = handlerObj.path || cwd;
              let {handler} = await import(path.resolve(modulePath, handlerObj.module));
              handlers.set(obj.type, handler);
              q.log('Handler module imported into Worker ' + id + ': ' + path.resolve(modulePath, handlerObj.module));
            }
            catch(err) {
              error = 'Unable to load Handler module ' + handlerObj.module;
              q.log(error);
              q.log(JSON.stringify(err, Object.getOwnPropertyNames(err)));
              q.emit('error', {
                error: error,
                caughtError: JSON.stringify(err, Object.getOwnPropertyNames(err))
              });
              return finished({
                error: error,
                 caughtError: JSON.stringify(err, Object.getOwnPropertyNames(err)),
                originalMessage: dispObj,
                workerId: id
              });
            }
          }
          q.emit('handler' + obj.type + 'Loaded');

        }
        noOfMessages++;
        let handler = handlers.get(obj.type);
        try {
          let ctx = {...q};
          if (q.mgdbx) ctx.mgdbx = q.mgdbx;  // to provide mgdbx container cacheing
          ctx.id = id;
          if (handler.constructor.name === 'AsyncFunction') {
            await handler.call(ctx, obj, finished);
          }
          else {
            handler.call(ctx, obj, finished);
          }
        }
        catch(err) {
          error = 'Error running Handler Method for type ' + obj.type;
          q.log(error);
          q.log(JSON.stringify(err, Object.getOwnPropertyNames(err)));
          q.emit('error', {
            error: error,
            caughtError: JSON.stringify(err, Object.getOwnPropertyNames(err))
          });

          // return the error and also signal that child process should be terminated
          //  to avoid any lasting side effects within the process due to the defective handler

          if (timer) clearInterval(timer);
          return finished({
            error: error,
            caughtError: JSON.stringify(err, Object.getOwnPropertyNames(err)),
            shutdown: true,
            originalMessage: dispObj,
            workerId: id
          });
        }
      }
      else {
        error = 'No handler defined for messages of type ' + obj.type;
        q.log(error);
        q.emit('error', error);
        return finished({
          error: error,
          originalMessage: dispObj
        });
      }
    };
  }

  getStats() {
    let mem = process.memoryUsage();
    return {
      pid: process.pid,
      uptime: this.upTime(),
      noOfMessages: this.getMessageCount(),
      memory: {
        rss: (mem.rss /1024 /1024).toFixed(2), 
        heapTotal: (mem.heapTotal /1024 /1024).toFixed(2), 
        heapUsed: (mem.heapUsed /1024 /1024).toFixed(2)
      }
    };
  }
};

let QOper8Worker = new QWorker();

process.on('message', async function(messageObj) {
  await QOper8Worker.onMessage(messageObj);
});


