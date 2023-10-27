/*
 ----------------------------------------------------------------------------
 | QOper8-cp: Queue-based Node.js Child Process Pool Manager                 |
 |            mg-dbx-napi child process startup loader module                |
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

26 October 2023

*/

import {server, mglobal, mclass} from 'mg-dbx-napi';
import {glsDB} from 'glsdb';

const onStartupModule = function(args) {
  let type = args.type;
  this.cache = new Map();
  let q = this;
  let db = new server();
  let opened = false;
  if (args && args.open) {
    db.open(args.open);
    this.mgdbx = {
      db: db,
      mglobal: mglobal,
      mclass: mclass
    };

    this.use = function() {
      let args = [...arguments];
      let key = args.toString();
      if (!q.cache.has(key)) {
        q.cache.set(key, {
          container: new mglobal(db, ...args),
          at: Date.now()
        });
      }
      return q.cache.get(key).container;
    };

    this.glsdb = new glsDB({
      type: type,
      db: db,
      mglobal: mglobal,
      mclass: mclass,
      use: this.use
    });

    opened = true;
  }
  this.on('stop', function() {
    q.log('Worker is about to be shut down by QOper8');
    if (opened) db.close();
  });
};
export {onStartupModule};