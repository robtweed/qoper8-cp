import {benchmark} from 'qoper8-cp/benchmark';

benchmark({
  poolSize: 1,
  maxMessages: 100,
  blockLength:10,
  delay: 14
});