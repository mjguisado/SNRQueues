import * as redis from 'redis';
import { UUID, randomUUID } from 'crypto';
import { argv } from 'process';

const redisDB = 'redis://redis-14998.manuguisado.primary.cs.redislabs.com:14998';
const activeQueuesKey =  "CICSPAC_CasTSQ_NR:queues";

function generateRandomString(length: number) {
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const charactersLength = characters.length;
  let result = '';
  for (let i = 0; i < length; i++) {
      const randomIndex = Math.floor(Math.random() * charactersLength);
      result += characters[randomIndex];
  }
  return result;
}

const writeQueue = async (queueName: string): Promise<void> => {

  const client = await redis.createClient({
      url: redisDB
    })
    .on('error', err => console.log('Redis Client Error', err))
    .connect();

  const hashKey = "CICSPAC_CasTSQ_NR:data:" + queueName;
  const lockKey = "CICSPAC_CasTSQ_NR:lock:" + queueName;

  let control = await client.hGet(hashKey, "CTL");

  // Acquire the lock
  const lock = await client.set(lockKey, randomUUID(), {NX: true, EX: 120});
  if ('OK' === lock ) {
    await client.hSet(hashKey, "CTL",   generateRandomString(100));
    await client.hSet(hashKey, "00001", generateRandomString(100));
    await client.expire(hashKey, 300);
    await client.sAdd(activeQueuesKey, queueName);
    await client.del(lockKey);
  } else {
    console.log("Queue: " + queueName + ' - Lock has NOT been acquired.');
  }

  await client.disconnect();

}

const readQueue = async (queueName: string): Promise<void> => {

  const client = await redis.createClient({
      url: redisDB
    })
    .on('error', err => console.log('Redis Client Error', err))
    .connect();

  const hashKey = "CICSPAC_CasTSQ_NR:data:" + queueName;
  const lockKey = "CICSPAC_CasTSQ_NR:lock:" + queueName;

  // Acquire the lock
  const lock = await client.set(lockKey, randomUUID(), {NX: true, EX: 120});
  if ('OK' === lock ) {
    let control = await client.hGet(hashKey, "CTL");
    let data    = await client.hGet(hashKey, "00001");
    await client.hSet(hashKey, "CTL", generateRandomString(100));
    await client.del(lockKey);
  } else {
    console.log("Queue: " + queueName + ' - Lock has NOT been acquired.');
  }
  await client.disconnect();
}

const deleteQueue = async (queueName: string): Promise<void> => {

  const client = await redis.createClient({
      url: redisDB
    })
    .on('error', err => console.log('Redis Client Error', err))
    .connect();
  
  const hashKey = "CICSPAC_CasTSQ_NR:data:" + queueName;
  
  // await client.del(hashKey);
  await client.unlink(hashKey);
  await client.sRem(activeQueuesKey, queueName);
  
  await client.disconnect();
}

const testQueue = async (queueName: string): Promise<void> => {
  await writeQueue(queueName);
  await readQueue(queueName);
  // await deleteQueue(queueName);
}

const testBatch = async (duration: number = 300, batchSize: number): Promise<void> => {
  const end: number = Date.now() + (duration * 1000);
  let batch: Promise<void>[] = [];
  while ( end > Date.now()) {
    for (let counter: number = 0; counter < batchSize; counter ++) {
      batch.push(testQueue(generateRandomString(16)));
    }
    await Promise.all(batch);
  }
}

const deleteQueues = async (batchSize: number): Promise<void> => {
  const client = await redis.createClient({
    url: redisDB
  })
  .on('error', err => console.log('Redis Client Error', err))
  .connect();
  for await (const member of client.sScanIterator(activeQueuesKey, {COUNT: batchSize})) {
    const hashKey = "CICSPAC_CasTSQ_NR:data:" + member;
    await client.del(hashKey)
    await client.sRem(activeQueuesKey, member)
  }

  await client.disconnect();
}

if (process.argv.length !== 4) {
  console.log('Usage: ' + argv[0] + ' ' + argv[1] + ' duration concurrency');
}

testBatch(Number(argv[2]), Number(argv[3]))
  .catch((error) => { console.error(error); });

