import crypto from 'node:crypto';
import { Redis } from 'ioredis';
import { env } from '../../config/env.js';

export interface ShardSelection {
  selectedShard: string;
  shardIndex: number;
  candidateA: number;
  candidateB: number;
  lengthA: number;
  lengthB: number;
}

export class ShuffleShardingRouter {
  private readonly shardCount: number;
  private readonly streamPrefix: string;
  private xlenCache: Map<number, { length: number; timestamp: number }> = new Map();
  private readonly cacheTtlMs = 100;

  constructor(shardCount = env.STREAM_SHARD_COUNT, streamPrefix = env.STREAM_PREFIX) {
    this.shardCount = shardCount;
    this.streamPrefix = streamPrefix;
  }

  /**
   * Deterministically maps a key and seed to a shard index [0, shardCount - 1]
   */
  getHashIndex(key: string, seed: number): number {
    const hash = crypto.createHash('sha256').update(`${seed}:${key}`).digest();
    const uintVal = hash.readUInt32BE(0);
    return uintVal % this.shardCount;
  }

  /**
   * Returns two distinct candidate shards for a given tenantId
   */
  getCandidateShards(tenantId: string): [number, number] {
    const candidateA = this.getHashIndex(tenantId, 1337);
    let candidateB = this.getHashIndex(tenantId, 7331);

    if (candidateA === candidateB) {
      candidateB = (candidateA + 1) % this.shardCount;
    }

    return [candidateA, candidateB];
  }

  getStreamKey(shardIndex: number): string {
    const padded = String(shardIndex).padStart(2, '0');
    return `${this.streamPrefix}:${padded}`;
  }

  /**
   * Gets queue length with short-term (100ms) cache to avoid flooding Redis XLEN
   */
  async getQueueLength(redis: Redis, shardIndex: number): Promise<number> {
    const cached = this.xlenCache.get(shardIndex);
    const now = Date.now();
    if (cached && (now - cached.timestamp) < this.cacheTtlMs) {
      return cached.length;
    }

    const key = this.getStreamKey(shardIndex);
    try {
      const len = await redis.xlen(key);
      this.xlenCache.set(shardIndex, { length: len, timestamp: now });
      return len;
    } catch {
      return 0;
    }
  }

  /**
   * Power of Two Choices: Chooses the shorter queue between candidateA and candidateB
   */
  async selectShard(tenantId: string, redis: Redis): Promise<ShardSelection> {
    const [candidateA, candidateB] = this.getCandidateShards(tenantId);

    const [lengthA, lengthB] = await Promise.all([
      this.getQueueLength(redis, candidateA),
      this.getQueueLength(redis, candidateB),
    ]);

    const chosenIndex = lengthA <= lengthB ? candidateA : candidateB;

    return {
      selectedShard: this.getStreamKey(chosenIndex),
      shardIndex: chosenIndex,
      candidateA,
      candidateB,
      lengthA,
      lengthB,
    };
  }
}

export const shuffleShardingRouter = new ShuffleShardingRouter();

