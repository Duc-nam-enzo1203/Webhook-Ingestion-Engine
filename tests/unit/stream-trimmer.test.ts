import { describe, it, expect, vi } from 'vitest';
import { SafeStreamTrimmer } from '../../src/worker/trimmer/stream-trimmer.js';

describe('SafeStreamTrimmer Unit Tests', () => {
  it('trims shard using oldest unacknowledged message ID from PEL (XPENDING)', async () => {
    const trimmer = new SafeStreamTrimmer();
    const mockRedis = {
      xpending: vi.fn().mockResolvedValue([5, '1761720000000-0', '1761720005000-0', [['worker1', 5]]]),
      xtrim: vi.fn().mockResolvedValue(12),
      xinfo: vi.fn(),
    } as any;

    const trimmed = await trimmer.trimShard(mockRedis, 'stream:webhooks:00');

    expect(mockRedis.xpending).toHaveBeenCalledWith('stream:webhooks:00', 'webhook_workers');
    expect(mockRedis.xtrim).toHaveBeenCalledWith('stream:webhooks:00', 'MINID', '~', '1761720000000-0');
    expect(trimmed).toBe(12);
  });

  it('trims shard using last-delivered-id when PEL has 0 pending messages', async () => {
    const trimmer = new SafeStreamTrimmer();
    const mockRedis = {
      xpending: vi.fn().mockResolvedValue([0, null, null, []]),
      xinfo: vi.fn().mockResolvedValue([
        [
          'name', 'webhook_workers',
          'consumers', 2,
          'pending', 0,
          'last-delivered-id', '1761720099999-0',
        ],
      ]),
      xtrim: vi.fn().mockResolvedValue(45),
    } as any;

    const trimmed = await trimmer.trimShard(mockRedis, 'stream:webhooks:01');

    expect(mockRedis.xpending).toHaveBeenCalledWith('stream:webhooks:01', 'webhook_workers');
    expect(mockRedis.xinfo).toHaveBeenCalledWith('GROUPS', 'stream:webhooks:01');
    expect(mockRedis.xtrim).toHaveBeenCalledWith('stream:webhooks:01', 'MINID', '~', '1761720099999-0');
    expect(trimmed).toBe(45);
  });

  it('skips trimming if last-delivered-id is 0-0 or stream is empty', async () => {
    const trimmer = new SafeStreamTrimmer();
    const mockRedis = {
      xpending: vi.fn().mockResolvedValue([0, null, null, []]),
      xinfo: vi.fn().mockResolvedValue([
        [
          'name', 'webhook_workers',
          'last-delivered-id', '0-0',
        ],
      ]),
      xtrim: vi.fn(),
    } as any;

    const trimmed = await trimmer.trimShard(mockRedis, 'stream:webhooks:02');

    expect(mockRedis.xtrim).not.toHaveBeenCalled();
    expect(trimmed).toBe(0);
  });
});
