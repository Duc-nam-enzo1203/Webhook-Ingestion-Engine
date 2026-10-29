import crypto from 'node:crypto';

/**
 * Constant-time string comparison to prevent Timing Attacks.
 */
export function timingSafeEqualString(a: string, b: string): boolean {
  const bufA = Buffer.from(a, 'utf8');
  const bufB = Buffer.from(b, 'utf8');

  if (bufA.length !== bufB.length) {
    // Return false without fast-path timing discrepancy
    return false;
  }

  return crypto.timingSafeEqual(bufA, bufB);
}

/**
 * Calculates HMAC-SHA256 of raw buffer with given secret, output format: 'base64' | 'hex'
 */
export function calculateHmac(
  data: Buffer | string,
  secret: string,
  encoding: 'base64' | 'hex'
): string {
  return crypto.createHmac('sha256', secret).update(data).digest(encoding);
}

