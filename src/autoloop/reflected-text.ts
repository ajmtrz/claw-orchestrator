/**
 * Autoloop-only policy for reflecting untrusted Coder text into Planner
 * handoff prompts. Kept out of the shared engine sanitizer: the allowed form
 * (whitespace normalization, Cc/Cf rejection, hard omit-on-oversize) is a
 * handoff contract, not a general stderr policy.
 */

import { sanitizeSecrets } from '../sanitize.js';

/** Max UTF-16 length for a Coder conclusion reflected into an incomplete handoff. */
export const AUTOLOOP_CONCLUSION_MAX = 2000;

/** Max UTF-16 length for a structured request_clarification question in the ack. */
export const AUTOLOOP_CLARIFICATION_MAX = 500;

/**
 * Validate untrusted text before interpolating it into an Autoloop Planner
 * handoff. Permitted normalization: CRLF/CR → LF, then all Unicode whitespace
 * (including LF) collapsed to a single space and trimmed — so a short multiline
 * clarification remains usable. Secrets are redacted. Remaining Unicode Cc/Cf
 * (after normalization), empty text, a trailing lone high surrogate, or length
 * above `maxChars` yield null so the caller omits the body and points at
 * current-turn persisted evidence instead of truncating-as-complete.
 */
export function boundAutoloopReflectedText(text: string, maxChars: number = AUTOLOOP_CONCLUSION_MAX): string | null {
  let t = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  t = t.replace(/\s+/gu, ' ').trim();
  t = sanitizeSecrets(t);
  if (/[\uD800-\uDBFF]$/.test(t)) t = t.slice(0, -1);
  if (!t) return null;
  if (/[\p{Cc}\p{Cf}]/u.test(t)) return null;
  if (t.length > maxChars) return null;
  return t;
}
