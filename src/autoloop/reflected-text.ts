/**
 * Autoloop-only policy for reflecting untrusted Coder text into Planner
 * handoff prompts. Kept out of the shared engine sanitizer: the allowed form
 * (whitespace normalization, Cc/Cf rejection, unpaired-surrogate rejection,
 * hard omit-on-oversize) is a handoff contract, not a general stderr policy.
 */

import { sanitizeSecrets } from '../sanitize.js';

/** Max UTF-16 length for a Coder conclusion reflected into an incomplete handoff. */
export const AUTOLOOP_CONCLUSION_MAX = 2000;

/** Max UTF-16 length for a structured request_clarification question in the ack. */
export const AUTOLOOP_CLARIFICATION_MAX = 500;

/**
 * Byte cap for newly introduced evidence-file reads before reflection.
 * Large enough for a max-length UTF-8 conclusion, small enough to bound I/O.
 * Oversized files fall back to the associated reference without body read.
 */
export const AUTOLOOP_EVIDENCE_READ_MAX_BYTES = AUTOLOOP_CONCLUSION_MAX * 4;

/** True when `text` contains any unpaired UTF-16 high or low surrogate. */
export function hasUnpairedSurrogate(text: string): boolean {
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      if (i + 1 >= text.length) return true;
      const low = text.charCodeAt(i + 1);
      if (low < 0xdc00 || low > 0xdfff) return true;
      i += 1;
      continue;
    }
    if (c >= 0xdc00 && c <= 0xdfff) return true;
  }
  return false;
}

/**
 * Validate untrusted text before interpolating it into an Autoloop Planner
 * handoff. Permitted normalization: CRLF/CR → LF, then all Unicode whitespace
 * (including LF) collapsed to a single space and trimmed — so a short multiline
 * clarification remains usable. Secrets are redacted. Unpaired UTF-16 surrogates
 * at any position, remaining Unicode Cc/Cf, empty text, or length above
 * `maxChars` yield null so the caller omits the body and points at send-bound
 * persisted evidence instead of truncating-as-complete or silently repairing.
 * Valid astral pairs (e.g. emoji) are accepted within the length budget.
 */
export function boundAutoloopReflectedText(text: string, maxChars: number = AUTOLOOP_CONCLUSION_MAX): string | null {
  let t = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  t = t.replace(/\s+/gu, ' ').trim();
  t = sanitizeSecrets(t);
  if (!t) return null;
  if (hasUnpairedSurrogate(t)) return null;
  if (/[\p{Cc}\p{Cf}]/u.test(t)) return null;
  if (t.length > maxChars) return null;
  return t;
}
