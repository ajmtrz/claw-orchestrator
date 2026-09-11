import { createHash, randomUUID } from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { types as nodeUtilTypes } from 'node:util';
import { isFileLockReleaseError, withFileLock } from '../kernel/file-lock.js';
import { MAX_PLANNER_CONTROL_BATCH_BYTES } from './planner-tools.js';
import {
  isCommittedSecureLedgerError,
  type SecureAutoloopLedger,
  type SecureAutoloopPreparedAppend,
} from './secure-ledger.js';
import {
  AutoloopDeliveryOutboxError,
  type DeliveryAcknowledgement,
  type DeliveryIntent,
  type PrepareDeliveryInput,
} from './types.js';

export { AutoloopDeliveryOutboxError } from './types.js';

const MAX_DELIVERY_IDEMPOTENCY_KEY_BYTES = 8_192;
const MAX_DECISION_LEDGER_BYTES = 64 * 1024 * 1024;
const MAX_DECISION_LEDGER_ROW_BYTES = MAX_PLANNER_CONTROL_BATCH_BYTES + 256 * 1024;
/** Root is depth 0; a value reached through exactly 64 property/index edges is valid. */
const MAX_DELIVERY_PAYLOAD_DEPTH = 64;
const DECISION_LEDGER_TAIL_VERIFICATION_BYTES = 4 * 1024;
const DELIVERY_OUTBOX_LOCK_WAIT_MS = 5_000;
const PREPARE_DELIVERY_INPUT_FIELDS = [
  'idempotency_key',
  'kind',
  'target_role',
  'target_generation',
  'payload',
] as const satisfies readonly (keyof PrepareDeliveryInput)[];

interface DecisionLedgerIndex {
  readonly dev: number;
  readonly ino: number;
  readonly fileBytes: number;
  readonly mtimeMs: number;
  readonly ctimeMs: number;
  readonly rowCount: number;
  readonly tail: Buffer;
  readonly deliveryRows: DeliveryLedgerRow[];
  readonly intentsByIdempotencyKey: Map<string, DeliveryIntent[]>;
  readonly intentsByDeliveryId: Map<string, DeliveryIntent[]>;
  readonly acknowledgementsByDeliveryId: Map<string, DeliveryAcknowledgement[]>;
}

type DeliveryLedgerRow =
  | { readonly kind: 'intent'; readonly rowNumber: number; readonly intent: DeliveryIntent }
  | { readonly kind: 'acknowledgement'; readonly rowNumber: number; readonly acknowledgement: DeliveryAcknowledgement };

type DecisionLedgerRead =
  | { readonly kind: 'missing' }
  | { readonly kind: 'unchanged'; readonly index: DecisionLedgerIndex }
  | {
      readonly kind: 'contents';
      readonly contents: string;
      readonly dev: number;
      readonly ino: number;
      readonly fileBytes: number;
      readonly mtimeMs: number;
      readonly ctimeMs: number;
      readonly rowOffset: number;
      readonly previous?: DecisionLedgerIndex;
      readonly tail: Buffer;
    };

const decisionLedgerIndexes = new WeakMap<SecureAutoloopLedger, DecisionLedgerIndex>();
const committedObservationFailures = new Map<string, AutoloopDeliveryOutboxError>();

export interface PrepareDeliveryOptions {
  now?: () => Date;
}

export interface AcknowledgeDeliveryOptions {
  now?: () => Date;
}

function invalidInput(message: string, options?: ErrorOptions): never {
  throw new AutoloopDeliveryOutboxError('AUTOLOOP_DELIVERY_INPUT_INVALID', message, options);
}

function invalidLedger(message: string, options?: ErrorOptions): never {
  throw new AutoloopDeliveryOutboxError('AUTOLOOP_DELIVERY_LEDGER_INVALID', message, options);
}

function idempotencyConflict(message: string, options?: ErrorOptions): never {
  throw new AutoloopDeliveryOutboxError('AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT', message, options);
}

function acknowledgementConflict(message: string, options?: ErrorOptions): never {
  throw new AutoloopDeliveryOutboxError('AUTOLOOP_DELIVERY_ACKNOWLEDGEMENT_CONFLICT', message, options);
}

function canonicalLedgerDirectory(ledger: SecureAutoloopLedger): string {
  try {
    return fs.realpathSync.native(ledger.directory);
  } catch (error) {
    return invalidLedger('Autoloop ledger directory could not be canonically identified', { cause: error });
  }
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function snapshotPrepareDeliveryInput(value: unknown): Readonly<PrepareDeliveryInput> {
  if (typeof value !== 'object' || value === null || nodeUtilTypes.isProxy(value) || Array.isArray(value)) {
    return invalidInput('Delivery input must be a plain, non-proxy object with exact own data fields');
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    return invalidInput('Delivery input must be a plain, non-proxy object with exact own data fields');
  }

  const descriptors = Object.getOwnPropertyDescriptors(value);
  const keys = Reflect.ownKeys(descriptors);
  if (
    keys.length !== PREPARE_DELIVERY_INPUT_FIELDS.length ||
    keys.some((key) => typeof key !== 'string' || !PREPARE_DELIVERY_INPUT_FIELDS.includes(key as never))
  ) {
    return invalidInput('Delivery input must contain exactly the supported own data fields');
  }
  for (const field of PREPARE_DELIVERY_INPUT_FIELDS) {
    const descriptor = descriptors[field];
    if (!descriptor || !Object.hasOwn(descriptor, 'value') || descriptor.enumerable !== true) {
      return invalidInput(`Delivery input field '${field}' must be an enumerable own data property`);
    }
  }

  return Object.freeze({
    idempotency_key: descriptors.idempotency_key!.value as string,
    kind: descriptors.kind!.value as PrepareDeliveryInput['kind'],
    target_role: descriptors.target_role!.value as PrepareDeliveryInput['target_role'],
    target_generation: descriptors.target_generation!.value as number,
    payload: descriptors.payload!.value as unknown,
  });
}

function validateIdempotencyKey(value: unknown): asserts value is string {
  if (
    typeof value !== 'string' ||
    !value.trim() ||
    value.trim() !== value ||
    Buffer.byteLength(value, 'utf8') > MAX_DELIVERY_IDEMPOTENCY_KEY_BYTES
  ) {
    invalidInput(
      `Delivery idempotency_key must be a non-empty unpadded string within ${MAX_DELIVERY_IDEMPOTENCY_KEY_BYTES} UTF-8 bytes`,
    );
  }
}

function validateDeliveryId(value: unknown): asserts value is string {
  if (
    typeof value !== 'string' ||
    !value.trim() ||
    value.trim() !== value ||
    Buffer.byteLength(value, 'utf8') > MAX_DELIVERY_IDEMPOTENCY_KEY_BYTES
  ) {
    invalidInput(
      `Delivery delivery_id must be a non-empty unpadded string within ${MAX_DELIVERY_IDEMPOTENCY_KEY_BYTES} UTF-8 bytes`,
    );
  }
}

function validatePayloadSha256(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value)) {
    invalidInput('Delivery payload_sha256 must be a lowercase SHA-256 digest');
  }
}

function validateTargetGeneration(value: unknown): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    invalidInput('Delivery target_generation must be a nonnegative safe integer');
  }
}

function deliveryCreatedAt(now: () => Date): string {
  let createdAt: unknown;
  try {
    createdAt = now().toISOString();
  } catch (error) {
    return invalidInput('Delivery created_at must be a canonical ISO timestamp', { cause: error });
  }
  if (
    typeof createdAt !== 'string' ||
    Number.isNaN(Date.parse(createdAt)) ||
    new Date(createdAt).toISOString() !== createdAt
  ) {
    return invalidInput('Delivery created_at must be a canonical ISO timestamp');
  }
  return createdAt;
}

function deliveryAcknowledgedAt(now: () => Date): string {
  let acknowledgedAt: unknown;
  try {
    acknowledgedAt = now().toISOString();
  } catch (error) {
    return invalidInput('Delivery acknowledged_at must be a canonical ISO timestamp', { cause: error });
  }
  if (
    typeof acknowledgedAt !== 'string' ||
    Number.isNaN(Date.parse(acknowledgedAt)) ||
    new Date(acknowledgedAt).toISOString() !== acknowledgedAt
  ) {
    return invalidInput('Delivery acknowledged_at must be a canonical ISO timestamp');
  }
  return acknowledgedAt;
}

function validateDeliveryKindAndRole(kind: unknown, targetRole: unknown): void {
  if (
    (kind !== 'coder_directive' && kind !== 'review_request') ||
    (targetRole !== 'coder' && targetRole !== 'reviewer')
  ) {
    invalidInput('Delivery kind and target_role must identify a Coder or Reviewer route');
  }
}

function validateDeliveryRoute(kind: unknown, targetRole: unknown): void {
  if (
    (kind === 'coder_directive' && targetRole !== 'coder') ||
    (kind === 'review_request' && targetRole !== 'reviewer')
  ) {
    invalidInput('Delivery kind and target_role must identify the matching Coder or Reviewer route');
  }
}

function canonicalJson(value: unknown): string {
  const ancestors = new Set<object>();
  const invalid = (reason: string): never => {
    return invalidInput(`Delivery payload is not strict JSON: ${reason}`);
  };
  const serialize = (nestedValue: unknown, depth: number): string => {
    if (depth > MAX_DELIVERY_PAYLOAD_DEPTH) return invalid('nesting depth exceeds 64 edges');
    if (nestedValue === null) return 'null';
    if (typeof nestedValue === 'string' || typeof nestedValue === 'boolean') {
      return JSON.stringify(nestedValue);
    }
    if (typeof nestedValue === 'number') {
      if (!Number.isFinite(nestedValue)) return invalid('numbers must be finite');
      return JSON.stringify(nestedValue);
    }
    if (typeof nestedValue !== 'object') {
      return invalid(`${typeof nestedValue} values are unsupported`);
    }
    if (nodeUtilTypes.isProxy(nestedValue)) return invalid('proxies are unsupported');
    if (ancestors.has(nestedValue)) return invalid('cycles are unsupported');

    ancestors.add(nestedValue);
    try {
      if (Array.isArray(nestedValue)) {
        const prototype = Object.getPrototypeOf(nestedValue);
        if (prototype !== Array.prototype && prototype !== null) {
          return invalid('arrays must have an ordinary prototype');
        }
        const lengthDescriptor = Object.getOwnPropertyDescriptor(nestedValue, 'length');
        if (!lengthDescriptor || !Object.hasOwn(lengthDescriptor, 'value')) {
          return invalid('arrays must have an own data length');
        }
        const length = lengthDescriptor.value as number;
        const toJsonDescriptor = Object.getOwnPropertyDescriptor(nestedValue, 'toJSON');
        const hasSafeToJsonShadow =
          toJsonDescriptor !== undefined &&
          Object.hasOwn(toJsonDescriptor, 'value') &&
          toJsonDescriptor.value === undefined &&
          toJsonDescriptor.configurable === false &&
          toJsonDescriptor.enumerable === false &&
          toJsonDescriptor.writable === false;
        if (toJsonDescriptor !== undefined && !hasSafeToJsonShadow) {
          return invalid('custom toJSON methods are unsupported');
        }
        const keys = Reflect.ownKeys(nestedValue);
        if (keys.length !== length + 1 + (hasSafeToJsonShadow ? 1 : 0)) {
          return invalid('arrays must contain exact contiguous indices');
        }

        const items: string[] = [];
        for (let index = 0; index < length; index += 1) {
          const descriptor = Object.getOwnPropertyDescriptor(nestedValue, String(index));
          if (!descriptor || !Object.hasOwn(descriptor, 'value') || descriptor.enumerable !== true) {
            return invalid('arrays must contain exact contiguous data elements');
          }
          items.push(serialize(descriptor.value, depth + 1));
        }
        for (const key of keys) {
          if (key === 'length' || (key === 'toJSON' && hasSafeToJsonShadow)) continue;
          if (typeof key !== 'string' || !/^(?:0|[1-9]\d*)$/.test(key) || Number(key) >= length) {
            return invalid('arrays must not contain extra properties');
          }
        }
        return `[${items.join(',')}]`;
      }

      const prototype = Object.getPrototypeOf(nestedValue);
      if (prototype !== Object.prototype && prototype !== null) {
        return invalid('objects must be plain');
      }
      const entries: Array<[string, unknown]> = [];
      for (const key of Reflect.ownKeys(nestedValue)) {
        if (typeof key !== 'string') return invalid('symbol properties are unsupported');
        const descriptor = Object.getOwnPropertyDescriptor(nestedValue, key);
        if (!descriptor || !Object.hasOwn(descriptor, 'value') || descriptor.enumerable !== true) {
          return invalid('object fields must be enumerable own data properties');
        }
        entries.push([key, descriptor.value]);
      }
      entries.sort(([left], [right]) => compareStrings(left, right));
      return `{${entries
        .map(([key, entryValue]) => `${JSON.stringify(key)}:${serialize(entryValue, depth + 1)}`)
        .join(',')}}`;
    } finally {
      ancestors.delete(nestedValue);
    }
  };

  return serialize(value, 0);
}

function serializeDeliveryIntent(intent: DeliveryIntent, canonicalPayload: string): string {
  return (
    `{"schema_version":1,"delivery_id":${JSON.stringify(intent.delivery_id)}` +
    `,"idempotency_key":${JSON.stringify(intent.idempotency_key)},"kind":${JSON.stringify(intent.kind)}` +
    `,"target_role":${JSON.stringify(intent.target_role)},"target_generation":${String(intent.target_generation)}` +
    `,"payload":${canonicalPayload},"payload_sha256":${JSON.stringify(intent.payload_sha256)}` +
    `,"created_at":${JSON.stringify(intent.created_at)}}`
  );
}

function serializeDeliveryAcknowledgement(acknowledgement: DeliveryAcknowledgement): string {
  return (
    `{"schema_version":1,"delivery_id":${JSON.stringify(acknowledgement.delivery_id)}` +
    `,"payload_sha256":${JSON.stringify(acknowledgement.payload_sha256)}` +
    `,"acknowledged_at":${JSON.stringify(acknowledgement.acknowledged_at)}}`
  );
}

function sameDeliveryIntent(left: DeliveryIntent, right: DeliveryIntent): boolean {
  return (
    left.schema_version === right.schema_version &&
    left.delivery_id === right.delivery_id &&
    left.idempotency_key === right.idempotency_key &&
    left.kind === right.kind &&
    left.target_role === right.target_role &&
    left.target_generation === right.target_generation &&
    left.payload_sha256 === right.payload_sha256 &&
    left.created_at === right.created_at
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const DELIVERY_INTENT_FIELDS = new Set([
  'schema_version',
  'delivery_id',
  'idempotency_key',
  'kind',
  'target_role',
  'target_generation',
  'payload',
  'payload_sha256',
  'created_at',
]);
const DELIVERY_ACKNOWLEDGEMENT_FIELDS = new Set(['schema_version', 'delivery_id', 'payload_sha256', 'acknowledged_at']);
const DELIVERY_SHAPE_FIELDS = [
  'delivery_id',
  'idempotency_key',
  'target_role',
  'target_generation',
  'payload_sha256',
  'created_at',
  'acknowledged_at',
] as const;

function hasExactEnumerableDataFields(value: Record<string, unknown>, expected: ReadonlySet<string>): boolean {
  const keys = Reflect.ownKeys(value);
  if (keys.length !== expected.size) return false;
  return keys.every((key) => {
    if (typeof key !== 'string' || !expected.has(key)) return false;
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor !== undefined && Object.hasOwn(descriptor, 'value') && descriptor.enumerable === true;
  });
}

function isReservedDeliveryAcknowledgement(value: Record<string, unknown>): boolean {
  return (
    hasExactEnumerableDataFields(value, DELIVERY_ACKNOWLEDGEMENT_FIELDS) &&
    value.schema_version === 1 &&
    typeof value.delivery_id === 'string' &&
    !!value.delivery_id.trim() &&
    value.delivery_id.trim() === value.delivery_id &&
    Buffer.byteLength(value.delivery_id, 'utf8') <= MAX_DELIVERY_IDEMPOTENCY_KEY_BYTES &&
    typeof value.payload_sha256 === 'string' &&
    /^[a-f0-9]{64}$/.test(value.payload_sha256) &&
    typeof value.acknowledged_at === 'string' &&
    !Number.isNaN(Date.parse(value.acknowledged_at))
  );
}

function isCanonicalTimestamp(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    !Number.isNaN(Date.parse(value)) &&
    (() => {
      try {
        return new Date(value).toISOString() === value;
      } catch {
        return false;
      }
    })()
  );
}

function parseDeliveryAcknowledgement(value: unknown, rowNumber: number): DeliveryAcknowledgement | undefined {
  if (!isRecord(value)) return undefined;
  if (isReservedDeliveryAcknowledgement(value) && isCanonicalTimestamp(value.acknowledged_at)) {
    return value as unknown as DeliveryAcknowledgement;
  }
  if (Object.hasOwn(value, 'acknowledged_at')) {
    invalidLedger(`decisions.jsonl record ${rowNumber} is not a valid delivery acknowledgement`);
  }
  return undefined;
}

function isDeliveryShaped(value: Record<string, unknown>): boolean {
  return (
    value.kind === 'coder_directive' ||
    value.kind === 'review_request' ||
    DELIVERY_SHAPE_FIELDS.some((field) => Object.hasOwn(value, field))
  );
}

function parseDeliveryIntent(value: unknown, rowNumber: number): DeliveryIntent | undefined {
  if (!isRecord(value)) {
    invalidLedger(`decisions.jsonl record ${rowNumber} must be a plain object with a non-empty kind`);
  }
  if (isReservedDeliveryAcknowledgement(value)) return undefined;
  if (typeof value.kind !== 'string' || !value.kind.trim()) {
    invalidLedger(`decisions.jsonl record ${rowNumber} must be a plain object with a non-empty kind`);
  }
  if (!isDeliveryShaped(value)) return undefined;
  if (
    !hasExactEnumerableDataFields(value, DELIVERY_INTENT_FIELDS) ||
    value.schema_version !== 1 ||
    typeof value.delivery_id !== 'string' ||
    !value.delivery_id.trim() ||
    value.delivery_id.trim() !== value.delivery_id ||
    Buffer.byteLength(value.delivery_id, 'utf8') > MAX_DELIVERY_IDEMPOTENCY_KEY_BYTES ||
    typeof value.idempotency_key !== 'string' ||
    !value.idempotency_key.trim() ||
    value.idempotency_key.trim() !== value.idempotency_key ||
    Buffer.byteLength(value.idempotency_key, 'utf8') > MAX_DELIVERY_IDEMPOTENCY_KEY_BYTES ||
    (value.kind !== 'coder_directive' && value.kind !== 'review_request') ||
    (value.target_role !== 'coder' && value.target_role !== 'reviewer') ||
    (value.kind === 'coder_directive' && value.target_role !== 'coder') ||
    (value.kind === 'review_request' && value.target_role !== 'reviewer') ||
    !Number.isSafeInteger(value.target_generation) ||
    (value.target_generation as number) < 0 ||
    typeof value.payload_sha256 !== 'string' ||
    !/^[a-f0-9]{64}$/.test(value.payload_sha256) ||
    typeof value.created_at !== 'string' ||
    Number.isNaN(Date.parse(value.created_at))
  ) {
    invalidLedger(`decisions.jsonl record ${rowNumber} is not a valid delivery intent`);
  }

  let canonicalPayload: string;
  try {
    canonicalPayload = canonicalJson(value.payload);
  } catch (error) {
    return invalidLedger(`decisions.jsonl record ${rowNumber} is not a valid delivery intent`, { cause: error });
  }
  const observedDigest = createHash('sha256').update(canonicalPayload, 'utf8').digest('hex');
  if (observedDigest !== value.payload_sha256) {
    invalidLedger(`decisions.jsonl record ${rowNumber} has a mismatched delivery payload digest`);
  }

  return value as unknown as DeliveryIntent;
}

function readExactLedgerBytes(fd: number, start: number, length: number): Buffer {
  const bytes = Buffer.allocUnsafe(length);
  let offset = 0;
  while (offset < bytes.length) {
    const count = fs.readSync(fd, bytes, offset, bytes.length - offset, start + offset);
    if (count <= 0) invalidLedger('decisions.jsonl ended before its validated snapshot was complete');
    offset += count;
  }
  return bytes;
}

function assertStableLedgerSnapshot(before: fs.Stats, after: fs.Stats): void {
  if (
    after.size !== before.size ||
    after.dev !== before.dev ||
    after.ino !== before.ino ||
    after.mtimeMs !== before.mtimeMs ||
    after.ctimeMs !== before.ctimeMs
  ) {
    invalidLedger('decisions.jsonl changed while its stable snapshot was being validated');
  }
}

function decodeLedgerBytes(bytes: Buffer): string {
  try {
    return new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes);
  } catch (error) {
    return invalidLedger('decisions.jsonl is not valid UTF-8', { cause: error });
  }
}

function readBoundedDecisionLedger(
  ledger: SecureAutoloopLedger,
  cached: DecisionLedgerIndex | undefined,
): DecisionLedgerRead {
  let handle;
  try {
    handle = ledger.openFlatFile('decisions.jsonl', 'read');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      if (cached !== undefined) {
        invalidLedger('decisions.jsonl disappeared after its delivery history was observed');
      }
      return { kind: 'missing' };
    }
    throw error;
  }
  try {
    const before = fs.fstatSync(handle.fd);
    if (before.size > MAX_DECISION_LEDGER_BYTES) {
      invalidLedger(`decisions.jsonl exceeds the ${MAX_DECISION_LEDGER_BYTES}-byte recovery limit`);
    }
    let bytes: Buffer;
    let previous: DecisionLedgerIndex | undefined;
    if (cached !== undefined) {
      if (cached.dev !== before.dev || cached.ino !== before.ino) {
        invalidLedger('decisions.jsonl identity changed after its delivery history was observed');
      }
      if (before.size < cached.fileBytes) {
        invalidLedger('decisions.jsonl shrank after its delivery history was observed');
      }
      const verificationStart = cached.fileBytes - cached.tail.length;
      const candidate = readExactLedgerBytes(handle.fd, verificationStart, before.size - verificationStart);
      if (!candidate.subarray(0, cached.tail.length).equals(cached.tail)) {
        invalidLedger('decisions.jsonl cached tail changed after its delivery history was observed');
      }
      if (before.mtimeMs !== cached.mtimeMs || before.ctimeMs !== cached.ctimeMs) {
        bytes = readExactLedgerBytes(handle.fd, 0, before.size);
      } else {
        if (before.size === cached.fileBytes) {
          assertStableLedgerSnapshot(before, fs.fstatSync(handle.fd));
          return { kind: 'unchanged', index: cached };
        } else {
          bytes = candidate.subarray(cached.tail.length);
          previous = cached;
        }
      }
    } else {
      bytes = readExactLedgerBytes(handle.fd, 0, before.size);
    }

    assertStableLedgerSnapshot(before, fs.fstatSync(handle.fd));
    const contents = decodeLedgerBytes(bytes);
    const tailSource = previous === undefined ? bytes : Buffer.concat([previous.tail, bytes]);
    return {
      kind: 'contents',
      contents,
      dev: before.dev,
      ino: before.ino,
      fileBytes: before.size,
      mtimeMs: before.mtimeMs,
      ctimeMs: before.ctimeMs,
      rowOffset: previous?.rowCount ?? 0,
      ...(previous === undefined ? {} : { previous }),
      tail: Buffer.from(tailSource.subarray(Math.max(0, tailSource.length - DECISION_LEDGER_TAIL_VERIFICATION_BYTES))),
    };
  } finally {
    fs.closeSync(handle.fd);
  }
}

function parseDecisionLedgerRows(
  contents: string,
  rowOffset: number,
): {
  intents: DeliveryIntent[];
  acknowledgements: DeliveryAcknowledgement[];
  deliveryRows: DeliveryLedgerRow[];
  rowCount: number;
} {
  if (contents === '') return { intents: [], acknowledgements: [], deliveryRows: [], rowCount: 0 };
  if (!contents.endsWith('\n')) invalidLedger('decisions.jsonl has an incomplete final record');

  const lines = contents.slice(0, -1).split('\n');
  const intents: DeliveryIntent[] = [];
  const acknowledgements: DeliveryAcknowledgement[] = [];
  const deliveryRows: DeliveryLedgerRow[] = [];
  lines.forEach((line, index) => {
    const rowNumber = rowOffset + index + 1;
    const normalized = line.endsWith('\r') ? line.slice(0, -1) : line;
    if (!normalized || Buffer.byteLength(normalized, 'utf8') > MAX_DECISION_LEDGER_ROW_BYTES) {
      invalidLedger(`decisions.jsonl record ${rowNumber} is empty or exceeds its byte limit`);
    }
    let row: unknown;
    try {
      row = JSON.parse(normalized) as unknown;
    } catch (error) {
      return invalidLedger(`decisions.jsonl record ${rowNumber} is malformed`, { cause: error });
    }
    const acknowledgement = parseDeliveryAcknowledgement(row, rowNumber);
    if (acknowledgement !== undefined) {
      acknowledgements.push(acknowledgement);
      deliveryRows.push({ kind: 'acknowledgement', rowNumber, acknowledgement });
      return;
    }
    const intent = parseDeliveryIntent(row, rowNumber);
    if (intent !== undefined) {
      intents.push(intent);
      deliveryRows.push({ kind: 'intent', rowNumber, intent });
    }
  });
  return { intents, acknowledgements, deliveryRows, rowCount: lines.length };
}

function addIndexedIntents(index: Map<string, DeliveryIntent[]>, intents: readonly DeliveryIntent[]): void {
  for (const intent of intents) {
    const matching = index.get(intent.idempotency_key);
    if (matching === undefined) index.set(intent.idempotency_key, [intent]);
    else matching.push(intent);
  }
}

function addIndexedIntentsByDeliveryId(index: Map<string, DeliveryIntent[]>, intents: readonly DeliveryIntent[]): void {
  for (const intent of intents) {
    const matching = index.get(intent.delivery_id);
    if (matching === undefined) index.set(intent.delivery_id, [intent]);
    else matching.push(intent);
  }
}

function addIndexedAcknowledgements(
  index: Map<string, DeliveryAcknowledgement[]>,
  acknowledgements: readonly DeliveryAcknowledgement[],
): void {
  for (const acknowledgement of acknowledgements) {
    const matching = index.get(acknowledgement.delivery_id);
    if (matching === undefined) index.set(acknowledgement.delivery_id, [acknowledgement]);
    else matching.push(acknowledgement);
  }
}

function validateDeliveryGraph(deliveryRows: readonly DeliveryLedgerRow[]): void {
  const intentsByDeliveryId = new Map<string, DeliveryLedgerRow & { readonly kind: 'intent' }>();
  const intentsByIdempotencyKey = new Map<string, DeliveryLedgerRow & { readonly kind: 'intent' }>();
  const acknowledgementsByDeliveryId = new Map<string, DeliveryLedgerRow & { readonly kind: 'acknowledgement' }>();

  for (const row of deliveryRows) {
    if (row.kind === 'intent') {
      if (intentsByDeliveryId.has(row.intent.delivery_id)) {
        idempotencyConflict(`decisions.jsonl record ${row.rowNumber} duplicates a persisted delivery_id`);
      }
      if (intentsByIdempotencyKey.has(row.intent.idempotency_key)) {
        idempotencyConflict(`decisions.jsonl record ${row.rowNumber} duplicates a persisted idempotency_key`);
      }
      intentsByDeliveryId.set(row.intent.delivery_id, row);
      intentsByIdempotencyKey.set(row.intent.idempotency_key, row);
      continue;
    }

    const intent = intentsByDeliveryId.get(row.acknowledgement.delivery_id);
    if (intent === undefined) {
      invalidLedger(`decisions.jsonl record ${row.rowNumber} acknowledges no earlier delivery intent`);
    }
    if (intent.intent.payload_sha256 !== row.acknowledgement.payload_sha256) {
      invalidLedger(
        `decisions.jsonl record ${row.rowNumber} acknowledgement digest does not match its earlier delivery intent`,
      );
    }
    if (acknowledgementsByDeliveryId.has(row.acknowledgement.delivery_id)) {
      invalidLedger(`decisions.jsonl record ${row.rowNumber} duplicates a persisted delivery acknowledgement`);
    }
    acknowledgementsByDeliveryId.set(row.acknowledgement.delivery_id, row);
  }
}

function readDeliveryIntentIndex(ledger: SecureAutoloopLedger): DecisionLedgerIndex | undefined {
  const cached = decisionLedgerIndexes.get(ledger);
  const read = readBoundedDecisionLedger(ledger, cached);
  if (read.kind === 'missing') {
    decisionLedgerIndexes.delete(ledger);
    return undefined;
  }
  if (read.kind === 'unchanged') return read.index;

  const parsed = parseDecisionLedgerRows(read.contents, read.rowOffset);
  const deliveryRows = [...(read.previous?.deliveryRows ?? []), ...parsed.deliveryRows];
  validateDeliveryGraph(deliveryRows);
  const intentsByIdempotencyKey = read.previous?.intentsByIdempotencyKey ?? new Map<string, DeliveryIntent[]>();
  const intentsByDeliveryId = read.previous?.intentsByDeliveryId ?? new Map<string, DeliveryIntent[]>();
  const acknowledgementsByDeliveryId =
    read.previous?.acknowledgementsByDeliveryId ?? new Map<string, DeliveryAcknowledgement[]>();
  addIndexedIntents(intentsByIdempotencyKey, parsed.intents);
  addIndexedIntentsByDeliveryId(intentsByDeliveryId, parsed.intents);
  addIndexedAcknowledgements(acknowledgementsByDeliveryId, parsed.acknowledgements);
  const index: DecisionLedgerIndex = {
    dev: read.dev,
    ino: read.ino,
    fileBytes: read.fileBytes,
    mtimeMs: read.mtimeMs,
    ctimeMs: read.ctimeMs,
    rowCount: read.rowOffset + parsed.rowCount,
    tail: read.tail,
    deliveryRows,
    intentsByIdempotencyKey,
    intentsByDeliveryId,
    acknowledgementsByDeliveryId,
  };
  decisionLedgerIndexes.set(ledger, index);
  return index;
}

function cloneDeliveryIntent(intent: DeliveryIntent): DeliveryIntent {
  return { ...intent, payload: structuredClone(intent.payload) };
}

function cloneDeliveryAcknowledgement(acknowledgement: DeliveryAcknowledgement): DeliveryAcknowledgement {
  return { ...acknowledgement };
}

function findDeliveryIntent(
  ledger: SecureAutoloopLedger,
  idempotencyKey: string,
): { intent: DeliveryIntent | undefined; fileBytes: number; dev: number | undefined; ino: number | undefined } {
  const index = readDeliveryIntentIndex(ledger);
  const matching = index?.intentsByIdempotencyKey.get(idempotencyKey) ?? [];
  if (matching.length > 1) {
    idempotencyConflict(`Delivery idempotency key '${idempotencyKey}' has duplicate persisted intents`);
  }
  return {
    intent: matching[0] === undefined ? undefined : cloneDeliveryIntent(matching[0]),
    fileBytes: index?.fileBytes ?? 0,
    dev: index?.dev,
    ino: index?.ino,
  };
}

function findDeliveryById(
  ledger: SecureAutoloopLedger,
  deliveryId: string,
): {
  intent: DeliveryIntent | undefined;
  acknowledgement: DeliveryAcknowledgement | undefined;
  intentCount: number;
  acknowledgementCount: number;
  fileBytes: number;
  dev: number | undefined;
  ino: number | undefined;
} {
  const index = readDeliveryIntentIndex(ledger);
  const intents = index?.intentsByDeliveryId.get(deliveryId) ?? [];
  const acknowledgements = index?.acknowledgementsByDeliveryId.get(deliveryId) ?? [];
  if (intents.length > 1) idempotencyConflict(`Delivery id '${deliveryId}' has duplicate persisted intents`);
  if (acknowledgements.length > 1) {
    invalidLedger(`Delivery id '${deliveryId}' has ambiguous persisted acknowledgements`);
  }
  const intent = intents[0];
  const acknowledgement = acknowledgements[0];
  if (
    intent !== undefined &&
    acknowledgement !== undefined &&
    acknowledgement.payload_sha256 !== intent.payload_sha256
  ) {
    invalidLedger(`Delivery acknowledgement for '${deliveryId}' does not match its persisted intent digest`);
  }
  return {
    intent: intent === undefined ? undefined : cloneDeliveryIntent(intent),
    acknowledgement: acknowledgement === undefined ? undefined : cloneDeliveryAcknowledgement(acknowledgement),
    intentCount: intents.length,
    acknowledgementCount: acknowledgements.length,
    fileBytes: index?.fileBytes ?? 0,
    dev: index?.dev,
    ino: index?.ino,
  };
}

function observeDurableDeliveryAcknowledgement(
  ledger: SecureAutoloopLedger,
  deliveryId: string,
  payloadSha256: string,
  acknowledgedAt: string,
  barrierDev: number,
  barrierIno: number,
): DeliveryAcknowledgement {
  // Acknowledgement success is published only from a full, post-barrier ledger
  // observation, never from the index read before that barrier.
  decisionLedgerIndexes.delete(ledger);
  const observed = findDeliveryById(ledger, deliveryId);
  if (
    observed.intentCount !== 1 ||
    !observed.intent ||
    observed.intent.delivery_id !== deliveryId ||
    observed.intent.payload_sha256 !== payloadSha256 ||
    observed.acknowledgementCount !== 1 ||
    !observed.acknowledgement ||
    observed.acknowledgement.delivery_id !== deliveryId ||
    observed.acknowledgement.payload_sha256 !== payloadSha256 ||
    observed.acknowledgement.acknowledged_at !== acknowledgedAt ||
    observed.dev !== barrierDev ||
    observed.ino !== barrierIno
  ) {
    invalidLedger(
      'Delivery acknowledgement was not freshly observed with its exact persisted intent and one matching row',
    );
  }
  return observed.acknowledgement;
}

function committedObservationFailure(message: string, cause: unknown): AutoloopDeliveryOutboxError {
  return new AutoloopDeliveryOutboxError('AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED', message, {
    cause,
    committed: true,
  });
}

function latchCommittedObservationFailure(
  observationFailureKey: string,
  message: string,
  committedError: Error,
  proofFailure: unknown,
): never {
  const failure = committedObservationFailure(message, committedError);
  failure.secondaryErrors.push(proofFailure instanceof Error ? proofFailure : new Error(String(proofFailure)));
  committedObservationFailures.set(observationFailureKey, failure);
  throw failure;
}

function observeAppendedDeliveryIntent(
  ledger: SecureAutoloopLedger,
  prepared: SecureAutoloopPreparedAppend,
  previousFileBytes: number,
  serializedIntent: string,
  intent: DeliveryIntent,
): DeliveryIntent {
  const appended = Buffer.from(`${serializedIntent}\n`, 'utf8');
  const cached = decisionLedgerIndexes.get(ledger);
  if (cached === undefined) {
    const pinnedLastLine = Buffer.from(prepared.readLastNonEmptyLine(), 'utf8');
    if (!pinnedLastLine.equals(Buffer.from(serializedIntent, 'utf8'))) {
      throw committedObservationFailure(
        'Pinned decisions.jsonl append no longer contains the intended delivery row',
        new Error('pinned row mismatch'),
      );
    }
  }
  const observePathnameRange = (): fs.Stats => {
    const handle = ledger.openFlatFile('decisions.jsonl', 'read');
    try {
      const observed = fs.fstatSync(handle.fd);
      if (
        observed.size !== previousFileBytes + appended.length ||
        (cached !== undefined && (cached.dev !== observed.dev || cached.ino !== observed.ino)) ||
        (cached === undefined && previousFileBytes !== 0)
      ) {
        invalidLedger('Appended delivery intent could not be observed at its expected decision-ledger range');
      }
      const observedRange = readExactLedgerBytes(handle.fd, previousFileBytes, appended.length);
      if (!observedRange.equals(appended)) {
        invalidLedger('Appended delivery intent does not match its expected decision-ledger range');
      }
      assertStableLedgerSnapshot(observed, fs.fstatSync(handle.fd));
      return observed;
    } finally {
      fs.closeSync(handle.fd);
    }
  };

  const beforeDurabilityBarrier = observePathnameRange();
  // The prepared descriptor proves the original append, while this fresh barrier
  // proves the pathname bytes we are about to cache are durable too.
  const flushed = ledger.flushFlatFile('decisions.jsonl');
  const observed = observePathnameRange();
  if (observed.dev !== beforeDurabilityBarrier.dev || observed.ino !== beforeDurabilityBarrier.ino) {
    invalidLedger('decisions.jsonl identity changed across the appended delivery durability barrier');
  }
  if (observed.dev !== flushed.dev || observed.ino !== flushed.ino) {
    invalidLedger('decisions.jsonl durability barrier did not cover its observed descriptor');
  }

  const intentsByIdempotencyKey = cached?.intentsByIdempotencyKey ?? new Map<string, DeliveryIntent[]>();
  const intentsByDeliveryId = cached?.intentsByDeliveryId ?? new Map<string, DeliveryIntent[]>();
  const acknowledgementsByDeliveryId =
    cached?.acknowledgementsByDeliveryId ?? new Map<string, DeliveryAcknowledgement[]>();
  addIndexedIntents(intentsByIdempotencyKey, [cloneDeliveryIntent(intent)]);
  addIndexedIntentsByDeliveryId(intentsByDeliveryId, [cloneDeliveryIntent(intent)]);
  const tailSource = Buffer.concat([cached?.tail ?? Buffer.alloc(0), appended]);
  decisionLedgerIndexes.set(ledger, {
    dev: observed.dev,
    ino: observed.ino,
    fileBytes: observed.size,
    mtimeMs: observed.mtimeMs,
    ctimeMs: observed.ctimeMs,
    rowCount: (cached?.rowCount ?? 0) + 1,
    tail: Buffer.from(tailSource.subarray(Math.max(0, tailSource.length - DECISION_LEDGER_TAIL_VERIFICATION_BYTES))),
    deliveryRows: [
      ...(cached?.deliveryRows ?? []),
      { kind: 'intent', rowNumber: (cached?.rowCount ?? 0) + 1, intent: cloneDeliveryIntent(intent) },
    ],
    intentsByIdempotencyKey,
    intentsByDeliveryId,
    acknowledgementsByDeliveryId,
  });
  return cloneDeliveryIntent(intent);
}

function observeAppendedDeliveryAcknowledgement(
  ledger: SecureAutoloopLedger,
  prepared: SecureAutoloopPreparedAppend,
  previousFileBytes: number,
  serializedAcknowledgement: string,
  acknowledgement: DeliveryAcknowledgement,
): { acknowledgement: DeliveryAcknowledgement; barrierDev: number; barrierIno: number } {
  const appended = Buffer.from(`${serializedAcknowledgement}\n`, 'utf8');
  const cached = decisionLedgerIndexes.get(ledger);
  if (cached === undefined) {
    const pinnedLastLine = Buffer.from(prepared.readLastNonEmptyLine(), 'utf8');
    if (!pinnedLastLine.equals(Buffer.from(serializedAcknowledgement, 'utf8'))) {
      throw committedObservationFailure(
        'Pinned decisions.jsonl append no longer contains the intended delivery acknowledgement row',
        new Error('pinned row mismatch'),
      );
    }
  }
  const observePathnameRange = (): fs.Stats => {
    const handle = ledger.openFlatFile('decisions.jsonl', 'read');
    try {
      const observed = fs.fstatSync(handle.fd);
      if (
        observed.size !== previousFileBytes + appended.length ||
        (cached !== undefined && (cached.dev !== observed.dev || cached.ino !== observed.ino)) ||
        (cached === undefined && previousFileBytes !== 0)
      ) {
        invalidLedger('Appended delivery acknowledgement could not be observed at its expected decision-ledger range');
      }
      const observedRange = readExactLedgerBytes(handle.fd, previousFileBytes, appended.length);
      if (!observedRange.equals(appended)) {
        invalidLedger('Appended delivery acknowledgement does not match its expected decision-ledger range');
      }
      assertStableLedgerSnapshot(observed, fs.fstatSync(handle.fd));
      return observed;
    } finally {
      fs.closeSync(handle.fd);
    }
  };

  const beforeDurabilityBarrier = observePathnameRange();
  const flushed = ledger.flushFlatFile('decisions.jsonl');
  const observed = observePathnameRange();
  if (observed.dev !== beforeDurabilityBarrier.dev || observed.ino !== beforeDurabilityBarrier.ino) {
    invalidLedger('decisions.jsonl identity changed across the appended delivery acknowledgement durability barrier');
  }
  if (observed.dev !== flushed.dev || observed.ino !== flushed.ino) {
    invalidLedger('decisions.jsonl durability barrier did not cover its observed delivery acknowledgement descriptor');
  }

  const intentsByIdempotencyKey = cached?.intentsByIdempotencyKey ?? new Map<string, DeliveryIntent[]>();
  const intentsByDeliveryId = cached?.intentsByDeliveryId ?? new Map<string, DeliveryIntent[]>();
  const acknowledgementsByDeliveryId =
    cached?.acknowledgementsByDeliveryId ?? new Map<string, DeliveryAcknowledgement[]>();
  addIndexedAcknowledgements(acknowledgementsByDeliveryId, [cloneDeliveryAcknowledgement(acknowledgement)]);
  const tailSource = Buffer.concat([cached?.tail ?? Buffer.alloc(0), appended]);
  decisionLedgerIndexes.set(ledger, {
    dev: observed.dev,
    ino: observed.ino,
    fileBytes: observed.size,
    mtimeMs: observed.mtimeMs,
    ctimeMs: observed.ctimeMs,
    rowCount: (cached?.rowCount ?? 0) + 1,
    tail: Buffer.from(tailSource.subarray(Math.max(0, tailSource.length - DECISION_LEDGER_TAIL_VERIFICATION_BYTES))),
    deliveryRows: [
      ...(cached?.deliveryRows ?? []),
      {
        kind: 'acknowledgement',
        rowNumber: (cached?.rowCount ?? 0) + 1,
        acknowledgement: cloneDeliveryAcknowledgement(acknowledgement),
      },
    ],
    intentsByIdempotencyKey,
    intentsByDeliveryId,
    acknowledgementsByDeliveryId,
  });
  return {
    acknowledgement: cloneDeliveryAcknowledgement(acknowledgement),
    barrierDev: flushed.dev,
    barrierIno: flushed.ino,
  };
}

function closePreparedAppend(prepared: SecureAutoloopPreparedAppend, primaryError?: unknown): Error | undefined {
  try {
    prepared.close();
    return undefined;
  } catch (closeError) {
    const secondary = closeError instanceof Error ? closeError : new Error(String(closeError));
    if (isCommittedSecureLedgerError(primaryError) || primaryError instanceof AutoloopDeliveryOutboxError) {
      primaryError.secondaryErrors.push(secondary);
      return undefined;
    }
    return secondary;
  }
}

export function lookupByIdempotencyKey(
  ledger: SecureAutoloopLedger,
  idempotencyKey: string,
): DeliveryIntent | undefined {
  validateIdempotencyKey(idempotencyKey);
  return findDeliveryIntent(ledger, idempotencyKey).intent;
}

/** Persist one receiver acknowledgement for the exact intent digest, or return its original durable record. */
export function acknowledgeDelivery(
  ledger: SecureAutoloopLedger,
  deliveryId: string,
  payloadSha256: string,
  options: AcknowledgeDeliveryOptions = {},
): DeliveryAcknowledgement {
  validateDeliveryId(deliveryId);
  validatePayloadSha256(payloadSha256);
  let acknowledgedAt: string | undefined;
  let acknowledgementClockFailure: unknown;
  try {
    const now = options.now;
    acknowledgedAt = deliveryAcknowledgedAt(now ?? (() => new Date()));
  } catch (error) {
    acknowledgementClockFailure =
      error instanceof AutoloopDeliveryOutboxError && error.code === 'AUTOLOOP_DELIVERY_INPUT_INVALID'
        ? error
        : new AutoloopDeliveryOutboxError(
            'AUTOLOOP_DELIVERY_INPUT_INVALID',
            'Delivery acknowledged_at must be a canonical ISO timestamp',
            { cause: error },
          );
  }
  ledger.assertIdentity();
  let locked;
  try {
    locked = withFileLock(
      path.join(ledger.directory, '.delivery-outbox.lock'),
      () => {
        ledger.assertIdentity();
        const observationFailureKey = canonicalLedgerDirectory(ledger);
        const halted = committedObservationFailures.get(observationFailureKey);
        if (halted !== undefined) throw halted;
        const existing = findDeliveryById(ledger, deliveryId);
        if (existing.intent === undefined) {
          acknowledgementConflict(`Delivery acknowledgement references unknown delivery '${deliveryId}'`);
        }
        if (existing.intent.payload_sha256 !== payloadSha256) {
          acknowledgementConflict(`Delivery acknowledgement digest does not match '${deliveryId}'`);
        }
        if (existing.acknowledgement !== undefined) {
          const flushed = ledger.flushFlatFile('decisions.jsonl');
          if (
            existing.dev === undefined ||
            existing.ino === undefined ||
            flushed.dev !== existing.dev ||
            flushed.ino !== existing.ino
          ) {
            invalidLedger(
              'Existing delivery acknowledgement durability barrier covered a different decisions.jsonl identity',
            );
          }
          return observeDurableDeliveryAcknowledgement(
            ledger,
            deliveryId,
            payloadSha256,
            existing.acknowledgement.acknowledged_at,
            flushed.dev,
            flushed.ino,
          );
        }

        if (acknowledgementClockFailure !== undefined || acknowledgedAt === undefined) {
          throw acknowledgementClockFailure;
        }

        const acknowledgement: DeliveryAcknowledgement = {
          schema_version: 1,
          delivery_id: deliveryId,
          payload_sha256: payloadSha256,
          acknowledged_at: acknowledgedAt,
        };
        const serialized = serializeDeliveryAcknowledgement(acknowledgement);
        if (Buffer.byteLength(serialized, 'utf8') > MAX_DECISION_LEDGER_ROW_BYTES) {
          invalidInput(`Delivery acknowledgement row exceeds the ${MAX_DECISION_LEDGER_ROW_BYTES}-byte limit`);
        }
        if (existing.fileBytes + Buffer.byteLength(serialized, 'utf8') + 1 > MAX_DECISION_LEDGER_BYTES) {
          invalidLedger(`Delivery acknowledgement would exceed the ${MAX_DECISION_LEDGER_BYTES}-byte ledger limit`);
        }

        const prepared = ledger.prepareFlatFileAppend('decisions.jsonl', `${serialized}\n`);
        try {
          prepared.commitDurable();
          const barrier = observeAppendedDeliveryAcknowledgement(
            ledger,
            prepared,
            existing.fileBytes,
            serialized,
            acknowledgement,
          );
          const closeFailure = closePreparedAppend(prepared);
          if (closeFailure !== undefined) {
            throw committedObservationFailure(
              'Committed delivery acknowledgement could not close its pinned observation descriptor',
              closeFailure,
            );
          }
          return observeDurableDeliveryAcknowledgement(
            ledger,
            deliveryId,
            payloadSha256,
            acknowledgement.acknowledged_at,
            barrier.barrierDev,
            barrier.barrierIno,
          );
        } catch (error) {
          closePreparedAppend(prepared, error);
          if (!isCommittedSecureLedgerError(error)) {
            if (!prepared.committed) throw error;
            const failure =
              error instanceof AutoloopDeliveryOutboxError && error.committed
                ? error
                : committedObservationFailure('Committed delivery acknowledgement could not be safely observed', error);
            committedObservationFailures.set(observationFailureKey, failure);
            throw failure;
          }
          try {
            decisionLedgerIndexes.delete(ledger);
            const reconciled = findDeliveryById(ledger, deliveryId);
            if (
              !reconciled.intent ||
              reconciled.intent.payload_sha256 !== payloadSha256 ||
              !reconciled.acknowledgement ||
              reconciled.acknowledgement.payload_sha256 !== payloadSha256 ||
              reconciled.acknowledgement.acknowledged_at !== acknowledgedAt
            ) {
              invalidLedger('Committed delivery acknowledgement could not be reconciled to its exact persisted row');
            }
            const flushed = ledger.flushFlatFile('decisions.jsonl');
            const observed = observeDurableDeliveryAcknowledgement(
              ledger,
              deliveryId,
              payloadSha256,
              acknowledgedAt,
              flushed.dev,
              flushed.ino,
            );
            if (
              reconciled.dev === undefined ||
              reconciled.ino === undefined ||
              flushed.dev !== reconciled.dev ||
              flushed.ino !== reconciled.ino ||
              observed.acknowledged_at !== acknowledgedAt
            ) {
              invalidLedger(
                'Committed delivery acknowledgement recovery durability barrier did not cover its exact row',
              );
            }
            return observed;
          } catch (proofFailure) {
            latchCommittedObservationFailure(
              observationFailureKey,
              'Committed delivery acknowledgement could not be observed at its exact durable decision-ledger row after recovery',
              error,
              proofFailure,
            );
          }
        }
      },
      { waitMs: DELIVERY_OUTBOX_LOCK_WAIT_MS },
    );
  } catch (error) {
    if (!isFileLockReleaseError(error)) throw error;
    const observationFailureKey = canonicalLedgerDirectory(ledger);
    try {
      decisionLedgerIndexes.delete(ledger);
      const persisted = findDeliveryById(ledger, deliveryId).acknowledgement;
      if (!persisted || persisted.payload_sha256 !== payloadSha256) {
        throw error;
      }
    } catch (reconciliationError) {
      const failure = committedObservationFailure(
        'Committed delivery acknowledgement could not be reconciled after its lock release failed',
        reconciliationError,
      );
      committedObservationFailures.set(observationFailureKey, failure);
      throw failure;
    }
    const failure = committedObservationFailure(
      'Committed delivery acknowledgement could not safely release its outbox lock',
      error,
    );
    committedObservationFailures.set(observationFailureKey, failure);
    throw failure;
  }

  if (!locked.ok) {
    if (locked.reason === 'cleanup_failed') {
      throw new AutoloopDeliveryOutboxError(
        'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CLEANUP_FAILED',
        `Autoloop delivery outbox lock cleanup failed after a published acquisition: ${locked.error}`,
        { cause: locked.cause },
      );
    }
    throw new AutoloopDeliveryOutboxError(
      'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CONTENDED',
      `Autoloop delivery outbox is contended: ${locked.error}`,
    );
  }
  return locked.value;
}

export function prepareDelivery(
  ledger: SecureAutoloopLedger,
  input: PrepareDeliveryInput,
  options: PrepareDeliveryOptions = {},
): DeliveryIntent {
  const snapshot = snapshotPrepareDeliveryInput(input);
  const { idempotency_key, kind, target_role, target_generation, payload } = snapshot;
  validateIdempotencyKey(idempotency_key);
  validateTargetGeneration(target_generation);
  validateDeliveryKindAndRole(kind, target_role);
  const canonicalPayload = canonicalJson(payload);
  const payloadSha256 = createHash('sha256').update(canonicalPayload, 'utf8').digest('hex');
  ledger.assertIdentity();
  let locked;
  try {
    locked = withFileLock(
      path.join(ledger.directory, '.delivery-outbox.lock'),
      () => {
        ledger.assertIdentity();
        const observationFailureKey = canonicalLedgerDirectory(ledger);
        const halted = committedObservationFailures.get(observationFailureKey);
        if (halted !== undefined) throw halted;
        const { intent: existing, fileBytes, dev, ino } = findDeliveryIntent(ledger, idempotency_key);
        if (existing) {
          if (
            existing.kind !== kind ||
            existing.target_role !== target_role ||
            existing.target_generation !== target_generation ||
            existing.payload_sha256 !== payloadSha256
          ) {
            idempotencyConflict(`Delivery idempotency key '${idempotency_key}' conflicts with its persisted intent`);
          }
          const flushed = ledger.flushFlatFile('decisions.jsonl');
          if (dev === undefined || ino === undefined || flushed.dev !== dev || flushed.ino !== ino) {
            invalidLedger('Existing delivery intent durability barrier covered a different decisions.jsonl identity');
          }
          const observed = findDeliveryIntent(ledger, idempotency_key).intent;
          if (!observed || !sameDeliveryIntent(observed, existing)) {
            invalidLedger('Existing delivery intent was not observed at its exact durable decision-ledger row');
          }
          return observed;
        }
        validateDeliveryRoute(kind, target_role);

        const intent: DeliveryIntent = {
          schema_version: 1,
          delivery_id: randomUUID(),
          idempotency_key,
          kind,
          target_role,
          target_generation,
          payload: JSON.parse(canonicalPayload) as unknown,
          payload_sha256: payloadSha256,
          created_at: deliveryCreatedAt(options.now ?? (() => new Date())),
        };

        const serializedIntent = serializeDeliveryIntent(intent, canonicalPayload);
        if (Buffer.byteLength(serializedIntent, 'utf8') > MAX_DECISION_LEDGER_ROW_BYTES) {
          invalidInput(`Delivery intent row exceeds the ${MAX_DECISION_LEDGER_ROW_BYTES}-byte limit`);
        }
        const appendedBytes = Buffer.byteLength(serializedIntent, 'utf8') + 1;
        if (fileBytes + appendedBytes > MAX_DECISION_LEDGER_BYTES) {
          invalidLedger(`Delivery intent would exceed the ${MAX_DECISION_LEDGER_BYTES}-byte ledger limit`);
        }

        const prepared = ledger.prepareFlatFileAppend('decisions.jsonl', `${serializedIntent}\n`);
        try {
          prepared.commitDurable();
          const observed = observeAppendedDeliveryIntent(ledger, prepared, fileBytes, serializedIntent, intent);
          const closeFailure = closePreparedAppend(prepared);
          if (closeFailure !== undefined) {
            throw committedObservationFailure(
              'Committed delivery intent could not close its pinned observation descriptor',
              closeFailure,
            );
          }
          return observed;
        } catch (error) {
          closePreparedAppend(prepared, error);
          if (!isCommittedSecureLedgerError(error)) {
            if (!prepared.committed) throw error;
            const failure =
              error instanceof AutoloopDeliveryOutboxError && error.committed
                ? error
                : committedObservationFailure('Committed delivery intent could not be safely observed', error);
            committedObservationFailures.set(observationFailureKey, failure);
            throw failure;
          }
          try {
            const reconciled = findDeliveryIntent(ledger, idempotency_key);
            const persisted = reconciled.intent;
            if (!persisted || !sameDeliveryIntent(persisted, intent)) {
              throw new AutoloopDeliveryOutboxError(
                'AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT',
                'Committed delivery intent could not be reconciled to its exact persisted row',
              );
            }
            const flushed = ledger.flushFlatFile('decisions.jsonl');
            if (
              reconciled.dev === undefined ||
              reconciled.ino === undefined ||
              flushed.dev !== reconciled.dev ||
              flushed.ino !== reconciled.ino
            ) {
              invalidLedger(
                'Committed delivery recovery durability barrier covered a different decisions.jsonl identity',
              );
            }
            const observed = findDeliveryIntent(ledger, idempotency_key).intent;
            if (!observed || !sameDeliveryIntent(observed, persisted)) {
              invalidLedger(
                'Committed delivery recovery intent was not observed at its exact durable decision-ledger row',
              );
            }
            return observed;
          } catch (proofFailure) {
            latchCommittedObservationFailure(
              observationFailureKey,
              'Committed delivery intent could not be observed at its exact durable decision-ledger row after recovery',
              error,
              proofFailure,
            );
          }
        }
      },
      { waitMs: DELIVERY_OUTBOX_LOCK_WAIT_MS },
    );
  } catch (error) {
    if (!isFileLockReleaseError(error)) throw error;
    const observationFailureKey = canonicalLedgerDirectory(ledger);
    let persisted: DeliveryIntent | undefined;
    try {
      persisted = findDeliveryIntent(ledger, idempotency_key).intent;
    } catch (reconciliationError) {
      const failure = committedObservationFailure(
        'Committed delivery intent could not be reconciled after its lock release failed',
        reconciliationError,
      );
      committedObservationFailures.set(observationFailureKey, failure);
      throw failure;
    }
    if (
      !persisted ||
      persisted.kind !== kind ||
      persisted.target_role !== target_role ||
      persisted.target_generation !== target_generation ||
      persisted.payload_sha256 !== payloadSha256
    ) {
      const failure = committedObservationFailure(
        'Committed delivery intent could not be reconciled after its lock release failed',
        error,
      );
      committedObservationFailures.set(observationFailureKey, failure);
      throw failure;
    }
    const failure = committedObservationFailure(
      'Committed delivery intent could not safely release its outbox lock',
      error,
    );
    committedObservationFailures.set(observationFailureKey, failure);
    throw failure;
  }

  if (!locked.ok) {
    if (locked.reason === 'cleanup_failed') {
      throw new AutoloopDeliveryOutboxError(
        'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CLEANUP_FAILED',
        `Autoloop delivery outbox lock cleanup failed after a published acquisition: ${locked.error}`,
        { cause: locked.cause },
      );
    }
    throw new AutoloopDeliveryOutboxError(
      'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CONTENDED',
      `Autoloop delivery outbox is contended: ${locked.error}`,
    );
  }
  return locked.value;
}
