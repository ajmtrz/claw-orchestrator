/**
 * Council bound-input preflight — configuration readiness, orchestrator input
 * verification, and mode selection. Native engine policy is never claimed here.
 */

import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn } from 'node:child_process';
import type { AgentPersona, CustomEngineConfig, EngineType, PermissionMode } from './types.js';
import { GIT_CMD_TIMEOUT_MS, WORKTREE_CMD_TIMEOUT_MS } from './constants.js';

// ─── Fixed vocabulary (never interpolate raw stderr / paths that failed validation) ─

export const PREFLIGHT_REASONS = [
  'contract-mixed',
  'capability-unsupported',
  'capability-unknown',
  'base-absent',
  'base-unreadable',
  'base-mismatch',
  'target-exists',
  'input-invalid',
  'input-missing',
  'input-hash-mismatch',
  'input-too-large',
  'input-blocked-source',
  'input-not-regular',
  'input-outside-view',
  'head-mismatch',
  'net-change-detected',
  'publish-failed',
] as const;

export type PreflightReason = (typeof PREFLIGHT_REASONS)[number];

export type CouncilRunMode = 'read-only' | 'git';

export interface CouncilInputSpec {
  /** Relative path inside the member view. */
  path: string;
  /** Lowercase hex sha256 of the bytes that must appear at path. */
  sha256: string;
  /** Absolute host path of provenance bytes (optional). */
  source?: string;
}

export interface PreflightFailure {
  member?: string;
  engine?: string;
  check: string;
  subject: string;
  reason: PreflightReason;
  correction?: string;
}

export interface PreflightMemberRecord {
  member: string;
  engine: string;
  view: string;
  head?: string;
  inputs: Array<{ path: string; sha256: string; ok: boolean }>;
  outsideRead: 'open' | 'unavailable' | 'unknown';
}

export interface CouncilPreflightResult {
  ok: boolean;
  mode: CouncilRunMode;
  base: string;
  members: PreflightMemberRecord[];
  failures: PreflightFailure[];
}

export interface EffectiveMember {
  agent: AgentPersona;
  engine: EngineType;
  permissionMode: PermissionMode;
  sandboxMode?: 'read-only' | 'workspace-write' | 'danger-full-access';
  readOnlyEffective: boolean;
  strictRequest: boolean;
}

const INPUT_PATH_RE = /^[A-Za-z0-9._/-]{1,256}$/;
const SHA256_RE = /^[0-9a-f]{64}$/;
const MAX_INPUTS = 32;
const MAX_INPUT_BYTES = 32 * 1024 * 1024;

const BLOCKED_PREFIXES = ['/etc', '/proc', '/sys', '/var/run', '/var/log', '/boot', '/sbin'];
const BLOCKED_HOME_SUBDIRS = ['.ssh', '.gnupg', '.aws', '.config/gcloud'];

function spawnAsync(
  cmd: string,
  args: string[],
  opts: { timeout: number },
): Promise<{ stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error('timeout'));
    }, opts.timeout);
    child.stdout.on('data', (d: Buffer) => {
      stdout += d.toString();
    });
    child.stderr.on('data', (d: Buffer) => {
      stderr += d.toString();
    });
    child.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`exit ${code}`));
    });
  });
}

/** True when dir is under the OS temp directory (mirrors grok helper; no raw path echoed). */
export function pathUnderTempDir(dir: string): boolean {
  try {
    const real = fs.realpathSync(dir);
    const tmp = fs.realpathSync(os.tmpdir());
    return real === tmp || real.startsWith(tmp + path.sep);
  } catch {
    return false;
  }
}

export function isBlockedLocation(candidate: string): boolean {
  let logical: string;
  let real: string;
  try {
    logical = path.resolve(candidate);
    try {
      real = fs.realpathSync(candidate);
    } catch {
      real = logical;
    }
  } catch {
    return true;
  }
  const pathsToCheck = new Set([logical, real]);
  for (const p of [logical, real]) {
    if (p.startsWith('/private/')) pathsToCheck.add(p.slice('/private'.length));
  }
  for (const check of pathsToCheck) {
    if (check === '/') return true;
    for (const prefix of BLOCKED_PREFIXES) {
      if (check === prefix || check.startsWith(prefix + '/')) return true;
    }
    const home = os.homedir();
    for (const subdir of BLOCKED_HOME_SUBDIRS) {
      const sensitive = path.join(home, subdir);
      if (check === sensitive || check.startsWith(sensitive + '/')) return true;
    }
  }
  return false;
}

export function validateInputPathForm(p: string): boolean {
  if (!INPUT_PATH_RE.test(p)) return false;
  const segments = p.split('/');
  if (segments.some((s) => s === '' || s === '.' || s === '..')) return false;
  if (path.isAbsolute(p)) return false;
  return true;
}

export function hashFileStreamed(
  filePath: string,
  maxBytes: number,
): { ok: true; sha256: string; size: number } | { ok: false; reason: PreflightReason } {
  try {
    const st = fs.statSync(filePath);
    if (!st.isFile()) return { ok: false, reason: 'input-not-regular' };
    if (st.size > maxBytes) return { ok: false, reason: 'input-too-large' };
    const hash = crypto.createHash('sha256');
    const fd = fs.openSync(filePath, 'r');
    try {
      const buf = Buffer.alloc(64 * 1024);
      let remaining = st.size;
      while (remaining > 0) {
        const n = fs.readSync(fd, buf, 0, Math.min(buf.length, remaining), null);
        if (n <= 0) break;
        hash.update(buf.subarray(0, n));
        remaining -= n;
      }
    } finally {
      fs.closeSync(fd);
    }
    return { ok: true, sha256: hash.digest('hex'), size: st.size };
  } catch {
    return { ok: false, reason: 'input-missing' };
  }
}

export function normalizeEffectiveMembers(
  agents: AgentPersona[],
  defaultPermissionMode?: PermissionMode,
): EffectiveMember[] {
  return agents.map((agent) => {
    const engine = (agent.engine || 'claude') as EngineType;
    const permissionMode = (agent.permissionMode ?? defaultPermissionMode ?? 'bypassPermissions') as PermissionMode;
    const sandboxMode = agent.sandboxMode;
    const readOnlyEffective = sandboxMode === 'read-only' || (engine === 'claude' && permissionMode === 'plan');
    const strictRequest = sandboxMode !== undefined || permissionMode === 'plan' || permissionMode === 'dontAsk';
    return { agent, engine, permissionMode, sandboxMode, readOnlyEffective, strictRequest };
  });
}

export function needsPreflight(
  agents: AgentPersona[],
  inputs: CouncilInputSpec[] | undefined,
  defaultPermissionMode?: PermissionMode,
): boolean {
  if (inputs && inputs.length > 0) return true;
  return normalizeEffectiveMembers(agents, defaultPermissionMode).some((m) => m.strictRequest);
}

/**
 * Configuration-readiness only. Native policy is not verified.
 * Returns 'supported' | 'unsupported' | 'unknown' for the requested mode.
 */
export function classifyCapability(
  member: EffectiveMember,
  mode: CouncilRunMode,
  projectDir: string,
): 'supported' | 'unsupported' | 'unknown' {
  const { engine, permissionMode, sandboxMode, agent } = member;

  if (mode === 'read-only') {
    if (engine === 'claude') return permissionMode === 'plan' || sandboxMode === 'read-only' ? 'supported' : 'unknown';
    if (engine === 'codex') return sandboxMode === 'read-only' || permissionMode === 'plan' ? 'supported' : 'unknown';
    if (engine === 'cursor' || engine === 'agy' || engine === 'opencode') {
      return sandboxMode === 'read-only' || permissionMode === 'plan' ? 'supported' : 'unknown';
    }
    if (engine === 'grok') {
      if (sandboxMode !== 'read-only' && permissionMode !== 'plan') return 'unknown';
      if (process.platform !== 'darwin' && process.platform !== 'linux') return 'unsupported';
      if (pathUnderTempDir(projectDir)) return 'unsupported';
      return 'supported';
    }
    if (engine === 'custom') {
      const mapping = (agent.customEngine as CustomEngineConfig | undefined)?.permissionModes;
      if (mapping && typeof mapping === 'object' && ('read-only' in mapping || 'plan' in mapping)) {
        return 'supported';
      }
      return 'unsupported';
    }
    return 'unknown';
  }

  // git mode
  if (engine === 'claude') {
    if (permissionMode === 'bypassPermissions') return 'supported';
    if (permissionMode === 'plan') return 'unsupported';
    return 'unknown'; // acceptEdits, default, manual, dontAsk, auto
  }
  if (engine === 'codex') {
    // workspace-write keeps .git read-only per vendor docs — git writes unsupported
    return 'unsupported';
  }
  if (engine === 'cursor') {
    return permissionMode === 'bypassPermissions' || !permissionMode ? 'supported' : 'unknown';
  }
  if (engine === 'agy' || engine === 'grok') {
    return permissionMode === 'bypassPermissions' || !permissionMode ? 'supported' : 'unknown';
  }
  // opencode, custom, legacy
  return 'unknown';
}

export function selectMode(members: EffectiveMember[]): { mode?: CouncilRunMode; failure?: PreflightFailure } {
  const allRo = members.every((m) => m.readOnlyEffective);
  const anyStrict = members.some((m) => m.strictRequest);
  const anyRo = members.some((m) => m.readOnlyEffective);
  if (allRo) return { mode: 'read-only' };
  if (!anyStrict && !anyRo) return { mode: 'git' };
  // Mixed read-only-effective with writable members is a contract error.
  if (anyRo && !allRo) {
    return {
      failure: {
        check: 'contract',
        subject: 'mode',
        reason: 'contract-mixed',
        correction: 'sandboxMode read-only',
      },
    };
  }
  // Strict but not read-only-effective (e.g. dontAsk, plan on non-claude): git mode;
  // capability classification fails before any view or session.
  return { mode: 'git' };
}

export function validateManifest(inputs: CouncilInputSpec[] | undefined): PreflightFailure[] {
  if (!inputs) return [];
  const failures: PreflightFailure[] = [];
  if (inputs.length > MAX_INPUTS) {
    failures.push({
      check: 'manifest',
      subject: 'inputs',
      reason: 'input-invalid',
    });
    return failures;
  }
  inputs.forEach((entry, index) => {
    const subject = `inputs[${index}]`;
    if (!validateInputPathForm(entry.path)) {
      failures.push({ check: 'manifest', subject, reason: 'input-invalid' });
      return;
    }
    if (!SHA256_RE.test(entry.sha256)) {
      failures.push({ check: 'manifest', subject, reason: 'input-invalid' });
      return;
    }
    if (entry.source !== undefined) {
      if (typeof entry.source !== 'string' || !path.isAbsolute(entry.source) || isBlockedLocation(entry.source)) {
        failures.push({ check: 'manifest', subject, reason: 'input-blocked-source' });
      }
    }
  });
  return failures;
}

export async function captureHead(cwd: string): Promise<string | undefined> {
  try {
    const r = await spawnAsync('git', ['-C', cwd, 'rev-parse', 'HEAD'], { timeout: GIT_CMD_TIMEOUT_MS });
    const sha = r.stdout.trim();
    return /^[0-9a-f]{7,40}$/i.test(sha) ? sha : undefined;
  } catch {
    return undefined;
  }
}

export async function createReadOnlyView(
  projectDir: string,
  baseSha: string,
  runId: string,
  agentName: string,
): Promise<{ ok: true; view: string } | { ok: false; reason: PreflightReason }> {
  const prefix = runId.replace(/[^a-zA-Z0-9]/g, '').slice(0, 8) || 'run';
  const view = path.join(projectDir, '.worktrees', `ro-${prefix}-${agentName}`);
  if (fs.existsSync(view)) {
    return { ok: false, reason: 'target-exists' };
  }
  fs.mkdirSync(path.dirname(view), { recursive: true });
  try {
    await spawnAsync('git', ['-C', projectDir, 'worktree', 'add', '--detach', view, baseSha], {
      timeout: WORKTREE_CMD_TIMEOUT_MS,
    });
    return { ok: true, view };
  } catch {
    return { ok: false, reason: 'base-unreadable' };
  }
}

export function deliverInputToView(
  view: string,
  entry: CouncilInputSpec,
  boundBaseDir: string,
): PreflightFailure | null {
  const dest = path.join(view, entry.path);
  const destRealParent = path.dirname(dest);
  try {
    fs.mkdirSync(destRealParent, { recursive: true });
  } catch {
    return { check: 'input', subject: entry.path, reason: 'input-missing' };
  }

  let sourcePath: string;
  if (entry.source) {
    if (isBlockedLocation(entry.source)) {
      return { check: 'input', subject: 'source', reason: 'input-blocked-source' };
    }
    try {
      const st = fs.statSync(entry.source);
      if (!st.isFile()) return { check: 'input', subject: 'source', reason: 'input-not-regular' };
    } catch {
      return { check: 'input', subject: 'source', reason: 'input-missing' };
    }
    sourcePath = entry.source;
  } else {
    sourcePath = path.join(boundBaseDir, entry.path);
  }

  const hashed = hashFileStreamed(sourcePath, MAX_INPUT_BYTES);
  if (!hashed.ok) return { check: 'input', subject: entry.path, reason: hashed.reason };
  if (hashed.sha256 !== entry.sha256) {
    return { check: 'input', subject: entry.path, reason: 'input-hash-mismatch' };
  }

  // Tracked at base with other bytes: refuse when source omitted and base file differs
  if (!entry.source) {
    const baseFile = path.join(boundBaseDir, entry.path);
    if (fs.existsSync(baseFile)) {
      const baseHash = hashFileStreamed(baseFile, MAX_INPUT_BYTES);
      if (baseHash.ok && baseHash.sha256 !== entry.sha256) {
        return { check: 'input', subject: entry.path, reason: 'input-hash-mismatch' };
      }
    }
  }

  try {
    fs.copyFileSync(sourcePath, dest);
    // Supplementary write-bit removal — never claimed as enforcement
    try {
      const st = fs.statSync(dest);
      fs.chmodSync(dest, st.mode & ~0o222);
    } catch {
      /* ignore */
    }
  } catch {
    return { check: 'input', subject: entry.path, reason: 'input-missing' };
  }

  // Verify inside view
  let realDest: string;
  try {
    realDest = fs.realpathSync(dest);
    const realView = fs.realpathSync(view);
    if (realDest !== realView && !realDest.startsWith(realView + path.sep)) {
      return { check: 'input', subject: entry.path, reason: 'input-outside-view' };
    }
  } catch {
    return { check: 'input', subject: entry.path, reason: 'input-missing' };
  }
  const again = hashFileStreamed(realDest, MAX_INPUT_BYTES);
  if (!again.ok || again.sha256 !== entry.sha256) {
    return { check: 'input', subject: entry.path, reason: 'input-hash-mismatch' };
  }
  return null;
}

export function verifyInputsInView(view: string, inputs: CouncilInputSpec[]): PreflightFailure[] {
  const failures: PreflightFailure[] = [];
  for (const entry of inputs) {
    const dest = path.join(view, entry.path);
    try {
      const realDest = fs.realpathSync(dest);
      const realView = fs.realpathSync(view);
      if (realDest !== realView && !realDest.startsWith(realView + path.sep)) {
        failures.push({ check: 'input', subject: entry.path, reason: 'input-outside-view' });
        continue;
      }
      const hashed = hashFileStreamed(realDest, MAX_INPUT_BYTES);
      if (!hashed.ok) {
        failures.push({ check: 'input', subject: entry.path, reason: hashed.reason });
      } else if (hashed.sha256 !== entry.sha256) {
        failures.push({ check: 'input', subject: entry.path, reason: 'input-hash-mismatch' });
      }
    } catch {
      failures.push({ check: 'input', subject: entry.path, reason: 'input-missing' });
    }
  }
  return failures;
}

export async function removeViewIfUnchanged(
  view: string,
  fingerprintBefore: string | undefined,
): Promise<'removed' | 'kept'> {
  if (!fingerprintBefore || !fs.existsSync(view)) return 'kept';
  const { treeFingerprint } = await import('./verify/baseline.js');
  const after = await treeFingerprint(view);
  if (after && after === fingerprintBefore) {
    // Exact path remove via git worktree when registered; else rmdir of the owned view
    try {
      const parent = path.dirname(view);
      // Prefer git worktree remove from the main repo
      const projectDir = path.resolve(view, '..', '..');
      await spawnAsync('git', ['-C', projectDir, 'worktree', 'remove', '--force', view], {
        timeout: WORKTREE_CMD_TIMEOUT_MS,
      }).catch(() => {
        fs.rmSync(view, { recursive: true, force: true });
      });
      void parent;
      return 'removed';
    } catch {
      return 'kept';
    }
  }
  return 'kept';
}

export const READ_ONLY_CHARTER = `# Council charter (read-only)

You are a read-only council member. You may inspect files in your assigned view and report findings.
Do not modify the tree, invoke mutating tooling, or perform version-control writes.
Native engine policy is separate from this charter and is not proven by this text.
`;

export function buildReadOnlyPrompt(agent: AgentPersona, task: string, round: number): string {
  return `${READ_ONLY_CHARTER}

# Round ${round} — Read-only review

## Task
${task}

## Your role
${agent.persona}

Produce a written assessment only. End with [CONSENSUS: YES] or [CONSENSUS: NO].
`;
}

export const UNSUPPORTED_OPERATION_REASON = 'read-only-council' as const;
