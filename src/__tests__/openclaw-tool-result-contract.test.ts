import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import plugin from '../index.js';
import { SessionManager } from '../session-manager.js';

interface RegisteredTool {
  name: string;
  execute: (toolCallId: string, params: Record<string, unknown>) => Promise<unknown>;
}

interface AgentToolResult {
  content: Array<{ type: 'text'; text: string }>;
  details: unknown;
}

function collectTools(): { tools: Map<string, RegisteredTool>; registeredNames: string[]; stop: () => void } {
  const tools = new Map<string, RegisteredTool>();
  const registeredNames: string[] = [];
  let stop = () => {};
  const fakeApi = {
    pluginConfig: {},
    logger: { info: () => {}, error: () => {}, warn: () => {} },
    registerTool: (definition: RegisteredTool) => {
      registeredNames.push(definition.name);
      tools.set(definition.name, definition);
    },
    on: () => {},
    registerHttpRoute: () => {},
    registerService: (service: { stop: () => void }) => {
      stop = service.stop;
    },
  };

  (plugin as unknown as { register: (api: unknown) => void }).register(fakeApi);
  return { tools, registeredNames, stop: () => stop() };
}

describe('OpenClaw tool result contract', () => {
  const registration = collectTools();
  const codexModels = vi.spyOn(SessionManager.prototype, 'codexModels');

  beforeAll(() => {
    vi.stubEnv('CLAWO_NO_EMBEDDED_SERVER', '1');
  });

  afterAll(() => {
    registration.stop();
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it('wraps a plain handler payload in content and details', async () => {
    const payload = { ok: true as const, models: [{ id: 'gpt-test' }] };
    codexModels.mockResolvedValueOnce(payload);

    const result = (await registration.tools.get('codex_models')!.execute('call-1', {
      name: 'session-1',
    })) as AgentToolResult;

    expect(result).toEqual({
      content: [
        { type: 'text', text: '{\n  "ok": true,\n  "models": [\n    {\n      "id": "gpt-test"\n    }\n  ]\n}' },
      ],
      details: payload,
    });
    expect(result.details).toBe(payload);
  });

  it('serializes BigInt values in text while preserving the details payload', async () => {
    const payload = { ok: true as const, models: [{ contextWindow: 128_000n }] };
    codexModels.mockResolvedValueOnce(payload);

    const result = (await registration.tools.get('codex_models')!.execute('call-2', {
      name: 'session-1',
    })) as AgentToolResult;

    expect(result.content).toEqual([
      {
        type: 'text',
        text: '{\n  "ok": true,\n  "models": [\n    {\n      "contextWindow": "128000"\n    }\n  ]\n}',
      },
    ]);
    expect(result.details).toBe(payload);
  });

  it('preserves an existing AgentToolResult object by identity', async () => {
    const existing: AgentToolResult = {
      content: [{ type: 'text', text: 'already wrapped' }],
      details: { ok: true },
    };
    codexModels.mockResolvedValueOnce(existing as never);

    const result = await registration.tools.get('codex_models')!.execute('call-3', {
      name: 'session-1',
    });

    expect(result).toBe(existing);
  });

  it('propagates the original handler exception', async () => {
    const error = new Error('codex app-server unavailable');
    codexModels.mockRejectedValueOnce(error);

    const promise = registration.tools.get('codex_models')!.execute('call-4', {
      name: 'session-1',
    });

    await expect(promise).rejects.toBe(error);
  });

  it('keeps the 78 upstream tools and adds request_review and recover in exact manifest parity', () => {
    const task4bTools = ['autoloop_request_review', 'autoloop_recover'];
    const internalOnlyTools = ['autoloop_spawn_coder', 'autoloop_spawn_reviewer'];
    const legacyTools = [
      'session_start',
      'session_send',
      'session_handoff',
      'session_stop',
      'session_list',
      'sessions_overview',
      'coding_session_status',
      'session_grep',
      'session_compact',
      'coding_agents_list',
      'team_list',
      'team_send',
      'session_update_tools',
      'session_switch_model',
      'project_purge',
      'plugin_details',
      'claude_goal_set',
      'claude_goal_clear',
      'claude_goal_status',
      'codex_resume',
      'codex_review',
      'codex_goal_set',
      'codex_goal_get',
      'codex_goal_pause',
      'codex_goal_resume',
      'codex_goal_clear',
      'codex_interrupt',
      'codex_steer',
      'codex_fork',
      'codex_rollback',
      'codex_models',
      'codex_thread_list',
      'claude_agents_list',
      'fanout_start',
      'fanout_status',
      'fanout_abort',
      'workflow_start',
      'workflow_status',
      'workflow_list',
      'workflow_resume',
      'workflow_cancel',
      'workflow_steer',
      'workflow_approve',
      'verify_run',
      'council_start',
      'council_status',
      'council_abort',
      'council_inject',
      'council_review',
      'council_accept',
      'council_reject',
      'autoloop_start',
      'autoloop_chat',
      'autoloop_status',
      'autoloop_list',
      'autoloop_reset_agent',
      'autoloop_stop',
      'session_send_to',
      'session_inbox',
      'session_deliver_inbox',
      'ultraplan_start',
      'ultraplan_status',
      'ultrareview_start',
      'ultrareview_status',
      'ultraapp_list',
      'ultraapp_get',
      'ultraapp_status',
      'ultraapp_new',
      'ultraapp_answer',
      'ultraapp_add_file',
      'ultraapp_spec_edit',
      'ultraapp_build_start',
      'ultraapp_build_cancel',
      'ultraapp_feedback',
      'ultraapp_promote_version',
      'ultraapp_start_container',
      'ultraapp_stop_container',
      'ultraapp_delete',
    ];
    const manifest = JSON.parse(
      readFileSync(join(dirname(fileURLToPath(import.meta.url)), '../../openclaw.plugin.json'), 'utf8'),
    ) as { contracts: { tools: string[] } };

    expect(registration.registeredNames).toHaveLength(80);
    expect(new Set(registration.registeredNames)).toHaveLength(80);
    expect(manifest.contracts.tools).toHaveLength(80);
    expect(new Set(manifest.contracts.tools)).toHaveLength(80);
    expect([...manifest.contracts.tools].sort()).toEqual([...registration.registeredNames].sort());
    expect(registration.registeredNames.filter((name) => !task4bTools.includes(name)).sort()).toEqual(
      [...legacyTools].sort(),
    );
    for (const name of task4bTools) {
      expect(registration.registeredNames.filter((candidate) => candidate === name)).toHaveLength(1);
      expect(manifest.contracts.tools.filter((candidate) => candidate === name)).toHaveLength(1);
    }
    for (const name of internalOnlyTools) {
      expect(registration.registeredNames).not.toContain(name);
      expect(manifest.contracts.tools).not.toContain(name);
    }
    expect(registration.tools.has('codex_thread_list')).toBe(true);
    expect(registration.tools.has('codex_threads')).toBe(false);
  });
});
