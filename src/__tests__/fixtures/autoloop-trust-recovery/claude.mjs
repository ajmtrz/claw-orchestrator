#!/usr/bin/env node
import fs from 'node:fs';
import process from 'node:process';
import path from 'node:path';
import readline from 'node:readline';

const argv = process.argv.slice(2);
const startup = JSON.parse(fs.readFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG, 'utf8'));
const id = '9a16a126-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const emit = (event) => {
  fs.appendFileSync(path.join(startup.directory, 'protocol.jsonl'), JSON.stringify({ pid: process.pid, event }) + '\n');
  process.stdout.write(JSON.stringify(event) + '\n');
};
emit({ type: 'system', subtype: 'init', session_id: id, tools: [], model: 'claude-haiku-4-5' });
for await (const line of readline.createInterface({ input: process.stdin })) {
  const config = JSON.parse(fs.readFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG, 'utf8'));
  fs.appendFileSync(
    path.join(config.directory, 'native.jsonl'),
    JSON.stringify({ engine: 'claude', argv, stdin: line, pid: process.pid }) + '\n',
  );
  emit(JSON.parse(line));
  const { trustNativeResponse } = await import(config.project + '/src/__tests__/helpers/autoloop-trust-recovery.ts');
  const incoming = JSON.parse(line);
  const prompt =
    typeof incoming.message.content === 'string'
      ? incoming.message.content
      : incoming.message.content.map((block) => block.text ?? '').join('');
  const text = config.mode === 'empty' ? '' : trustNativeResponse(config, prompt);
  if (config.mode === 'protocol') {
    process.stdout.write('{broken-json\n');
    process.exit(0);
  }
  emit({ type: 'assistant', message: { role: 'assistant', content: [{ type: 'text', text }] } });
  if (config.mode === 'partial' || config.mode === 'process') process.exit(config.mode === 'process' ? 23 : 0);
  emit({
    type: 'result',
    subtype: config.mode === 'denied' ? 'error_during_execution' : 'success',
    is_error: config.mode === 'denied',
    result: text,
    session_id: id,
    ...(config.mode === 'denied' ? { permission_denials: [{ tool_name: 'Read' }] } : {}),
  });
}
