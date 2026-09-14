#!/usr/bin/env node
import fs from 'node:fs';
import process from 'node:process';
import path from 'node:path';

const config = JSON.parse(fs.readFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG, 'utf8'));
const argv = process.argv.slice(2);
const prompt = argv.at(-1);
const { trustNativeResponse } = await import(config.project + '/src/__tests__/helpers/autoloop-trust-recovery.ts');
const id = '019c6dcb-93ad-7dc1-b531-418d213b8761';
fs.appendFileSync(
  path.join(config.directory, 'native.jsonl'),
  JSON.stringify({ engine: 'codex', argv, prompt, pid: process.pid }) + '\n',
);
const reply = trustNativeResponse(config, prompt);
const emit = (event) => {
  fs.appendFileSync(path.join(config.directory, 'protocol.jsonl'), JSON.stringify({ pid: process.pid, event }) + '\n');
  process.stdout.write(JSON.stringify(event) + '\n');
};
emit({ type: 'thread.started', thread_id: id });
if (config.mode === 'protocol') {
  process.stdout.write('{broken-json\n');
} else {
  emit({ type: 'item.completed', item: { type: 'agent_message', text: config.mode === 'empty' ? '' : reply } });
  if (config.mode === 'denied') emit({ type: 'turn.failed', error: { message: 'required tool permission denied' } });
  else if (config.mode !== 'partial') emit({ type: 'turn.completed', usage: { input_tokens: 1, output_tokens: 1 } });
}
if (config.mode === 'process') process.exitCode = 23;
