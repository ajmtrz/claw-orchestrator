#!/usr/bin/env node
import fs from 'node:fs';
import process from 'node:process';
import path from 'node:path';

const config = JSON.parse(fs.readFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG, 'utf8'));
const argv = process.argv.slice(2);
const { trustNativeResponse } = await import(config.project + '/src/__tests__/helpers/autoloop-trust-recovery.ts');
const id = 'a126a126-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const emit = (event) => {
  fs.appendFileSync(path.join(config.directory, 'protocol.jsonl'), JSON.stringify({ pid: process.pid, event }) + '\n');
  process.stdout.write(JSON.stringify(event) + '\n');
};
fs.appendFileSync(
  path.join(config.directory, 'native.jsonl'),
  JSON.stringify({ engine: 'agy', argv, pid: process.pid }) + '\n',
);
const reply = trustNativeResponse(config, argv[argv.indexOf('-p') + 1]);
emit({ event: 'init', conversation_id: id });
if (config.mode === 'protocol') process.stdout.write('{broken-json\n');
else if (config.mode === 'partial') process.stdout.write(reply + '\n');
else
  emit({
    event: 'result',
    result: {
      conversation_id: id,
      status: config.mode === 'denied' || config.mode === 'denied-empty' ? 'STOPPED' : 'SUCCESS',
      response: config.mode === 'empty' || config.mode === 'denied-empty' ? '' : reply,
      usage: { input_tokens: 1, output_tokens: 1, cache_read_tokens: 0 },
    },
  });
if (config.mode === 'process') process.exitCode = 23;
