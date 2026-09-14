#!/usr/bin/env node
import fs from 'node:fs';
import process from 'node:process';
import path from 'node:path';

const config = JSON.parse(fs.readFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG, 'utf8'));
const argv = process.argv.slice(2);
const { trustNativeResponse } = await import(config.project + '/src/__tests__/helpers/autoloop-trust-recovery.ts');
const id = 'b126a126-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const emit = (event) => {
  fs.appendFileSync(path.join(config.directory, 'protocol.jsonl'), JSON.stringify({ pid: process.pid, event }) + '\n');
  process.stdout.write(JSON.stringify(event) + '\n');
};
const permissionFile = path.join(process.cwd(), '.cursor/cli.json');
fs.appendFileSync(
  path.join(config.directory, 'native.jsonl'),
  JSON.stringify({
    engine: 'cursor',
    argv,
    pid: process.pid,
    cwd: process.cwd(),
    permission_config: fs.existsSync(permissionFile) ? JSON.parse(fs.readFileSync(permissionFile, 'utf8')) : null,
  }) + '\n',
);
const reply = trustNativeResponse(config, argv[argv.indexOf('-p') + 1]);
emit({ type: 'system', subtype: 'init', session_id: id });
if (config.mode === 'protocol') process.stdout.write('{broken-json\n');
else {
  const text = config.mode === 'empty' ? '' : reply;
  emit({ type: 'assistant', message: { role: 'assistant', content: [{ type: 'text', text }] } });
  if (config.mode !== 'partial')
    emit({
      type: 'result',
      subtype: config.mode === 'denied' ? 'error_during_execution' : 'success',
      is_error: config.mode === 'denied',
      result: text,
    });
}
if (config.mode === 'process') process.exitCode = 23;
