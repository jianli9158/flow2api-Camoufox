import json
import shutil
import subprocess
import types
import unittest
from unittest.mock import AsyncMock

from src.services.recaptcha_runtime import (
    TRUSTED_RECAPTCHA_HOOK_SOURCE,
    build_recaptcha_execute_script,
)


class LocalCaptchaActionTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(
        shutil.which("node"), "Node.js is required for the JavaScript contract"
    )
    def test_shared_execute_script_preserves_requested_action(self):
        source = r"""
const vm = require('vm');
const input = JSON.parse(require('fs').readFileSync(0, 'utf8'));
(async () => {
  const sandbox = { setTimeout, clearTimeout, location: { href: 'https://flow.google.com/project/example' } };
  sandbox.window = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(input.hook, sandbox);
  vm.runInContext(`
    globalThis.calls = [];
    globalThis.grecaptcha = { enterprise: {
      execute: function(key, options) { calls.push({ key, action: options.action }); return Promise.resolve('captcha'); }
    }};
    const original = grecaptcha.enterprise.execute.bind(grecaptcha.enterprise);
    grecaptcha.enterprise.execute = function(key, options) {
      return original(key, Object.assign({}, options, { action: 'extension_hijack_detected' }));
    };
  `, sandbox);
  for (const script of input.scripts) {
    const result = await vm.runInContext(script, sandbox);
    if (!result.ok || result.token !== 'captcha') throw Error('captcha execution failed');
  }
  const actual = sandbox.calls.map(call => call.action);
  if (JSON.stringify(actual) !== JSON.stringify(input.actions)) throw Error('action mismatch');
  if (sandbox.calls.some(call => call.key !== input.siteKey)) throw Error('site key mismatch');
})().catch(error => { process.stderr.write(error.message); process.exitCode = 1; });
"""
        actions = [
            "IMAGE_GENERATION",
            "VIDEO_GENERATION",
            "UPLOAD_IMAGE",
            "CHAT_GENERATION",
        ]
        site_key = 'site-key"\\value'
        result = subprocess.run(
            [shutil.which("node"), "-e", source],
            input=json.dumps(
                {
                    "hook": TRUSTED_RECAPTCHA_HOOK_SOURCE,
                    "scripts": [
                        build_recaptcha_execute_script(site_key, action, 1000)
                        for action in actions
                    ],
                    "actions": actions,
                    "siteKey": site_key,
                }
            ),
            text=True,
            capture_output=True,
            timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
