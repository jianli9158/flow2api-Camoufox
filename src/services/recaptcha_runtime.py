"""Shared reCAPTCHA execution helpers for local browser providers."""

import json


TRUSTED_RECAPTCHA_HOOK_SOURCE = r"""
(() => {
    if (window.__flow2apiTrustedRecaptchaHookInstalled) return;
    Object.defineProperty(
        window,
        '__flow2apiTrustedRecaptchaHookInstalled',
        { value: true }
    );

    const storageKey = '__flow2apiTrustedRecaptchaExecute';
    const originalBind = Function.prototype.bind;
    Function.prototype.bind = function(thisArg, ...args) {
        const bound = originalBind.call(this, thisArg, ...args);
        try {
            const enterprise = window.grecaptcha && window.grecaptcha.enterprise;
            if (
                !window[storageKey] &&
                enterprise &&
                this === enterprise.execute &&
                typeof bound === 'function'
            ) {
                window[storageKey] = bound;
            }
        } catch (error) {}
        return bound;
    };

    // New Flow replaces the public execute wrapper with an assignment that
    // overwrites the caller action by extension_hijack_detected. Preserve the
    // requested action when that exact assignment shape is used.
    Object.defineProperty(
        window,
        '__flow2apiRecaptchaActionGuardInstalled',
        { value: true }
    );
    const originalAssign = Object.assign;
    Object.assign = function(target, ...sources) {
        let nextSources = sources;
        if (sources.length >= 2) {
            const previous = sources[sources.length - 2];
            const last = sources[sources.length - 1];
            if (
                previous &&
                typeof previous.action === 'string' &&
                previous.action &&
                last &&
                last.action === 'extension_hijack_detected'
            ) {
                nextSources = sources.slice(0, -1);
            }
        }
        return originalAssign.call(this, target, ...nextSources);
    };
})();
"""

RECAPTCHA_EXECUTE_SOURCE = r"""(async (siteKey, requestedAction, timeoutMs) => {
            const storageKey = '__flow2apiTrustedRecaptchaExecute';
            for (let attempt = 0; attempt < 40; attempt += 1) {
                if (
                    window[storageKey] ||
                    (window.grecaptcha &&
                        window.grecaptcha.enterprise &&
                        typeof window.grecaptcha.enterprise.execute === 'function')
                ) {
                    break;
                }
                await new Promise(resolve => setTimeout(resolve, 250));
            }

            if (
                !window[storageKey] &&
                window.grecaptcha &&
                window.grecaptcha.enterprise &&
                typeof window.grecaptcha.enterprise.ready === 'function'
            ) {
                try {
                    await new Promise((resolve) => {
                        const timer = setTimeout(resolve, 2500);
                        window.grecaptcha.enterprise.ready(() => {
                            clearTimeout(timer);
                            setTimeout(resolve, 60);
                        });
                    });
                } catch (error) {}
            }

            const capturedExecute = !!window[storageKey];
            let execute = window[storageKey];
            let executeSource = '';
            try {
                executeSource = String(execute || '');
            } catch (error) {}

            if (!execute) {
                const enterprise = window.grecaptcha && window.grecaptcha.enterprise;
                execute = enterprise && enterprise.execute;
            }

            if (typeof execute !== 'function') {
                return { ok: false, error: 'reCAPTCHA execute is unavailable' };
            }

            const runExecute = (target) => new Promise((resolve, reject) => {
                let settled = false;
                const finish = (handler, value) => {
                    if (settled) return;
                    settled = true;
                    handler(value);
                };
                const timer = setTimeout(() => {
                    finish(reject, new Error('execute timeout'));
                }, timeoutMs);
                const originalAssign = Object.assign;
                Object.assign = function(assignTarget, ...sources) {
                    let nextSources = sources;
                    if (sources.length >= 2) {
                        const previous = sources[sources.length - 2];
                        const last = sources[sources.length - 1];
                        if (
                            previous &&
                            typeof previous.action === 'string' &&
                            previous.action &&
                            last &&
                            last.action === 'extension_hijack_detected'
                        ) {
                            nextSources = sources.slice(0, -1);
                        }
                    }
                    return originalAssign.call(this, assignTarget, ...nextSources);
                };
                try {
                    Promise.resolve(execute(target, { action: requestedAction }))
                        .then(value => {
                            Object.assign = originalAssign;
                            clearTimeout(timer);
                            finish(resolve, value);
                        })
                        .catch(error => {
                            Object.assign = originalAssign;
                            clearTimeout(timer);
                            finish(reject, error);
                        });
                } catch (error) {
                    Object.assign = originalAssign;
                    clearTimeout(timer);
                    finish(reject, error);
                }
            });

            let executeTarget = window.__flow2apiRecaptchaClientId !== undefined
                ? window.__flow2apiRecaptchaClientId
                : siteKey;

            try {
                const token = await runExecute(executeTarget);
                return { ok: true, token };
            } catch (firstError) {
                // Fallback: if siteKey was not auto-rendered, render into an empty detached/hidden div
                try {
                    if (
                        !capturedExecute &&
                        window.grecaptcha &&
                        window.grecaptcha.enterprise &&
                        typeof window.grecaptcha.enterprise.render === 'function' &&
                        window.__flow2apiRecaptchaClientId === undefined
                    ) {
                        let container = null;
                        if (typeof document !== 'undefined' && document && typeof document.createElement === 'function') {
                            container = document.createElement('div');
                            container.style.display = 'none';
                            if (document.body) {
                                document.body.appendChild(container);
                            }
                        } else if (typeof document !== 'undefined' && document && document.documentElement) {
                            container = document.documentElement;
                        }
                        if (container) {
                            window.__flow2apiRecaptchaClientId =
                                window.grecaptcha.enterprise.render(
                                    container,
                                    { sitekey: siteKey, action: requestedAction, size: 'invisible' }
                                );
                            executeTarget = window.__flow2apiRecaptchaClientId;
                            const token = await runExecute(executeTarget);
                            return { ok: true, token };
                        }
                    }
                } catch (_) {}

                const error = firstError;
                const debug = {
                    captured: capturedExecute,
                    actionGuard: !!window.__flow2apiRecaptchaActionGuardInstalled,
                    renderType: window.grecaptcha && window.grecaptcha.enterprise
                        ? typeof window.grecaptcha.enterprise.render
                        : 'missing',
                    clientId: String(window.__flow2apiRecaptchaClientId),
                    executeTarget: String(executeTarget),
                    url: location.href
                };
                return {
                    ok: false,
                    error: (error && error.message ? error.message : String(error))
                        + ' | ' + JSON.stringify(debug)
                };
            }
        })(__SITE_KEY__, __REQUESTED_ACTION__, __TIMEOUT_MS__)"""


def build_recaptcha_execute_script(site_key: str, action: str, timeout_ms: int) -> str:
    return (
        RECAPTCHA_EXECUTE_SOURCE
        .replace("__SITE_KEY__", json.dumps(site_key))
        .replace("__REQUESTED_ACTION__", json.dumps(action))
        .replace("__TIMEOUT_MS__", json.dumps(timeout_ms))
    )
