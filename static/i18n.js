/* Shared localization for the Flow2API console. */
(() => {
    'use strict';

    const storageKey = 'flow2api.language';
    const supported = ['zh-CN', 'en'];
    const messages = { 'zh-CN': Object.create(null), en: Object.create(null) };
    const errorMessages = { 'zh-CN': Object.create(null), en: Object.create(null) };
    const bindings = new WeakMap();
    const messageBindings = new WeakMap();
    const errorBindings = new WeakMap();
    const aliases = new Map();
    const errorAliases = new Map();
    let patterns = [];
    let errorPatterns = [];

    function normalize(language) {
        const value = String(language || '').toLowerCase();
        if (/^zh(?:-|$)/.test(value)) return 'zh-CN';
        if (/^en(?:-|$)/.test(value)) return 'en';
        return null;
    }

    function preferredLanguage() {
        try {
            const saved = normalize(localStorage.getItem(storageKey));
            if (saved) return saved;
        } catch (_) { /* Storage can be disabled by the browser. */ }
        for (const language of navigator.languages || [navigator.language]) {
            const match = normalize(language);
            if (match) return match;
        }
        return 'zh-CN';
    }

    let locale = preferredLanguage();
    document.documentElement.lang = locale;

    function interpolate(message, params = {}) {
        return String(message).replace(/\{(\w+)\}/g, (match, key) =>
            Object.prototype.hasOwnProperty.call(params, key) ? String(params[key]) : match);
    }

    function t(source, params = {}) {
        const text = String(source ?? '');
        const translated = messages[locale][text] ?? messages['zh-CN'][text] ?? text;
        return interpolate(translated, params);
    }

    function indexMessages(catalog, aliasMap) {
        const variants = [];
        for (const source of Object.keys(catalog['zh-CN'])) {
            for (const text of new Set([source, catalog['zh-CN'][source], catalog.en[source]])) {
                if (typeof text !== 'string') continue;
                if (/\{\w+\}/.test(text)) variants.push({ source, text });
                else if (!aliasMap.has(text)) aliasMap.set(text, source);
            }
        }
        return variants.map(({ source, text }) => {
            const names = [];
            let position = 0;
            let expression = '^';
            const escape = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            for (const match of text.matchAll(/\{(\w+)\}/g)) {
                expression += escape(text.slice(position, match.index)) + '(.*?)';
                names.push(match[1]);
                position = match.index + match[0].length;
            }
            expression += escape(text.slice(position)) + '$';
            return { source, names, specificity: text.replace(/\{\w+\}/g, '').length, regex: new RegExp(expression, 's') };
        }).sort((a, b) => b.specificity - a.specificity
            || Number(b.names.includes('error')) - Number(a.names.includes('error')));
    }

    function translateKnown(message, catalog, aliasMap, matchers, depth = 0) {
        const text = String(message ?? '');
        const translate = (source, params = {}) => interpolate(catalog[locale][source] ?? catalog['zh-CN'][source] ?? source, params);
        if (Object.prototype.hasOwnProperty.call(catalog[locale], text)) return translate(text);
        if (aliasMap.has(text)) return translate(aliasMap.get(text));
        if (text.length > 8192) return text;
        for (const { source, regex, names } of matchers) {
            const match = regex.exec(text);
            if (!match) continue;
            const params = Object.create(null);
            names.forEach((name, index) => {
                const value = match[index + 1];
                params[name] = name === 'error' && depth < 4
                    ? translateKnown(value, errorMessages, errorAliases, errorPatterns, depth + 1)
                    : value;
            });
            return translate(source, params);
        }
        return text;
    }

    function translateMessage(message) {
        return translateKnown(message, messages, aliases, patterns);
    }

    function translateError(message) {
        return translateKnown(message, errorMessages, errorAliases, errorPatterns);
    }

    function resolveElement(element) {
        return typeof element === 'string' ? document.getElementById(element) : element;
    }

    function setText(element, source, params = {}) {
        const target = resolveElement(element);
        if (!target) return;
        target.dataset.i18n = source;
        delete target.dataset.i18nMessage;
        delete target.dataset.i18nError;
        messageBindings.delete(target);
        errorBindings.delete(target);
        bindings.set(target, params);
        const text = t(source, params);
        if (target.textContent !== text) target.textContent = text;
    }

    function setRawText(element, value) {
        const target = resolveElement(element);
        if (!target) return value;
        delete target.dataset.i18n;
        delete target.dataset.i18nMessage;
        delete target.dataset.i18nError;
        bindings.delete(target);
        messageBindings.delete(target);
        errorBindings.delete(target);
        target.textContent = value;
        return value;
    }

    function setMessage(element, message) {
        const target = resolveElement(element);
        if (!target) return message;
        delete target.dataset.i18n;
        delete target.dataset.i18nError;
        bindings.delete(target);
        errorBindings.delete(target);
        target.dataset.i18nMessage = '';
        messageBindings.set(target, String(message ?? ''));
        target.textContent = translateMessage(message);
        return message;
    }

    const attributes = ['placeholder', 'title', 'aria-label'];
    const selector = '[data-i18n], [data-i18n-message], [data-i18n-error], ' + attributes.map(name => `[data-i18n-${name}]`).join(', ');

    function apply(root = document) {
        const elements = [];
        if (root.nodeType === Node.ELEMENT_NODE && root.matches(selector)) elements.push(root);
        if (root.querySelectorAll) elements.push(...root.querySelectorAll(selector));
        for (const element of elements) {
            if (element.hasAttribute('data-i18n')) {
                const text = t(element.dataset.i18n, bindings.get(element) || {});
                if (element.textContent !== text) element.textContent = text;
            }
            if (element.hasAttribute('data-i18n-message') && !messageBindings.has(element)) {
                messageBindings.set(element, element.textContent);
            }
            if (messageBindings.has(element)) {
                const text = translateMessage(messageBindings.get(element));
                if (element.textContent !== text) element.textContent = text;
            }
            if (element.hasAttribute('data-i18n-error')) {
                if (!errorBindings.has(element)) errorBindings.set(element, element.textContent);
                const text = translateError(errorBindings.get(element));
                if (element.textContent !== text) element.textContent = text;
            }
            for (const attribute of attributes) {
                const source = element.getAttribute(`data-i18n-${attribute}`);
                if (source !== null) element.setAttribute(attribute, t(source));
            }
        }
    }

    function updateSelectors() {
        document.querySelectorAll('[data-language-select], [data-language-mirror]').forEach(select => {
            select.value = locale;
            select.setAttribute('aria-label', t('界面语言'));
        });
    }

    function setLanguage(language, persist = true) {
        const next = normalize(language);
        if (!next) return;
        const changed = locale !== next;
        locale = next;
        document.documentElement.lang = locale;
        if (persist) {
            try { localStorage.setItem(storageKey, locale); } catch (_) { /* Optional storage. */ }
        }
        apply();
        updateSelectors();
        if (changed) window.dispatchEvent(new CustomEvent('languagechange', { detail: { locale } }));
    }

    const domReady = new Promise(resolve => {
        if (document.readyState !== 'loading') resolve();
        else document.addEventListener('DOMContentLoaded', resolve, { once: true });
    });
    const page = document.documentElement.dataset.i18nPage;
    const catalogs = ['common', 'backend'];
    if (['login', 'manage', 'test'].includes(page)) catalogs.push(page);

    const catalogReady = Promise.all(catalogs.map(async name => {
        try {
            const response = await fetch(`/static/locales/${name}.json`, { cache: 'no-cache' });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return await response.json();
        } catch (error) {
            console.error(`Unable to load ${name} translations:`, error);
            return {};
        }
    })).then(data => {
        for (const [index, catalog] of data.entries()) {
            for (const language of supported) {
                Object.assign(messages[language], catalog[language] || {});
                if (catalogs[index] === 'backend') Object.assign(errorMessages[language], catalog[language] || {});
            }
        }
        patterns = indexMessages(messages, aliases);
        errorPatterns = indexMessages(errorMessages, errorAliases);
    });

    const ready = Promise.all([catalogReady, domReady]).then(() => {
        apply();
        updateSelectors();
        document.addEventListener('change', event => {
            if (event.target.matches('[data-language-select], [data-language-mirror]')) setLanguage(event.target.value);
        });
        const observer = new MutationObserver(records => {
            for (const record of records) {
                for (const node of record.addedNodes) {
                    if (node.nodeType === Node.ELEMENT_NODE) apply(node);
                }
            }
        });
        observer.observe(document.body, { childList: true, subtree: true });
    });

    const themeKey = 'flow2api.theme';
    function normalizeTheme(value) {
        return value === 'dark' ? 'dark' : 'light';
    }
    function preferredTheme() {
        try {
            const saved = localStorage.getItem(themeKey);
            if (saved === 'dark' || saved === 'light') return saved;
        } catch (_) { /* Storage can be disabled by the browser. */ }
        return 'light';
    }
    let currentTheme = preferredTheme();
    document.documentElement.dataset.theme = currentTheme;
    function setTheme(theme, persist = true) {
        const next = normalizeTheme(theme);
        const changed = next !== currentTheme;
        currentTheme = next;
        document.documentElement.dataset.theme = next;
        if (persist) {
            try { localStorage.setItem(themeKey, next); } catch (_) {}
        }
        document.querySelectorAll('[data-theme-toggle]').forEach(btn => {
            btn.setAttribute('aria-pressed', next === 'dark' ? 'true' : 'false');
            btn.dataset.currentTheme = next;
        });
        if (changed) window.dispatchEvent(new CustomEvent('themechange', { detail: { theme: next } }));
    }
    domReady.then(() => {
        setTheme(currentTheme, false);
        document.addEventListener('click', event => {
            const trigger = event.target.closest('[data-theme-toggle]');
            if (trigger) setTheme(currentTheme === 'dark' ? 'light' : 'dark');
        });
    });

    window.addEventListener('storage', event => {
        if (event.key === storageKey && event.newValue) setLanguage(event.newValue, false);
        if (event.key === themeKey && event.newValue) setTheme(event.newValue, false);
    });
    window.FlowTheme = {
        get current() { return currentTheme; },
        set: setTheme,
        toggle: () => setTheme(currentTheme === 'dark' ? 'light' : 'dark'),
    };
    window.I18n = {
        t, translateMessage, translateError, setText, setRawText, setMessage, apply, setLanguage, ready,
        escapeHtml: value => String(value ?? '').replace(/[&<>"']/g, character => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
        }[character])),
        get locale() { return locale; },
        headers: () => ({ 'Accept-Language': locale }),
    };
})();
