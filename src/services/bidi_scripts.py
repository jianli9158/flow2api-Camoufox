"""Static JavaScript snippets executed inside Camoufox WebDriver BiDi contexts."""

FLOW_BATCHEXECUTE_XHR_FN = r"""
async (cfg) => {
    let wiz = window.WIZ_global_data || {};
    let bl = wiz.cfb2h || cfg.cachedBl || '';
    let fSid = wiz.FdrFJe || cfg.cachedFSid || '';
    let at = wiz.SNlM0e || cfg.cachedAt || '';

    const fetchBootstrap = async (pathOverride) => {
        const targetPath = pathOverride || ((cfg.sourcePath && cfg.sourcePath.includes('/edit/')) ? '/projects' : (cfg.sourcePath || '/projects'));
        const pageResp = await fetch(targetPath, { credentials: 'include' });
        const html = await pageResp.text();
        const blMatch = html.match(/boq_labs-ai-sandbox-frontend_[A-Za-z0-9_.-]+/);
        const sidMatch = html.match(/"FdrFJe":"(-?[0-9]+)"/);
        const atMatch = html.match(/"SNlM0e"\s*:\s*"((?:\\.|[^"\\])+)"/);
        if (blMatch) bl = blMatch[0];
        if (sidMatch) fSid = sidMatch[1];
        if (atMatch) {
            try { at = JSON.parse('"' + atMatch[1] + '"'); } catch (_) { at = atMatch[1]; }
        }
    };

    if (!bl || !fSid || !at) {
        await fetchBootstrap();
        if (!at) {
            await fetchBootstrap('/projects');
        }
    }
    if (!bl || !fSid) {
        return { ok: false, error: 'Flow WIZ_global_data metadata unavailable' };
    }

    const postOnce = (xsrfToken) => new Promise((resolve, reject) => {
        const reqId = Math.floor(10000 + Math.random() * 80000);
        const url = `/_/AiSandboxAngularFrontend/data/batchexecute?rpcids=${encodeURIComponent(cfg.rpcId)}&source-path=${encodeURIComponent(cfg.sourcePath)}&bl=${encodeURIComponent(bl)}&f.sid=${encodeURIComponent(fSid)}&hl=en&_reqid=${reqId}&rt=c`;
        const params = new URLSearchParams();
        params.set('f.req', cfg.fReq);
        if (xsrfToken) params.set('at', xsrfToken);

        const xhr = new XMLHttpRequest();
        xhr.open('POST', url, true);
        xhr.withCredentials = true;
        xhr.timeout = cfg.timeoutMs;
        xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded;charset=UTF-8');
        xhr.setRequestHeader('x-same-domain', '1');
        xhr.onload = () => resolve({ ok: true, status: xhr.status, text: xhr.responseText || '', bl, fSid, at: xsrfToken });
        xhr.onerror = () => reject(new Error('XHR network error'));
        xhr.ontimeout = () => reject(new Error('XHR timeout'));
        xhr.send(params.toString());
    });

    let result = await postOnce(at);
    const xsrfMatch = (result.text || '').match(/\["xsrf"\s*,\s*"((?:\\.|[^"\\])*)"/);
    if (xsrfMatch) {
        let refreshedAt = xsrfMatch[1];
        try { refreshedAt = JSON.parse('"' + refreshedAt + '"'); } catch (_) {}
        if (window.WIZ_global_data) window.WIZ_global_data.SNlM0e = refreshedAt;
        at = refreshedAt;
        result = await postOnce(refreshedAt);
    } else if (result.status === 401 || /UNAUTHENTICATED|,\s*16\s*\]/.test(result.text || '')) {
        await fetchBootstrap('/projects');
        if (at) {
            if (window.WIZ_global_data) window.WIZ_global_data.SNlM0e = at;
            result = await postOnce(at);
        }
    }
    return result;
}
"""
