import graphitePrototypeHtml from "../../prototype/atoapi-graphite-ui.html?raw";
import lucideUmdUrl from "lucide/dist/umd/lucide.min.js?url";

/**
 * Builds the immutable Graphite iframe document once per desktop bundle.
 * Runtime metrics travel over the frame protocol; they must never recreate
 * this document or reset the iframe's DOM/event state.
 */
export function createGraphitePrototypeDocument(bridgeSource: string): string {
  const withIds = graphitePrototypeHtml
    .replace('<link rel="preconnect" href="https://fonts.googleapis.com" />\n', "")
    .replace('<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />\n', "")
    .replace('<link href="https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500;600&display=swap" rel="stylesheet" />\n', "")
    .replace(
      '<script src="https://unpkg.com/lucide@0.468.0/dist/umd/lucide.min.js"></script>',
      `<link rel="preload" as="script" href="${lucideUmdUrl}"><script src="${lucideUmdUrl}"></script>`
    )
    .replace(
      '<label class="field"><span>通道</span><select><option>Auto</option><option selected>Responses</option><option>Chat</option><option>Anthropic</option></select></label>',
      '<label class="field"><span>通道</span><select id="providerChannelInput"><option value="auto">Auto</option><option value="responses" selected>Responses</option><option value="chat">Chat</option><option value="anthropic">Anthropic</option></select></label>'
    )
    .replace(
      '<label class="field wide"><span>Models URL（可选）</span><input value="https://api.yunzhou.example/v1/models"',
      '<label class="field wide"><span>Models URL（可选）</span><input id="providerModelsUrlInput" value="https://api.yunzhou.example/v1/models"'
    )
    .replace(
      '<label class="field"><span>自定义 User-Agent</span><input value="Atoapi/next"',
      '<label class="field"><span>自定义 User-Agent</span><input id="providerCustomUserAgentInput" value="Atoapi/next"'
    )
    .replace(
      '<label class="field"><span>统计刷新</span><select><option>页面可见时 1 秒</option><option>5 秒</option><option>手动</option></select></label>',
      '<label class="field"><span>统计刷新</span><select id="settingsRefreshPolicy"><option value="visible-1s">页面可见时 1 秒</option><option value="5s">5 秒</option><option value="manual">手动</option></select></label>'
    )
    .replace(
      '<label class="field wide"><span>版本</span><input value="vNext UI Prototype" readonly autocomplete="off" /></label>',
      '<label class="field wide"><span>版本</span><input id="settingsAppVersion" value="vNext UI Prototype" readonly autocomplete="off" /></label>'
    );
  return withIds.replace("</body>", `<script>${bridgeSource}</script></body>`);
}
