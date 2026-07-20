# Design System: Atoapi Quiet Relay

## 1. Product Role

Atoapi is a desktop relay control surface for repeated operational work. The primary screen lets a user switch between AI agents, inspect the selected agent endpoint, bind an upstream provider, open provider configuration, and enter cache/request observability. It is not a marketing site and must never use hero composition, oversized product claims, decorative illustrations, or explanatory feature cards.

The UI should feel like a premium instrument panel: quiet, exact, fast to scan, and comfortable during long sessions.

## 2. Visual Theme & Atmosphere

- **Theme name:** Quiet Precision.
- **Density:** Cockpit Dense, 8/10.
- **Variance:** Predictable with controlled asymmetry, 3/10.
- **Motion:** Restrained and functional, 2/10.
- **Mood:** Neutral graphite, precise machining, soft material depth, no gaming or cyberpunk cues.
- Build hierarchy with surface luminance, whitespace, typography weight, and sparse hairlines. Do not surround every control with a box.
- The first viewport is the actual control desk. It must show the Agent rail, selected Agent context, upstream list, and cache entry without scrolling at 1100x660.

## 3. Color Palette & Roles

Use one neutral temperature across the entire interface. No purple, blue, violet, beige, brown, orange-dominant, or gradient theme.

- **Carbon Canvas** (`#0C0E10`) — application background.
- **Graphite Chrome** (`#101316`) — top command rail and bottom utility rail.
- **Quiet Surface** (`#15191D`) — list body and configuration surfaces.
- **Raised Graphite** (`#1B2025`) — active row, selected Agent, hover and focus surfaces.
- **Pressed Graphite** (`#21272C`) — pressed control state only.
- **Soft Hairline** (`#252B30`) — internal row dividers.
- **Structural Hairline** (`#30373D`) — boundaries between major application regions.
- **Primary Text** (`#EEF1F2`) — headings, selected values and provider names.
- **Secondary Text** (`#B8C0C4`) — ordinary values and button labels.
- **Muted Text** (`#899399`) — metadata and inactive labels.
- **Faint Text** (`#606B71`) — URLs, timestamps and disabled states.
- **Mineral Sage** (`#78B596`) — the only product accent; active state, healthy state and cache hit rate.
- **Signal Amber** (`#C7A15F`) — warning state only, never decoration.
- **Signal Red** (`#CB7474`) — error and destructive state only, never decoration.

Never use gradients, neon glows, colored outer shadows, colored glass, bokeh, or decorative color blobs. A status dot may use a subtle opacity pulse; no other perpetual animation.

## 4. Typography Rules

- **UI family:** Geist, sans-serif.
- **Mono family:** Geist Mono or JetBrains Mono for URLs, endpoints, ports, keys, timings, token counts and percentages.
- Serif type is forbidden in this software UI.
- Letter spacing is always `0`.
- Use weight and color before increasing size.

### Desktop type scale

- Page/Agent title: `14px`, weight 650, line-height 18px.
- Section title: `12px`, weight 650, line-height 16px.
- Provider name and primary control label: `11.5px`, weight 600.
- Body/value text: `11px`, weight 450-550.
- Metadata and URLs: `10px`, weight 450, mono where numeric or technical.
- Micro label: `9.5px`, weight 500. Never go below 9.5px.
- Cache hit rate in the bottom rail: `13px`, weight 700, mono.
- No visible text on the primary screen may exceed 14px.

Text must remain readable at 100%, 125%, and 150% OS scaling. Use single-line ellipsis for URLs and long model names. Do not shrink text dynamically to hide layout problems.

## 5. Layout Architecture

Target the real desktop window first: 1100x660, with a supported minimum width of 900px.

- App shell rows: `48px` top command rail, `56px` selected Agent context, flexible workspace, `36px` cache utility rail.
- Main workspace padding: 14px horizontal, 12px vertical.
- Maximum content width: 1500px, centered only on very wide monitors.
- Use CSS Grid with `minmax(0, 1fr)` for every shrinkable track.
- Major surfaces may use one 1px boundary. Nested regions use spacing or a single divider, not another complete frame.
- Radius scale: 3px for compact controls, 5px for grouped surfaces, 7px only for drawers and dialogs.
- No nested cards. The upstream list is one flat grouped surface with rows, not a stack of cards.

## 6. Primary Screen Anatomy

### Top command rail

- Left: one icon-only Settings button and a compact Atoapi wordmark.
- Center: horizontal Agent rail with Codex, Claude, Gemini, OpenCode, OpenClaw and Proxy Mode.
- Right: compact running state, endpoint `127.0.0.1:18883`, Refresh and Info icons.
- Rail height is 48px. Agent items are 88-100px wide and never taller than 38px.
- Inactive Agents have no frame. The selected Agent uses Raised Graphite and a quiet 2px bottom indicator.
- Agent icon size 15px. Do not put every icon inside a bordered square.

### Selected Agent context

- Left: 34x19px switch, Agent name and route summary.
- Center: one consolidated endpoint rail containing Base URL and Local Key, separated by one vertical divider.
- Right: compact `+ 上游` command, height 30px.
- Do not use two large endpoint cards.

### Upstream region

- Header is a single 26px line with `上游路由` on the left and `3 可用 · 当前 云舟` on the right.
- Provider rows are 46px high; absolute maximum 50px at 1100px and 900px widths.
- Row columns: provider identity, channel/mapping/key metadata, actions.
- Provider name is first line. URL is second line and uses mono 10px.
- Metadata is plain text separated with middle dots: `Responses · 3 mappings · 2 keys`. Do not create three pills.
- Secondary line: `最近连通 188ms`.
- Active row uses Raised Graphite and a 2px neutral/sage inset indicator. Other rows remain flat.
- Actions: one compact text command (`当前` or `使用`), one test icon, one overflow menu. Editing and deletion live in the overflow menu. No row may show four boxed buttons.

### Cache utility rail

- Always visible at the bottom.
- Left: database icon plus `缓存与请求记录`.
- Right: `命中率 95.8%` and `128 成功 · 2 error`.
- No surrounding card and no large blank button frame.

## 7. Component Styling

### Buttons

- Icon button visible box: 28x28px; icon 14-15px; radius 3px.
- Primary compact command: height 30px; horizontal padding 10px; radius 4px.
- Default icon-only actions are transparent. Border and Raised Graphite appear on hover/focus.
- Primary command uses light neutral fill with charcoal text, not a bright accent fill.
- Active press translates down 1px. No glow, bounce, spring overshoot, or large shadow.
- Every unfamiliar icon has a tooltip and accessible name.

### Switches

- Desktop size 34x19px; familiar horizontal track and circular thumb.
- On state is light neutral track with dark thumb. Sage is reserved for status, not the entire switch.

### Inputs and selectors

- Height 32px; radius 4px; 1px Soft Hairline.
- Labels sit above inputs at 10px.
- Focus uses Structural Hairline and a 1px Mineral Sage inset ring.
- Error text sits below the input in Signal Red.

### Drawers and dialogs

- Drawer maximum width 960px for provider configuration and 500px for settings/details.
- Drawers may use a soft black shadow because elevation has meaning.
- Provider editor categories: Connection, Model Mapping, Multi-Key, Transport & Cache, Compatibility.
- Desktop layout uses a compact vertical category rail and an unframed content column.
- A model mapping row must keep request model, target model, context, reasoning and remove action visible without horizontal clipping.

### Status and tags

- Status pills are allowed only for health, error, warning, current selection or compatibility state.
- Do not use pills for ordinary metadata, channels, counts or labels.
- Status pills are 18px high with 6px horizontal padding.

## 8. Responsive Rules

- At 900px the overall desktop anatomy remains intact.
- Endpoint values may ellipsize, but copy buttons remain visible.
- Provider rows remain two-line rows and no taller than 52px.
- Preserve all actions. Collapse edit/delete into the overflow menu before hiding any functionality.
- Below 760px, the Agent rail scrolls horizontally with labels preserved. The Agent context becomes two rows, and provider identity/metadata share the left column while actions remain a fixed right column.
- No horizontal page overflow. Drawers become full width below 760px.
- Touch targets may expand to 40px below 760px while the icon remains 15px.

## 9. Motion & Interaction

- Hover/focus/press transitions: 110-140ms, opacity and transform only.
- Drawer entry: 160ms opacity plus 10px horizontal transform.
- List changes use a 60ms stagger only when items are newly added, never on every refresh.
- One running-status dot may pulse opacity from 0.65 to 1 over 3 seconds.
- Respect `prefers-reduced-motion` and remove nonessential motion.
- Metrics refresh must not move, resize or reflow the configuration surface.

## 10. Required Content For The Style Concept

Use realistic simulated data:

- Selected Agent: Codex, enabled.
- Route summary: `云舟 · Responses · 3 个模型映射`.
- Base URL: `http://127.0.0.1:18883/codex/v1`.
- Local Key: masked or `atoapi-codex-demo-key`.
- Providers:
  - 云舟 — Responses · 3 mappings · 2 keys · 188ms · current.
  - api.aiaiaiai — Auto · Agent passthrough · 1 key · 241ms.
  - sheapi — Chat · 2 mappings · 4 keys · 326ms.
- Bottom rail: `命中率 95.8% · 128 成功 · 2 error`.

## 11. Banned Patterns

- No marketing hero, landing page, product pitch, feature cards or instructional copy.
- No oversized typography, oversized buttons or tall list rows.
- No pure black, purple/blue neon, gradients, glows or glassmorphism.
- No beige, cream, brown, orange-dominant or blue/slate-dominant theme.
- No nested cards, bento layout, card grid, floating section cards or rounded container stacks.
- No repeated boxed icons and no forest of pills.
- No visible action text when a familiar icon plus tooltip is clearer.
- No hidden core actions at 900px.
- No negative letter spacing and no viewport-scaled font sizes.
- No overlapping text, controls, status indicators or dynamic content.
- No generic AI copywriting or visible explanations of how the interface works.
- No decorative images, illustrations, blobs, or fake charts on the primary screen.
