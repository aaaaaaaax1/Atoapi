import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { writeIsolatedResponsesConfig } from "./isolated-responses-fixture.mjs";

// Secret-free, localhost-only. No real browser, ADB, or upstream requests.
const executable = resolve(process.argv[2] ?? "src-tauri/target/release/atoapi.exe");
const diagnosticsEnabled = !process.argv.includes("--diagnostics-off");
const simulateMissingOutput = process.argv.includes("--missing-output-error");
const turnCount = simulateMissingOutput ? 1 : 4;
const captured = [];
const outputs = [
  { type: "function_call", id: "fc_browser", call_id: "call_browser", namespace: "browser", name: "open", arguments: "{}", status: "completed" },
  { type: "function_call", id: "fc_local", call_id: "call_local", namespace: "functions", name: "exec_command", arguments: JSON.stringify({ cmd: "adb devices" }), status: "completed" },
  { type: "custom_tool_call", id: "ctc_patch", call_id: "call_custom_patch", name: "apply_patch", input: "*** Begin Patch\n*** End Patch", status: "completed" }
];
const tools = [
  { type: "namespace", name: "functions", tools: [
    { type: "function", name: "exec_command", parameters: { type: "object", properties: { cmd: { type: "string" } } } },
    { type: "function", name: "read_file", parameters: { type: "object", properties: { path: { type: "string" } } } }
  ] },
  { type: "namespace", name: "browser", tools: [
    { type: "function", name: "open", parameters: { type: "object", properties: {} } }
  ] },
  { type: "function", name: "adb", parameters: { type: "object", properties: { args: { type: "array", items: { type: "string" } } } } },
  { type: "custom", name: "apply_patch", format: { type: "text" } },
  { type: "tool_search", execution: "client" }
];
const toolChoice = { type: "allowed_tools", mode: "auto", tools: [
  { type: "function", namespace: "functions", name: "exec_command" },
  { type: "function", namespace: "browser", name: "open" },
  { type: "function", name: "adb" },
  { type: "custom", name: "apply_patch" }
] };
let child;
let tempRoot;
const upstream = createServer(async (request, response) => {
  try {
    assert.equal(request.method, "POST");
    assert.equal(request.url, "/v1/responses");
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    captured.push(body);
    const output = outputs[captured.length - 1];
    response.writeHead(200, { "content-type": "text/event-stream" });
    if (simulateMissingOutput) {
      response.end("event: response.failed\ndata: " + JSON.stringify({ type: "response.failed", response: {
        id: "resp_missing_output", model: body.model, status: "failed",
        error: { code: "invalid_request_error", message: "No tool output found for custom tool call call_custom_patch. SECRET_UPSTREAM_ERROR_DETAIL" }
      } }) + "\n\n");
    } else response.end("event: response.completed\ndata: " + JSON.stringify({ type: "response.completed", response: {
      id: "resp_tool_wire_" + captured.length, model: body.model, status: "completed",
      output: output ? [output] : [], usage: { input_tokens: 100, output_tokens: 1 }
    } }) + "\n\ndata: [DONE]\n\n");
  } catch (error) {
    response.writeHead(500); response.end(String(error));
  }
});
try {
  await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  tempRoot = await mkdtemp(join(tmpdir(), "atoapi-tools-wire-"));
  const { localKey } = await writeIsolatedResponsesConfig(tempRoot, upstream.address().port, { model: "tool-wire-model", toolDiagnosticsEnabled: diagnosticsEnabled });
  const reservation = createNetServer();
  await new Promise((resolve) => reservation.listen(0, "127.0.0.1", resolve));
  const port = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));
  child = spawn(executable, [], { stdio: "ignore", windowsHide: true, env: {
    ...process.env, ATOAPI_CONFIG_DIR: tempRoot, ATOAPI_ISOLATED_TEST_INSTANCE: "1",
    ATOAPI_HEADLESS_ISOLATED_TEST: "1", ATOAPI_TEST_LISTEN_PORT: String(port),
    ATOAPI_AUTOMATIC_CACHE_CANARY: "0"
  } });
  let launchError;
  child.on("error", (error) => { launchError = error; });
  const baseUrl = "http://127.0.0.1:" + port;
  let ready = false;
  for (let retry = 0; retry < 100; retry += 1) {
    if (launchError) throw launchError;
    if (child.exitCode != null) throw new Error("isolated proxy exited: " + child.exitCode);
    try { ready = (await fetch(baseUrl + "/health", { signal: AbortSignal.timeout(1000) })).ok; } catch {}
    if (ready) break;
    await delay(100);
  }
  assert.ok(ready, "isolated proxy did not become healthy");
  const input = [{ type: "message", role: "user", content: "SECRET_CHAT_MUST_NOT_BE_LOGGED" }];
  if (simulateMissingOutput) input.push(outputs[2], { type: "custom_tool_call_output", call_id: outputs[2].call_id, output: [{ type: "input_text", text: "SECRET_TOOL_OUTPUT_MUST_NOT_BE_LOGGED" }] });
  for (let turn = 0; turn < turnCount; turn += 1) {
    if (turn > 0) {
      input.push(outputs[turn - 1]);
      const custom = outputs[turn - 1].type === "custom_tool_call";
      input.push({ type: custom ? "custom_tool_call_output" : "function_call_output", call_id: outputs[turn - 1].call_id, output: custom ? [{ type: "input_text", text: "SECRET_TOOL_OUTPUT_MUST_NOT_BE_LOGGED" }] : "SECRET_TOOL_OUTPUT_MUST_NOT_BE_LOGGED" });
      input.push({ type: "message", role: "user", content: turn === 1 ? "Now use local execution" : "Finish" });
    }
    const response = await fetch(baseUrl + "/codex/v1/responses", { method: "POST", headers: {
      "content-type": "application/json", authorization: "Bearer " + localKey,
      "x-codex-turn-metadata": JSON.stringify({ session_id: "tools-wire", thread_id: "tools-wire", request_kind: "turn" })
    }, body: JSON.stringify({ model: "tool-wire-model", tools, tool_choice: toolChoice, parallel_tool_calls: true, input, stream: true }), signal: AbortSignal.timeout(15_000) });
    const text = await response.text();
    assert.equal(response.status, 200, text.slice(0, 500));
    assert.match(text, simulateMissingOutput ? /response.failed/ : /response.completed/);
    assert.equal(captured.length, turn + 1, "must forward exactly once per inbound");
    assert.deepEqual(captured[turn].tools, tools, "tool definitions changed on turn " + turn);
    assert.deepEqual(captured[turn].tool_choice, toolChoice, "tool choice changed on turn " + turn);
    assert.equal(captured[turn].parallel_tool_calls, true);
    assert.deepEqual(captured[turn].input, input, "tool history changed on turn " + turn);
    if (!simulateMissingOutput && turn < outputs.length) {
      assert.ok(text.includes(outputs[turn].name), "tool call name missing downstream");
      if (outputs[turn].namespace) assert.ok(text.includes(outputs[turn].namespace), "tool namespace missing downstream");
    }
  }
  let events = [];
  if (diagnosticsEnabled) {
  for (let retry = 0; retry < 40; retry += 1) {
    try { events = (await readFile(join(tempRoot, "tool-diagnostics.jsonl"), "utf8")).trim().split("\n").filter(Boolean).map(JSON.parse); } catch {}
    if (events.length >= turnCount * 4) break;
    await delay(50);
  }
  assert.ok(events.length >= turnCount * 4, "diagnostics must default on and capture all boundaries");
  for (let index = 0; index < turnCount; index += 1) {
    const ingress = events.filter((event) => event.stage === "ingress")[index];
    const pair = events.filter((event) => event.request_id === ingress.request_id);
    assert.equal(pair.length, 4, "ingress, egress and both responses must share a request ID");
    const egress = pair.find((event) => event.stage === "egress");
    assert.deepEqual(egress.metadata.tools, ingress.metadata.tools);
    assert.deepEqual(egress.metadata.tool_choice, ingress.metadata.tool_choice);
    assert.equal(egress.metadata.source, "frozen_wire_tool_roots");
    assert.deepEqual(egress.metadata.history_links, ingress.metadata.history_links);
    assert.deepEqual(egress.metadata.pairing.calls_without_results_in_this_request, []);
    for (const event of pair.filter((event) => event.stage.endsWith("response"))) {
      assert.equal(event.metadata.complete, true);
      if (simulateMissingOutput) {
        assert.equal(event.metadata.error_class, "missing_custom_tool_output");
        assert.equal(event.metadata.missing_output_call_id, "call_custom_patch");
      } else if (index < outputs.length) {
        assert.ok(event.metadata.tool_calls.some((call) => call.call_id === outputs[index].call_id));
      }
    }
  }
  const diagnosticText = JSON.stringify(events);
  assert.ok(!diagnosticText.includes("SECRET_"), "diagnostics leaked content");
  assert.ok(!diagnosticText.includes("adb devices"), "diagnostics leaked arguments");
  } else {
    await delay(100);
    await assert.rejects(readFile(join(tempRoot, "tool-diagnostics.jsonl"), "utf8"), { code: "ENOENT" });
  }
  console.log(JSON.stringify({ pass: true, executable, scenario: simulateMissingOutput ? "upstream-missing-output-error" : "browser-local-custom-four-turn", turns: captured.length, inboundEqualsUpstreamPosts: true, toolsExact: true, toolChoiceExact: true, browserThenLocalExecution: !simulateMissingOutput, customToolOutputPairing: true, diagnosticsEnabled, diagnosticBoundaries: diagnosticsEnabled, diagnosticPrivacy: diagnosticsEnabled, realUpstreamRequests: 0 }, null, 2));
} finally {
  if (child && child.exitCode == null) {
    child.kill();
    await Promise.race([new Promise((resolve) => child.once("exit", resolve)), delay(5000)]);
  }
  await new Promise((resolve) => upstream.close(resolve));
  if (tempRoot) await rm(tempRoot, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
}
