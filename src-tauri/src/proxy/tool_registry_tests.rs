use super::*;

fn mixed_tools() -> Value {
    json!([
        {"type":"namespace","name":"functions","tools":[
            {"type":"function","name":"exec_command","parameters":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}},
            {"type":"function","name":"read_file","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}
        ]},
        {"type":"function","name":"adb","parameters":{"type":"object","properties":{"args":{"type":"array","items":{"type":"string"}}}}},
        {"type":"custom","name":"apply_patch","format":{"type":"text"}},
        {"type":"namespace","name":"browser","tools":[
            {"type":"function","name":"open","parameters":{"type":"object","properties":{"url":{"type":"string"}}}}
        ]},
        {"type":"tool_search","execution":"client"}
    ])
}

fn browser_history() -> Value {
    json!([
        {"type":"message","role":"user","content":"inspect local device"},
        {"type":"function_call","call_id":"call-browser","namespace":"browser","name":"open","arguments":"{}"},
        {"type":"function_call_output","call_id":"call-browser","output":"browser OK"},
        {"type":"message","role":"user","content":"now run adb devices"}
    ])
}

#[test]
fn native_responses_tools_survive_browser_followup_and_exact_wire_serialization() {
    for input in [json!("first turn"), browser_history()] {
        let body = json!({
            "model":"future-official-model", "stream":true, "tools":mixed_tools(),
            "tool_choice":{"type":"allowed_tools","mode":"auto","tools":[
                {"type":"function","name":"adb"},
                {"type":"function","namespace":"functions","name":"exec_command"},
                {"type":"function","namespace":"browser","name":"open"}
            ]},
            "parallel_tool_calls":true, "input":input
        });
        let transformed =
            transform_request_for_channel(&body, &Channel::Responses, &Channel::Responses);
        let wire = PreparedWireRequest::from_value(&Channel::Responses, &transformed);
        let sent: Value = serde_json::from_slice(wire.body()).unwrap();
        assert_eq!(sent["tools"], body["tools"]);
        assert_eq!(sent["tool_choice"], body["tool_choice"]);
        assert_eq!(sent["parallel_tool_calls"], true);
        assert_eq!(sent["input"], body["input"]);
    }
}

#[test]
fn cross_protocol_tools_include_local_execution_after_browser_followup() {
    let body = json!({"model":"mock","stream":true,"tools":mixed_tools(),"tool_choice":"auto","input":browser_history()});
    for channel in [Channel::Chat, Channel::Anthropic] {
        // /codex/v1 uses the dedicated converter for a Chat upstream;
        // the generic API adapter is not the Codex production entry point.
        let transformed = if matches!(channel, Channel::Chat) {
            transform_codex_chat::responses_to_chat_completions(body.clone()).unwrap()
        } else {
            responses_to_anthropic_request(&body)
        };
        let names: HashSet<&str> = transformed["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool.pointer("/function/name").or_else(|| tool.get("name")))
            .filter_map(Value::as_str)
            .collect();
        for expected in [
            "functions__exec_command",
            "functions__read_file",
            "adb",
            "apply_patch",
            "browser__open",
            "tool_search",
        ] {
            assert!(
                names.contains(expected),
                "missing {expected} on {channel:?}"
            );
        }
    }
}

#[test]
fn dynamically_loaded_browser_tools_do_not_replace_local_tools() {
    let body = json!({"model":"mock","stream":true,"tools":[
        {"type":"function","name":"exec_command","parameters":{"type":"object"}},
        {"type":"tool_search","execution":"client"}
    ], "input":[
        {"type":"tool_search_call","call_id":"search-1","arguments":{"query":"browser"}},
        {"type":"tool_search_output","call_id":"search-1","tools":[
            {"type":"namespace","name":"browser","tools":[{"type":"function","name":"open","parameters":{"type":"object"}}]}
        ]},
        {"type":"message","role":"user","content":"run adb via exec_command"}
    ]});
    let context = transform_codex_chat::build_codex_tool_context_from_request(&body);
    assert!(context.lookup_chat_name("exec_command").is_some());
    assert!(context.lookup_chat_name("browser__open").is_some());
    assert!(context.lookup_chat_name("tool_search").is_some());
}

#[test]
fn custom_tool_call_and_output_keep_their_pairing_on_every_codex_adapter() {
    let body = json!({"model":"mock","tools":mixed_tools(),"input":[
        {"type":"custom_tool_call","id":"ctc_test","call_id":"call_custom_test","name":"apply_patch","input":"*** Begin Patch\n*** End Patch"},
        {"type":"custom_tool_call_output","id":"ctco_test","call_id":"call_custom_test","output":"patch applied"},
        {"type":"message","role":"user","content":"continue"}
    ]});
    let native = transform_request_for_channel(&body, &Channel::Responses, &Channel::Responses);
    let wire = PreparedWireRequest::from_value(&Channel::Responses, &native);
    let sent: Value = serde_json::from_slice(wire.body()).unwrap();
    assert_eq!(sent["input"], body["input"]);
    let chat = transform_codex_chat::responses_to_chat_completions(body.clone()).unwrap();
    assert_eq!(
        chat["messages"][0]["tool_calls"][0]["id"],
        "call_custom_test"
    );
    assert_eq!(chat["messages"][1]["tool_call_id"], "call_custom_test");
    assert_eq!(chat["messages"][1]["content"], "patch applied");
    let anthropic = responses_to_anthropic_request(&body);
    assert_eq!(
        anthropic["messages"][0]["content"][0]["id"],
        "call_custom_test"
    );
    assert_eq!(
        anthropic["messages"][1]["content"][0]["tool_use_id"],
        "call_custom_test"
    );
    assert_eq!(
        anthropic["messages"][1]["content"][0]["content"],
        "patch applied"
    );
}

#[test]
fn namespaced_custom_tools_round_trip_declarations_choice_history_and_response() {
    let body = json!({"model":"mock", "tools":[
        {"type":"namespace","name":"functions","tools":[
            {"type":"function","name":"exec","parameters":{"type":"object"}},
            {"type":"custom","name":"apply_patch","format":{"type":"text"}}
        ]}
    ],"tool_choice":{"type":"custom","namespace":"functions","name":"apply_patch"},"input":[
        {"type":"custom_tool_call","id":"ctc_namespaced","call_id":"call_namespaced","namespace":"functions","name":"apply_patch","input":"patch"},
        {"type":"custom_tool_call_output","call_id":"call_namespaced","output":"applied"}
    ]});
    let context = transform_codex_chat::build_codex_tool_context_from_request(&body);
    let spec = context.lookup_chat_name("functions__apply_patch").unwrap();
    assert_eq!(spec.kind, transform_codex_chat::CodexToolKind::Custom);
    assert_eq!(spec.namespace.as_deref(), Some("functions"));
    let chat = transform_codex_chat::responses_to_chat_completions(body.clone()).unwrap();
    assert!(chat["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tool| tool["function"]["name"] == "functions__apply_patch"));
    assert_eq!(
        chat["tool_choice"]["function"]["name"],
        "functions__apply_patch"
    );
    assert_eq!(
        chat["messages"][0]["tool_calls"][0]["function"]["name"],
        "functions__apply_patch"
    );
    assert_eq!(
        chat["messages"][0]["tool_calls"][0]["id"],
        "call_namespaced"
    );
    assert_eq!(chat["messages"][1]["tool_call_id"], "call_namespaced");
    assert_eq!(chat["messages"][1]["content"], "applied");
    let anthropic = responses_to_anthropic_request(&body);
    assert!(anthropic["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tool| tool["name"] == "functions__apply_patch"));
    assert_eq!(anthropic["tool_choice"]["name"], "functions__apply_patch");
    assert_eq!(
        anthropic["messages"][0]["content"][0]["name"],
        "functions__apply_patch"
    );
    assert_eq!(
        anthropic["messages"][0]["content"][0]["id"],
        "call_namespaced"
    );
    assert_eq!(
        anthropic["messages"][1]["content"][0]["tool_use_id"],
        "call_namespaced"
    );
    let item_id = transform_codex_chat::response_tool_call_item_id_from_chat_name(
        "call_return",
        "functions__apply_patch",
        &context,
    );
    assert!(item_id.starts_with("ctc_"));
    let restored = transform_codex_chat::response_tool_call_item_from_chat_name(
        &item_id,
        "completed",
        "call_return",
        "functions__apply_patch",
        r#"{"input":"patch"}"#,
        None,
        &context,
    );
    assert_eq!(restored["type"], "custom_tool_call");
    assert_eq!(restored["namespace"], "functions");
    assert_eq!(restored["name"], "apply_patch");
    assert_eq!(restored["call_id"], "call_return");
    assert_eq!(restored["input"], "patch");
}
