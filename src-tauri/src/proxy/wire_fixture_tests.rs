use super::*;

#[test]
fn responses_body_serialization_keeps_stable_history_before_dynamic_invocation() {
    // Frozen from the v0.2.3 canonical Responses serializer at commit
    // 6b22e24c4106706680b3a3699f2e5f3d40b27669. Keep this literal independent
    // from every current helper so an accidental change to both serializers
    // cannot make the regression test drift with the implementation.
    const V0_2_3_RESPONSES_WIRE_FIXTURE: &str = concat!(
        r#"{"model":"gpt-5.5","prompt_cache_key":"stable-cache-key","instructions":"stable system","tools":[{"name":"read_file","type":"function"}],"input":[{"content":"fresh","role":"user","type":"message"}],"reasoning":{"effort":"low"},"temperature":0,"include":["reasoning.encrypted_content"],"stream":true,"store":true,"service_tier":"auto","truncation":"auto","previous_response_id":"resp_dynamic","metadata":{"trace":"dynamic"},"z_vendor_extension":{"escaped":"line\n\"雪\""}}"#,
    );
    let body = json!({
        "temperature": 0,
        "reasoning": { "effort": "low" },
        "previous_response_id": "resp_dynamic",
        "stream": true,
        "store": true,
        "include": ["reasoning.encrypted_content"],
        "service_tier": "auto",
        "truncation": "auto",
        "input": [{ "type": "message", "role": "user", "content": "fresh" }],
        "tools": [{ "type": "function", "name": "read_file" }],
        "prompt_cache_key": "stable-cache-key",
        "model": "gpt-5.5",
        "instructions": "stable system",
        "metadata": { "trace": "dynamic" },
        "z_vendor_extension": { "escaped": "line\n\"雪\"" }
    });

    let serialized = serialize_responses_body_for_provider_prefix(&body);
    let current_reference = current_reference_serialize_responses_body_for_provider_prefix(&body);
    let serialized_bytes = serialize_responses_body_bytes_for_provider_prefix(&body);
    let prepared = PreparedWireRequest::from_value(&Channel::Responses, &body);

    let model_at = serialized.find("\"model\"").unwrap();
    let cache_key_at = serialized.find("\"prompt_cache_key\"").unwrap();
    let instructions_at = serialized.find("\"instructions\"").unwrap();
    let tools_at = serialized.find("\"tools\"").unwrap();
    let input_at = serialized.find("\"input\"").unwrap();
    let reasoning_at = serialized.find("\"reasoning\"").unwrap();
    let temperature_at = serialized.find("\"temperature\"").unwrap();
    let include_at = serialized.find("\"include\"").unwrap();
    let stream_at = serialized.find("\"stream\"").unwrap();
    let store_at = serialized.find("\"store\"").unwrap();
    let service_tier_at = serialized.find("\"service_tier\"").unwrap();
    let truncation_at = serialized.find("\"truncation\"").unwrap();
    let previous_response_at = serialized.find("\"previous_response_id\"").unwrap();
    let metadata_at = serialized.find("\"metadata\"").unwrap();

    assert!(model_at < cache_key_at);
    assert!(cache_key_at < instructions_at);
    assert!(instructions_at < tools_at);
    assert!(tools_at < input_at);
    assert!(input_at < reasoning_at);
    assert!(reasoning_at < temperature_at);
    assert!(temperature_at < include_at);
    assert!(include_at < stream_at);
    assert!(stream_at < store_at);
    assert!(store_at < service_tier_at);
    assert!(service_tier_at < truncation_at);
    assert!(truncation_at < previous_response_at);
    assert!(previous_response_at < metadata_at);
    assert!(serde_json::from_str::<Value>(&serialized).is_ok());
    assert_eq!(serialized, V0_2_3_RESPONSES_WIRE_FIXTURE);
    assert_eq!(serialized, current_reference);
    assert_eq!(serialized_bytes.as_slice(), current_reference.as_bytes());
    assert_eq!(prepared.body().as_ref(), serialized.as_bytes());
    assert_eq!(prepared.len(), serialized.len());
    assert!(prepared.is_stream());
}
