use serde_json::Value;

/// Extract a user-facing text string from an incoming Envelope payload.
///
/// - a JSON string is used as-is
/// - an object with a `message` (or `goal`) string field uses that field
/// - an array (fan-in input) joins its elements' extracted text
/// - anything else is stringified JSON
pub fn extract_text(payload: &Value) -> String {
    match payload {
        Value::String(s) => s.clone(),
        Value::Object(map) => {
            if let Some(Value::String(s)) = map.get("message") {
                s.clone()
            } else if let Some(Value::String(s)) = map.get("goal") {
                s.clone()
            } else {
                payload.to_string()
            }
        }
        Value::Array(items) => items
            .iter()
            .map(extract_text)
            .collect::<Vec<_>>()
            .join("\n\n---\n\n"),
        _ => payload.to_string(),
    }
}

/// Best-effort: pull the first JSON object out of model text, tolerating
/// markdown fences or surrounding prose by slicing first `{` .. last `}`.
pub fn extract_json(text: &str) -> Option<Value> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end < start {
        return None;
    }
    serde_json::from_str(&text[start..=end]).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_text_from_message_object() {
        assert_eq!(extract_text(&json!({ "message": "hi" })), "hi");
    }

    #[test]
    fn extract_text_from_goal_object() {
        assert_eq!(extract_text(&json!({ "goal": "do X" })), "do X");
    }

    #[test]
    fn extract_text_from_string() {
        assert_eq!(extract_text(&json!("plain")), "plain");
    }

    #[test]
    fn extract_text_joins_array() {
        let v = json!([{ "message": "a" }, { "message": "b" }]);
        assert_eq!(extract_text(&v), "a\n\n---\n\nb");
    }

    #[test]
    fn extract_json_plain_object() {
        let v = extract_json(r#"{"nodes":[]}"#).unwrap();
        assert_eq!(v, json!({ "nodes": [] }));
    }

    #[test]
    fn extract_json_strips_fences_and_prose() {
        let raw = "Here is the plan:\n```json\n{\"nodes\":[]}\n```\nDone.";
        assert_eq!(extract_json(raw).unwrap(), json!({ "nodes": [] }));
    }

    #[test]
    fn extract_json_returns_none_when_no_object() {
        assert!(extract_json("no json here").is_none());
    }
}
