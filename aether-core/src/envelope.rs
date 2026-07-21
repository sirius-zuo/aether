use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    Invoke,
    Result,
    Error,
    Suspended,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub kind: EnvelopeKind,
    pub payload: Value,
    pub metadata: HashMap<String, String>,
}

impl Envelope {
    pub fn invoke(payload: Value, metadata: HashMap<String, String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind: EnvelopeKind::Invoke,
            payload,
            metadata,
        }
    }
}

/// Flatten an arbitrary node-input payload into the plain-text `input` the
/// AgentVerse built-in server reads from `payload.input`. Handles the shapes the
/// orchestrator produces: the initial goal (`{"goal": …}`), a prior agent's
/// result (`{"output": …}`), fan-in maps (`{dep_id: {…}}`), bare strings, and
/// arrays. Key precedence within an object: input, output, message, goal.
pub fn payload_text(payload: &serde_json::Value) -> String {
    use serde_json::Value;
    match payload {
        Value::String(s) => s.clone(),
        Value::Object(map) => {
            for key in ["input", "output", "message", "goal"] {
                if let Some(Value::String(s)) = map.get(key) {
                    return s.clone();
                }
            }
            map.values()
                .map(payload_text)
                .collect::<Vec<_>>()
                .join("\n\n---\n\n")
        }
        Value::Array(items) => items
            .iter()
            .map(payload_text)
            .collect::<Vec<_>>()
            .join("\n\n---\n\n"),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn payload_text_reads_known_keys_and_falls_back() {
        use serde_json::json;
        assert_eq!(super::payload_text(&json!("plain")), "plain");
        assert_eq!(super::payload_text(&json!({ "goal": "g" })), "g");
        assert_eq!(super::payload_text(&json!({ "output": "o" })), "o");
        assert_eq!(super::payload_text(&json!({ "input": "i" })), "i");
        assert_eq!(super::payload_text(&json!({ "message": "m" })), "m");
    }

    #[test]
    fn payload_text_joins_fan_in_map_and_array() {
        use serde_json::json;
        let fan_in = json!({ "pros": { "output": "A" }, "cons": { "output": "B" } });
        let joined = super::payload_text(&fan_in);
        assert!(joined.contains('A') && joined.contains('B') && joined.contains("---"));
        assert_eq!(
            super::payload_text(&json!([{ "output": "A" }, { "output": "B" }])),
            "A\n\n---\n\nB"
        );
    }
}
