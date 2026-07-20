use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Payload carried in an `EnvelopeKind::Suspended` response (agent -> aether).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SuspendPayload {
    pub session_id: String,
    pub approval_id: String,
    pub kind: String,
    pub prompt: String,
}

/// Human decision relayed by aether to the agent's `/aether/resume` endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approved,
    Rejected { reason: Option<String> },
    Modified { payload: Value },
}

/// Body of a `POST /aether/resume` request (aether -> agent).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResumeRequest {
    pub session_id: String,
    pub approval_id: String,
    pub decision: ApprovalDecision,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suspend_payload_roundtrips() {
        let p = SuspendPayload {
            session_id: "s1".into(),
            approval_id: "a1".into(),
            kind: "tool_approval".into(),
            prompt: "Approve deleting file X?".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        let back: SuspendPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn resume_request_serializes_decision() {
        let req = ResumeRequest {
            session_id: "s1".into(),
            approval_id: "a1".into(),
            decision: ApprovalDecision::Rejected {
                reason: Some("no".into()),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["decision"]["type"], "rejected");
        assert_eq!(json["decision"]["reason"], "no");
    }

    fn assert_fixture_roundtrip<T>(fixture: &str)
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let expected: serde_json::Value = serde_json::from_str(fixture).unwrap();
        let parsed: T = serde_json::from_str(fixture).unwrap();
        let actual = serde_json::to_value(&parsed).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn suspend_payload_matches_golden_fixture() {
        assert_fixture_roundtrip::<SuspendPayload>(include_str!(
            "../tests/fixtures/suspend_payload.json"
        ));
    }

    #[test]
    fn resume_request_matches_golden_fixtures() {
        assert_fixture_roundtrip::<ResumeRequest>(include_str!(
            "../tests/fixtures/resume_request_approved.json"
        ));
        assert_fixture_roundtrip::<ResumeRequest>(include_str!(
            "../tests/fixtures/resume_request_rejected.json"
        ));
        assert_fixture_roundtrip::<ResumeRequest>(include_str!(
            "../tests/fixtures/resume_request_modified.json"
        ));
    }
}
