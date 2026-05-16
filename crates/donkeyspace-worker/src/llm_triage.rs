use donkeyspace_core::RunResult;
use octocrab::Octocrab;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriageProvider {
    Auto,
    Deterministic,
    OpenAiCompatible,
    Agent,
}

impl TriageProvider {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "llm" | "openai" | "openai-compatible" | "openrouter" => Self::OpenAiCompatible,
            "deterministic" | "fake" | "local" => Self::Deterministic,
            "agent" | "command" | "external" => Self::Agent,
            _ => Self::Auto,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LlmTriageConfig {
    pub provider: TriageProvider,
    pub base_url: String,
    pub api_key: Option<String>,
    pub model: String,
}

impl LlmTriageConfig {
    pub fn should_use_llm(&self) -> bool {
        match self.provider {
            TriageProvider::OpenAiCompatible => true,
            TriageProvider::Auto => self
                .api_key
                .as_ref()
                .map(|key| !key.trim().is_empty())
                .unwrap_or(false),
            TriageProvider::Deterministic | TriageProvider::Agent => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenAiTriageClient {
    client: Octocrab,
    model: String,
}

impl OpenAiTriageClient {
    pub fn new(config: &LlmTriageConfig) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        if !config.should_use_llm() {
            return Ok(None);
        }

        let Some(api_key) = config
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|key| !key.is_empty())
        else {
            return Err(
                "LLM triage provider requires DONKEYSPACE_LLM_API_KEY or OPENROUTER_API_KEY".into(),
            );
        };

        Ok(Some(Self {
            client: Octocrab::builder()
                .base_uri(config.base_url.trim_end_matches('/'))?
                .personal_token(api_key.to_string())
                .build()?,
            model: config.model.clone(),
        }))
    }

    pub async fn triage_issue(
        &self,
        input: &Value,
    ) -> Result<RunResult, Box<dyn std::error::Error>> {
        let request = chat_request(&self.model, input);
        let response: ChatCompletionResponse = self
            .client
            .post("/chat/completions", Some(&request))
            .await?;
        let content = response
            .choices
            .first()
            .map(|choice| choice.message.content.trim())
            .filter(|content| !content.is_empty())
            .ok_or("LLM response did not include message content")?;
        let result = parse_triage_result(content)?;
        result.validate_for_orchestration()?;
        Ok(result)
    }
}

#[derive(Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f32,
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Debug, Deserialize)]
struct ChatChoiceMessage {
    content: String,
}

fn chat_request(model: &str, input: &Value) -> ChatCompletionRequest {
    ChatCompletionRequest {
        model: model.to_string(),
        temperature: 0.0,
        messages: vec![
            ChatMessage {
                role: "system",
                content: system_prompt(),
            },
            ChatMessage {
                role: "user",
                content: issue_prompt(input),
            },
        ],
    }
}

fn system_prompt() -> String {
    [
        "You are donkeyspace's repository issue triage agent.",
        "Decide whether a GitHub issue has enough context for an implementation agent.",
        "Return only one JSON object. Use these fields exactly:",
        r#"{"outcome":"ready","summary":"short summary","confidence":"medium","risk":"unknown","questions":[],"tests":[],"changed_files":[],"human_review_reason":null,"blocked_reason":null}"#,
        "Use outcome needs_info when acceptance criteria, reproduction steps, expected behavior, or relevant files are missing.",
        "Do not ask for repository files, paths, or contents that are already present in the repository context.",
        "For needs_info, include one to three specific questions.",
        "Use outcome ready only when a developer agent can start without guessing.",
        "Use outcome needs_human for high-risk, security-sensitive, legal, credentials, production data, or ambiguous ownership work.",
        "For needs_human, set human_review_reason to a short reason.",
        "For triage, tests must always be an empty array. Do not put suggested test commands in tests.",
        "Do not propose implementation steps. Do not include markdown.",
    ]
    .join("\n")
}

fn issue_prompt(input: &Value) -> String {
    let issue = input.pointer("/issue").unwrap_or(input);
    let labels = issue
        .pointer("/labels")
        .and_then(Value::as_array)
        .map(|labels| {
            labels
                .iter()
                .filter_map(|label| label.pointer("/name").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    let comment = input
        .pointer("/comment/body")
        .and_then(Value::as_str)
        .unwrap_or_default();

    format!(
        "Repository: {owner}/{repo}\nIssue number: {number}\nTitle: {title}\nLabels: {labels}\nBody:\n{body}\n\nLatest human comment:\n{comment}\n\nRepository context:\n{repository_context}",
        owner = input
            .pointer("/repository/owner/login")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        repo = input
            .pointer("/repository/name")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        number = issue
            .pointer("/number")
            .and_then(Value::as_i64)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        title = issue
            .pointer("/title")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        body = issue
            .pointer("/body")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        repository_context = repository_context_prompt(input),
    )
}

fn repository_context_prompt(input: &Value) -> String {
    let Some(context) = input.pointer("/repository_context") else {
        return "No repository checkout context was available.".to_string();
    };

    serde_json::to_string_pretty(context)
        .unwrap_or_else(|_| "Repository context was not serializable.".to_string())
}

fn extract_json_object(content: &str) -> Result<&str, Box<dyn std::error::Error>> {
    let trimmed = content.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        return Ok(trimmed);
    }

    let start = trimmed
        .find('{')
        .ok_or("LLM response did not contain JSON")?;
    let end = trimmed
        .rfind('}')
        .ok_or("LLM response did not contain JSON")?;
    if start >= end {
        return Err("LLM response JSON was malformed".into());
    }
    Ok(&trimmed[start..=end])
}

fn parse_triage_result(content: &str) -> Result<RunResult, Box<dyn std::error::Error>> {
    let json = extract_json_object(content)?;
    let mut value: Value = serde_json::from_str(json)?;

    if let Some(object) = value.as_object_mut() {
        object.insert("tests".to_string(), Value::Array(Vec::new()));
    }

    Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
    use super::{
        LlmTriageConfig, TriageProvider, chat_request, extract_json_object, parse_triage_result,
    };
    use donkeyspace_core::{Confidence, Outcome, Risk};
    use serde_json::json;

    #[test]
    fn auto_provider_uses_llm_when_api_key_is_present() {
        let config = LlmTriageConfig {
            provider: TriageProvider::Auto,
            base_url: "https://openrouter.ai/api/v1".to_string(),
            api_key: Some("key".to_string()),
            model: "openrouter/free".to_string(),
        };

        assert!(config.should_use_llm());
    }

    #[test]
    fn deterministic_provider_does_not_use_llm() {
        let config = LlmTriageConfig {
            provider: TriageProvider::Deterministic,
            base_url: "https://openrouter.ai/api/v1".to_string(),
            api_key: Some("key".to_string()),
            model: "openrouter/free".to_string(),
        };

        assert!(!config.should_use_llm());
    }

    #[test]
    fn agent_provider_does_not_use_llm() {
        let config = LlmTriageConfig {
            provider: TriageProvider::Agent,
            base_url: "https://openrouter.ai/api/v1".to_string(),
            api_key: Some("key".to_string()),
            model: "openrouter/free".to_string(),
        };

        assert!(!config.should_use_llm());
        assert_eq!(TriageProvider::parse("external"), TriageProvider::Agent);
    }

    #[test]
    fn request_includes_issue_context_and_model() {
        let request = chat_request(
            "openrouter/free",
            &json!({
                "repository": {"owner": {"login": "DanNicolau"}, "name": "donkeyspace-test-repo"},
                "issue": {"number": 7, "title": "Broken export", "body": "Export fails", "labels": [{"name": "ai:needs-info"}]},
                "comment": {"body": "It fails when exporting a large account."},
                "repository_context": {"file_tree": ["README.md"], "excerpts": [{"path": "README.md", "content": "# test", "truncated": false}]}
            }),
        );

        assert_eq!(request.model, "openrouter/free");
        assert!(request.messages[1].content.contains("Broken export"));
        assert!(request.messages[1].content.contains("ai:needs-info"));
        assert!(request.messages[1].content.contains("large account"));
        assert!(request.messages[1].content.contains("README.md"));
    }

    #[test]
    fn extracts_json_from_fenced_response() {
        let json = extract_json_object(
            "```json\n{\"outcome\":\"ready\",\"summary\":\"ok\",\"confidence\":\"high\",\"risk\":\"low\",\"questions\":[],\"tests\":[],\"changed_files\":[],\"human_review_reason\":null,\"blocked_reason\":null}\n```",
        )
        .unwrap();

        assert!(json.starts_with('{'));
        assert!(json.ends_with('}'));
    }

    #[test]
    fn parse_triage_result_discards_malformed_tests() {
        let result = parse_triage_result(
            r#"{"outcome":"ready","summary":"clear issue","confidence":"medium","risk":"low","questions":[],"tests":["cargo build should compile successfully"],"changed_files":[],"human_review_reason":null,"blocked_reason":null}"#,
        )
        .unwrap();

        assert_eq!(result.outcome, Outcome::Ready);
        assert_eq!(result.confidence, Confidence::Medium);
        assert_eq!(result.risk, Risk::Low);
        assert!(result.tests.is_empty());
    }

    #[test]
    fn system_prompt_requires_empty_tests_for_triage() {
        let prompt = super::system_prompt();

        assert!(prompt.contains("tests must always be an empty array"));
    }
}
