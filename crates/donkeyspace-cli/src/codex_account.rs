//! Read account metadata through Codex, without reading or copying credentials.
use serde::Deserialize;
use serde_json::{Value, json};
use std::{fmt, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum CodexAccount {
    #[default]
    NotConfigured,
    Checking,
    SignedOut,
    ChatGpt {
        email: Option<String>,
        plan: String,
    },
    ApiKey,
    Unavailable,
}

impl fmt::Display for CodexAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotConfigured => f.write_str("not configured"),
            Self::Checking => f.write_str("checking account…"),
            Self::SignedOut => f.write_str("not signed in"),
            Self::ChatGpt { email, plan } => {
                let plan = match plan.as_str() {
                    "free" => "Free",
                    "go" => "Go",
                    "plus" => "Plus",
                    "pro" => "Pro",
                    "prolite" => "Pro Lite",
                    "team" => "Team",
                    "business" => "Business",
                    "self_serve_business_prolite" => "Business Pro Lite",
                    "self_serve_business_usage_based" => "Business Usage Based",
                    "enterprise" => "Enterprise",
                    "edu" => "Edu",
                    "unknown" => "plan unavailable",
                    other => other,
                };
                write!(
                    f,
                    "{} ({plan})",
                    email.as_deref().unwrap_or("ChatGPT; email unavailable")
                )
            }
            Self::ApiKey => f.write_str("API key (account email unavailable)"),
            Self::Unavailable => f.write_str("account unavailable"),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum AccountMetadata {
    #[serde(rename = "chatgpt")]
    ChatGpt {
        email: Option<String>,
        #[serde(rename = "planType")]
        plan: String,
    },
    #[serde(rename = "apiKey")]
    ApiKey,
}

fn parse_account(result: Value) -> Result<CodexAccount, ()> {
    match result.get("account") {
        Some(Value::Null) => Ok(CodexAccount::SignedOut),
        Some(account) => match serde_json::from_value(account.clone()).map_err(|_| ())? {
            AccountMetadata::ChatGpt { email, plan } => Ok(CodexAccount::ChatGpt { email, plan }),
            AccountMetadata::ApiKey => Ok(CodexAccount::ApiKey),
        },
        None => Err(()),
    }
}

async fn response(
    output: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    id: u64,
) -> Result<Value, ()> {
    while let Some(line) = output.next_line().await.map_err(|_| ())? {
        let message: Value = serde_json::from_str(&line).map_err(|_| ())?;
        if message.get("id").and_then(Value::as_u64) == Some(id) {
            // Never include raw RPC errors or subprocess output in the UI.
            if message.get("error").is_some() {
                return Err(());
            }
            return message.get("result").cloned().ok_or(());
        }
    }
    Err(())
}

pub(crate) async fn read_account(home: &Path) -> CodexAccount {
    let mut command = Command::new("codex");
    command.arg("app-server");
    query_account(&mut command, home, Duration::from_secs(5)).await
}

async fn query_account(command: &mut Command, home: &Path, timeout: Duration) -> CodexAccount {
    // Match the configured mount, even when this CLI was launched with another
    // CODEX_HOME. Avoid loading the current repository's Codex configuration.
    command
        .env("CODEX_HOME", home)
        .current_dir(home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let Ok(mut child) = command.spawn() else {
        return CodexAccount::Unavailable;
    };
    let result = tokio::time::timeout(timeout, async {
        let mut input = child.stdin.take().ok_or(())?;
        let mut output = BufReader::new(child.stdout.take().ok_or(())?).lines();
        let initialize = json!({
            "method": "initialize", "id": 0,
            "params": {"clientInfo": {"name": "donkeyspace", "version": env!("CARGO_PKG_VERSION")}}
        });
        input
            .write_all(format!("{initialize}\n").as_bytes())
            .await
            .map_err(|_| ())?;
        response(&mut output, 0).await?;
        input.write_all(
            b"{\"method\":\"initialized\",\"params\":{}}\n{\"method\":\"account/read\",\"id\":1,\"params\":{\"refreshToken\":false}}\n"
        ).await.map_err(|_| ())?;
        parse_account(response(&mut output, 1).await?)
    })
    .await;
    // This short-lived server never starts an agent thread. Reap it on every
    // outcome, including a hung or unsupported account endpoint.
    let _ = child.kill().await;
    let _ = child.wait().await;
    result
        .ok()
        .and_then(Result::ok)
        .unwrap_or(CodexAccount::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_metadata_distinguishes_login_types_without_exposing_secrets() {
        let status = parse_account(json!({"account": {
            "type": "chatgpt", "email": "work@example.com", "planType": "business",
            "accessToken": "must-not-display"
        }}))
        .unwrap();
        assert_eq!(status.to_string(), "work@example.com (Business)");
        let status =
            parse_account(json!({"account": {"type": "apiKey", "apiKey": "secret"}})).unwrap();
        assert_eq!(status, CodexAccount::ApiKey);
        assert!(!status.to_string().contains("secret"));
        assert_eq!(
            parse_account(json!({"account": null})).unwrap(),
            CodexAccount::SignedOut
        );
        assert!(parse_account(json!({})).is_err());
        assert!(parse_account(json!({"account": {"type": "unknown"}})).is_err());
        assert!(parse_account(json!({"account": {"type": "chatgpt"}})).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn lookup_uses_configured_home_and_completes_the_handshake() {
        let home = std::env::temp_dir();
        let mut command = Command::new("sh");
        command.args(["-c", r#"
            test "$CODEX_HOME" = "$1" || exit 1
            read -r initialize
            case "$initialize" in *'"method":"initialize"'*) ;; *) exit 2 ;; esac
            printf '%s\n' '{"id":0,"result":{}}'
            read -r initialized
            case "$initialized" in *'"method":"initialized"'*) ;; *) exit 3 ;; esac
            read -r account
            case "$account" in *'"refreshToken":false'*) ;; *) exit 4 ;; esac
            printf '%s\n' '{"method":"account/updated","params":{}}'
            printf '%s\n' '{"id":1,"result":{"account":{"type":"chatgpt","email":"work@example.com","planType":"business"}}}'
        "#, "account-test"]).arg(&home);
        let status = query_account(&mut command, &home, Duration::from_secs(2)).await;
        assert_eq!(status.to_string(), "work@example.com (Business)");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn lookup_timeout_and_rpc_errors_are_unavailable() {
        for script in [
            "read -r request; read -r never_sent",
            "printf '%s\\n' '{\"id\":0,\"error\":{\"message\":\"secret\"}}'",
        ] {
            let mut command = Command::new("sh");
            command.args(["-c", script]);
            assert_eq!(
                query_account(
                    &mut command,
                    &std::env::temp_dir(),
                    Duration::from_millis(100)
                )
                .await,
                CodexAccount::Unavailable,
            );
        }
    }
}
