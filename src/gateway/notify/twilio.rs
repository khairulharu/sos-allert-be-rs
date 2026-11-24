//! Twilio SMS (`twilio_notifier.go`).

use crate::config::TwilioConfig;

use super::{http_client, response_snippet, ProviderError, ProviderResult};

pub const DEFAULT_BASE_URL: &str = "https://api.twilio.com";

pub struct TwilioNotifier {
    enabled: bool,
    account_sid: String,
    auth_token: String,
    from_number: String,
    base_url: String,
    client: reqwest::Client,
}

impl TwilioNotifier {
    pub fn new(config: &TwilioConfig) -> Self {
        Self {
            enabled: config.enabled,
            account_sid: config.account_sid.clone(),
            auth_token: config.auth_token.clone(),
            from_number: config.from_number.clone(),
            base_url: DEFAULT_BASE_URL.to_string(),
            client: http_client(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into().trim_end_matches('/').to_string();
        self
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// One SMS. `to` must already be canonical E.164 (the emergency-contact
    /// rule guarantees it for new rows).
    pub async fn send_sms(&self, to: &str, body: &str) -> ProviderResult<()> {
        if !self.enabled
            || self.account_sid.is_empty()
            || self.auth_token.is_empty()
            || self.from_number.is_empty()
        {
            return Err(ProviderError::NotConfigured);
        }

        let url = format!(
            "{}/2010-04-01/Accounts/{}/Messages.json",
            self.base_url, self.account_sid
        );
        let form = [
            ("To", to),
            ("From", self.from_number.as_str()),
            ("Body", body),
        ];

        let response = self
            .client
            .post(&url)
            .basic_auth(&self.account_sid, Some(&self.auth_token))
            .form(&form)
            .send()
            .await
            .map_err(|e| ProviderError::Failed(format!("twilio request: {e}")))?;

        if response.status().is_success() {
            return Ok(());
        }
        Err(ProviderError::Failed(format!(
            "twilio request failed: {}",
            response_snippet(response).await
        )))
    }
}
