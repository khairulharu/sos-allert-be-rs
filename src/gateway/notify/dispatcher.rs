//! `AlertDispatcher`: push first, then the emergency-contact SMS fallback
//! (`dispatch_notifier.go`).
//!
//! Nothing here fails an SOS request: an unconfigured channel is logged, and
//! only a combination of a failed push **and** a failed SMS returns an error
//! (the worker then retries or dead-letters the event).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::AppResult;
use crate::model::sos::AlertNotice;
use crate::usecase::auth::UserStore;
use crate::usecase::emergency::{EmergencyContactStore, MAX_EMERGENCY_CONTACTS_PER_USER};
use crate::usecase::sos::SosNotifier;

use super::{ApnsNotifier, FcmNotifier, ProviderError, TwilioNotifier, WebPushNotifier};

pub struct AlertDispatcher {
    pub fcm: Option<FcmNotifier>,
    pub apns: Option<ApnsNotifier>,
    pub twilio: Option<TwilioNotifier>,
    /// Browser Web Push; `Arc` because the notifier owns a transport and
    /// subscription store and is not cloned per send.
    pub web: Option<Arc<WebPushNotifier>>,
    pub tokens: Option<Arc<dyn UserStore>>,
    pub contacts: Option<Arc<dyn EmergencyContactStore>>,
}

impl AlertDispatcher {
    /// Push title/body/data shared by the initial fan-out and an escalation.
    pub fn notification_content(notice: &AlertNotice) -> (String, String, HashMap<String, String>) {
        let title = "SOS Alert".to_string();
        let mut body = if notice.emergency_type.is_empty() {
            "Emergency alert nearby. Open the app for the live location.".to_string()
        } else {
            format!(
                "Emergency {} nearby. Open the app for the live location.",
                notice.emergency_type
            )
        };

        if let Some(summary) = Some(notice.summary.trim()).filter(|s| !s.is_empty()) {
            body = format!("{summary} — {body}");
        }
        // The sender's own claim is labelled as such: a responder must never
        // read it as the system's assessment.
        if let Some(claim) = Some(notice.client_summary.trim()).filter(|s| !s.is_empty()) {
            body = format!("Pelapor menyebut: {claim} — {body}");
        }

        let mut data = HashMap::new();
        data.insert("alert_id".into(), notice.alert_id.clone());
        data.insert("sender_id".into(), notice.sender_id.clone());
        data.insert("emergency_type".into(), notice.emergency_type.clone());
        data.insert("latitude".into(), format!("{:.6}", notice.latitude));
        data.insert("longitude".into(), format!("{:.6}", notice.longitude));
        if let Some(severity) = notice.severity.filter(|s| *s > 0.0) {
            data.insert("severity".into(), format!("{severity:.2}"));
        }
        let summary = notice.summary.trim();
        if !summary.is_empty() {
            data.insert("summary".into(), summary.to_string());
        }
        if let Some(client_severity) = notice.client_severity {
            data.insert("client_severity".into(), format!("{client_severity:.2}"));
        }
        let claim = notice.client_summary.trim();
        if !claim.is_empty() {
            data.insert("client_summary".into(), claim.to_string());
        }

        (title, body, data)
    }

    /// `(attempted, failed)`: a channel that is merely unconfigured is not a
    /// failure.
    async fn push_to_recipients(
        &self,
        recipient_ids: &[String],
        notice: &AlertNotice,
    ) -> (bool, bool) {
        let (title, body, data) = Self::notification_content(notice);
        let mut attempted = false;
        let mut failed = false;

        for recipient_id in recipient_ids {
            let mut fcm_token = String::new();
            let mut apns_token = String::new();

            if let Some(tokens) = &self.tokens {
                match tokens.find_by_id(recipient_id).await {
                    Ok(Some(user)) => {
                        fcm_token = user.fcm_token.unwrap_or_default();
                        apns_token = user.apns_token.unwrap_or_default();
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, recipient_id, "could not load push tokens")
                    }
                }
            }

            if let Some(fcm) = &self.fcm {
                attempted = true;
                let result = if fcm_token.is_empty() {
                    fcm.send_to_topic(&format!("user_{recipient_id}"), &title, &body, &data)
                        .await
                } else {
                    fcm.send_to_token(&fcm_token, &title, &body, &data).await
                };
                match result {
                    Ok(()) => {}
                    Err(ProviderError::NotConfigured) => tracing::info!(
                        alert_id = %notice.alert_id,
                        recipient_id,
                        "FCM not configured, log-only"
                    ),
                    Err(e) => {
                        failed = true;
                        tracing::error!(error = %e, recipient_id, "FCM delivery failed")
                    }
                }
            }

            if let (Some(apns), true) = (&self.apns, !apns_token.is_empty()) {
                attempted = true;
                match apns
                    .send(&apns_token, &title, &body, &notice.alert_id)
                    .await
                {
                    Ok(()) => {}
                    Err(ProviderError::NotConfigured) => {
                        tracing::info!(alert_id = %notice.alert_id, "APNs not configured, log-only")
                    }
                    Err(e) => {
                        failed = true;
                        tracing::error!(error = %e, recipient_id, "APNs delivery failed")
                    }
                }
            }

            if let Some(web) = &self.web {
                attempted = true;
                match web.send_to_user(recipient_id, &title, &body, &data).await {
                    Ok(()) => {}
                    Err(ProviderError::NotConfigured) => tracing::info!(
                        alert_id = %notice.alert_id,
                        "web push not configured, log-only"
                    ),
                    Err(e) => {
                        failed = true;
                        tracing::error!(error = %e, recipient_id, "web push delivery failed")
                    }
                }
            }
        }

        (attempted, failed)
    }

    /// SMS to the sender's Tier-1 contacts with a maps link.
    async fn notify_emergency_contacts(&self, notice: &AlertNotice, body: &str) -> AppResult<()> {
        let Some(contacts) = &self.contacts else {
            tracing::info!(alert_id = %notice.alert_id, "no emergency-contact resolver, skipping SMS");
            return Ok(());
        };

        let mut rows = contacts.list_by_user(&notice.sender_id).await?;
        if rows.is_empty() {
            tracing::info!(alert_id = %notice.alert_id, "no emergency contacts configured");
            return Ok(());
        }
        // Rows stored before the cap can be longer: never text more than the
        // limit, so a legacy account cannot become an SMS bomb.
        if rows.len() > MAX_EMERGENCY_CONTACTS_PER_USER {
            tracing::warn!(
                alert_id = %notice.alert_id,
                total = rows.len(),
                cap = MAX_EMERGENCY_CONTACTS_PER_USER,
                "texting only the first contacts"
            );
            rows.truncate(MAX_EMERGENCY_CONTACTS_PER_USER);
        }

        let maps_link = format!(
            "https://www.google.com/maps/search/?api=1&query={:.6},{:.6}",
            notice.latitude, notice.longitude
        );
        let text = format!(
            "SOS ALERT ({}): {} Location: {}",
            notice.emergency_type, body, maps_link
        );

        let Some(twilio) = &self.twilio else {
            for contact in &rows {
                tracing::info!(
                    alert_id = %notice.alert_id,
                    name = %contact.contact_name,
                    phone = %contact.phone_number,
                    "SMS not configured, would text: {text}"
                );
            }
            return Ok(());
        };

        let mut first_error: Option<AppResult<()>> = None;
        for contact in &rows {
            match twilio.send_sms(&contact.phone_number, &text).await {
                Ok(()) => {}
                Err(ProviderError::NotConfigured) => tracing::info!(
                    alert_id = %notice.alert_id,
                    phone = %contact.phone_number,
                    "Twilio not configured, log-only SMS"
                ),
                Err(e) => {
                    tracing::error!(error = %e, phone = %contact.phone_number, "Twilio SMS failed");
                    if first_error.is_none() {
                        first_error = Some(Err(crate::error::AppError::Unavailable(format!(
                            "twilio: {e}"
                        ))));
                    }
                }
            }
        }

        match first_error {
            Some(error) => error,
            None => Ok(()),
        }
    }
}

#[async_trait]
impl SosNotifier for AlertDispatcher {
    async fn notify(&self, recipient_ids: &[String], notice: &AlertNotice) -> AppResult<()> {
        let (_, body, _) = Self::notification_content(notice);
        let (attempted, push_failed) = self.push_to_recipients(recipient_ids, notice).await;
        if recipient_ids.is_empty() {
            tracing::info!(alert_id = %notice.alert_id, "no recipients in range");
        }

        let sms = self.notify_emergency_contacts(notice, &body).await;

        match (attempted && push_failed, sms.is_err()) {
            (true, true) => Err(crate::error::AppError::Unavailable(
                "push delivery failed and SMS fallback failed".into(),
            )),
            _ => Ok(()),
        }
    }

    /// Push-only: the extra responders of an escalation are reached, but the
    /// sender's emergency contacts are not texted twice.
    async fn notify_recipients(
        &self,
        recipient_ids: &[String],
        notice: &AlertNotice,
    ) -> AppResult<()> {
        if recipient_ids.is_empty() {
            return Ok(());
        }
        let (attempted, failed) = self.push_to_recipients(recipient_ids, notice).await;
        if attempted && failed {
            return Err(crate::error::AppError::Unavailable(format!(
                "push delivery failed for {} recipient(s)",
                recipient_ids.len()
            )));
        }
        Ok(())
    }
}
