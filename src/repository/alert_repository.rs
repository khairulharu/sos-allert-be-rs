//! Postgres alert store: alerts plus breadcrumbs, responses and recipients.
//!
//! Every statement is explicit SQL (no ORM): the fan-out and history paths
//! must stay on the indexes added by the migrations.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::domain::{Alert, AlertBreadcrumb, AlertMedia, AlertResponse};
use crate::error::{AppError, AppResult};
use crate::model::sos::{ResponderSummary, SearchAlertHistoryRequest};
use crate::usecase::sos::{AlertStore, AlertUpdate};

const ALERT_COLUMNS: &str = "id, sender_id, longitude, latitude, triggered_at, status, \
     emergency_type, severity, summary, accuracy_meters, battery_percentage, \
     cancellation_reason, resolved_at, cancelled_at, dispatch_due_at, dispatched_at";

const BREADCRUMB_COLUMNS: &str = "id, alert_id, latitude, longitude, speed_mps, heading_deg, \
     altitude_meters, accuracy_meters, battery_percentage, recorded_at";

#[derive(Clone)]
pub struct PgAlertRepository {
    pool: PgPool,
}

impl PgAlertRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Statuses implied by the API filter: empty = all, `active`/`closed` are
    /// the frontend chip shorthands.
    fn statuses_for(filter: &str) -> Vec<String> {
        match filter {
            "" => Vec::new(),
            "active" => vec!["TRIGGERED".into(), "ACKNOWLEDGED".into()],
            "closed" => vec!["RESOLVED".into(), "CANCELLED".into(), "EXPIRED".into()],
            other => vec![other.to_owned()],
        }
    }
}

#[async_trait]
impl AlertStore for PgAlertRepository {
    async fn create(&self, alert: &Alert) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO alerts (id, sender_id, longitude, latitude, triggered_at, status, \
                                 emergency_type, severity, summary, accuracy_meters, \
                                 battery_percentage, dispatch_due_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(&alert.id)
        .bind(&alert.sender_id)
        .bind(alert.longitude)
        .bind(alert.latitude)
        .bind(alert.triggered_at)
        .bind(&alert.status)
        .bind(&alert.emergency_type)
        .bind(alert.severity)
        .bind(&alert.summary)
        .bind(alert.accuracy_meters)
        .bind(alert.battery_percentage)
        .bind(alert.dispatch_due_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_by_id(&self, id: &str) -> AppResult<Option<Alert>> {
        let alert = sqlx::query_as::<_, Alert>(&format!(
            "SELECT {ALERT_COLUMNS} FROM alerts WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(alert)
    }

    async fn list_active_by_sender(&self, sender_id: &str, limit: i64) -> AppResult<Vec<Alert>> {
        let rows = sqlx::query_as::<_, Alert>(&format!(
            "SELECT {ALERT_COLUMNS} FROM alerts \
             WHERE sender_id = $1 AND status IN ('TRIGGERED','ACKNOWLEDGED') \
             ORDER BY triggered_at DESC LIMIT $2"
        ))
        .bind(sender_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn list_inbox(&self, user_id: &str, limit: i64) -> AppResult<Vec<Alert>> {
        let rows = sqlx::query_as::<_, Alert>(&format!(
            "SELECT {ALERT_COLUMNS} FROM alerts \
             JOIN alert_recipients r ON r.alert_id = alerts.id \
             WHERE r.user_id = $1 AND alerts.status IN ('TRIGGERED','ACKNOWLEDGED') \
             ORDER BY alerts.triggered_at DESC LIMIT $2"
        ))
        .bind(user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn transition(&self, id: &str, from: &[&str], to: &AlertUpdate) -> AppResult<bool> {
        let from_statuses: Vec<String> = from.iter().map(|s| (*s).to_owned()).collect();
        let result = sqlx::query(
            "UPDATE alerts SET status = $2, \
                 cancellation_reason = COALESCE($3, cancellation_reason), \
                 cancelled_at = COALESCE($4, cancelled_at), \
                 resolved_at = COALESCE($5, resolved_at) \
             WHERE id = $1 AND status = ANY($6)",
        )
        .bind(id)
        .bind(&to.status)
        .bind(&to.cancellation_reason)
        .bind(to.cancelled_at)
        .bind(to.resolved_at)
        .bind(&from_statuses)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn count_active(&self) -> AppResult<i64> {
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM alerts WHERE status IN ('TRIGGERED','ACKNOWLEDGED')",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(total)
    }

    async fn expire_older_than(&self, before_ms: i64) -> AppResult<u64> {
        let result = sqlx::query(
            "UPDATE alerts SET status = 'EXPIRED', cancelled_at = NULL, resolved_at = NULL \
             WHERE status = 'TRIGGERED' AND triggered_at < $1",
        )
        .bind(before_ms)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn purge_breadcrumbs(&self, before_ms: i64) -> AppResult<u64> {
        let result = sqlx::query(
            "DELETE FROM alert_breadcrumbs b USING alerts a \
             WHERE b.alert_id = a.id AND b.recorded_at < $1 \
               AND a.status IN ('RESOLVED','CANCELLED','EXPIRED')",
        )
        .bind(before_ms)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn list_due_dispatch(&self, due_before_ms: i64, limit: i64) -> AppResult<Vec<Alert>> {
        let rows = sqlx::query_as::<_, Alert>(&format!(
            "SELECT {ALERT_COLUMNS} FROM alerts \
             WHERE status = 'TRIGGERED' AND dispatch_due_at IS NOT NULL \
               AND dispatched_at IS NULL AND dispatch_due_at <= $1 \
             ORDER BY dispatch_due_at ASC LIMIT $2"
        ))
        .bind(due_before_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn mark_dispatched(&self, alert_id: &str, at_ms: i64) -> AppResult<bool> {
        let result = sqlx::query(
            "UPDATE alerts SET dispatched_at = $1 WHERE id = $2 AND dispatched_at IS NULL",
        )
        .bind(at_ms)
        .bind(alert_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn create_breadcrumb(&self, crumb: &AlertBreadcrumb) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO alert_breadcrumbs \
                (alert_id, latitude, longitude, speed_mps, heading_deg, altitude_meters, \
                 accuracy_meters, battery_percentage, recorded_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(&crumb.alert_id)
        .bind(crumb.latitude)
        .bind(crumb.longitude)
        .bind(crumb.speed_mps)
        .bind(crumb.heading_deg)
        .bind(crumb.altitude_meters)
        .bind(crumb.accuracy_meters)
        .bind(crumb.battery_percentage)
        .bind(crumb.recorded_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_breadcrumbs(
        &self,
        alert_id: &str,
        limit: i64,
    ) -> AppResult<Vec<AlertBreadcrumb>> {
        let rows = sqlx::query_as::<_, AlertBreadcrumb>(&format!(
            "SELECT {BREADCRUMB_COLUMNS} FROM alert_breadcrumbs \
             WHERE alert_id = $1 ORDER BY recorded_at ASC, id ASC LIMIT $2"
        ))
        .bind(alert_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn latest_breadcrumb(&self, alert_id: &str) -> AppResult<Option<AlertBreadcrumb>> {
        let row = sqlx::query_as::<_, AlertBreadcrumb>(&format!(
            "SELECT {BREADCRUMB_COLUMNS} FROM alert_breadcrumbs \
             WHERE alert_id = $1 ORDER BY recorded_at DESC, id DESC LIMIT 1"
        ))
        .bind(alert_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn create_response(&self, response: &AlertResponse) -> AppResult<()> {
        // The unique (alert_id, responder_id) index turns a repeat answer into
        // a no-op reported as a 409, not a duplicate row or a 500.
        let result = sqlx::query(
            "INSERT INTO alert_responses \
                (id, alert_id, responder_id, responder_latitude, responder_longitude, \
                 distance_meters, eta_seconds, responded_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (alert_id, responder_id) DO NOTHING",
        )
        .bind(&response.id)
        .bind(&response.alert_id)
        .bind(&response.responder_id)
        .bind(response.responder_latitude)
        .bind(response.responder_longitude)
        .bind(response.distance_meters)
        .bind(response.eta_seconds)
        .bind(response.responded_at)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(AppError::Conflict(
                "responder has already responded to this alert".into(),
            ));
        }
        Ok(())
    }

    async fn is_responder(&self, alert_id: &str, user_id: &str) -> AppResult<bool> {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM alert_responses WHERE alert_id = $1 AND responder_id = $2",
        )
        .bind(alert_id)
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }

    async fn create_recipients(
        &self,
        alert_id: &str,
        user_ids: &[String],
        notified_at: i64,
    ) -> AppResult<()> {
        if user_ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO alert_recipients (alert_id, user_id, notified_at) \
             SELECT $1, unnest($2::text[]), $3 \
             ON CONFLICT (alert_id, user_id) DO NOTHING",
        )
        .bind(alert_id)
        .bind(user_ids)
        .bind(notified_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_recipients(&self, alert_id: &str) -> AppResult<Vec<String>> {
        let rows: Vec<String> =
            sqlx::query_scalar("SELECT user_id FROM alert_recipients WHERE alert_id = $1")
                .bind(alert_id)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
    }

    async fn history(&self, request: &SearchAlertHistoryRequest) -> AppResult<(Vec<Alert>, i64)> {
        let statuses = Self::statuses_for(&request.status);
        let offset = (request.page.max(1) - 1) * request.size.clamp(1, 100);
        let role_received = request.role == "received";

        let join = if role_received {
            "JOIN alert_recipients r ON r.alert_id = alerts.id"
        } else {
            ""
        };
        let owner = if role_received {
            "r.user_id = $1"
        } else {
            "alerts.sender_id = $1"
        };

        let sql = format!(
            "SELECT {ALERT_COLUMNS} FROM alerts {join} \
             WHERE {owner} \
               AND (cardinality($2::text[]) = 0 OR alerts.status = ANY($2)) \
               AND ($3::bigint IS NULL OR alerts.triggered_at >= $3) \
               AND ($4::bigint IS NULL OR alerts.triggered_at <= $4) \
             ORDER BY alerts.triggered_at DESC, alerts.id DESC \
             LIMIT $5 OFFSET $6"
        );

        let rows = sqlx::query_as::<_, Alert>(&sql)
            .bind(&request.user_id)
            .bind(&statuses)
            .bind(request.from_ms)
            .bind(request.to_ms)
            .bind(request.size.clamp(1, 100))
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;

        let count_sql = format!(
            "SELECT count(*) FROM alerts {join} \
             WHERE {owner} \
               AND (cardinality($2::text[]) = 0 OR alerts.status = ANY($2)) \
               AND ($3::bigint IS NULL OR alerts.triggered_at >= $3) \
               AND ($4::bigint IS NULL OR alerts.triggered_at <= $4)"
        );
        let total: i64 = sqlx::query_scalar(&count_sql)
            .bind(&request.user_id)
            .bind(&statuses)
            .bind(request.from_ms)
            .bind(request.to_ms)
            .fetch_one(&self.pool)
            .await?;

        Ok((rows, total))
    }

    async fn summary_counts(
        &self,
        user_id: &str,
    ) -> AppResult<(HashMap<String, i64>, HashMap<String, i64>, i64, i64)> {
        // One statement for every card, like the Go `SummaryCounts`.
        let rows = sqlx::query(
            "SELECT 'sent' AS kind, status AS status, COUNT(*) AS total \
               FROM alerts WHERE sender_id = $1 GROUP BY status \
             UNION ALL \
             SELECT 'received', alerts.status, COUNT(*) \
               FROM alerts JOIN alert_recipients r ON r.alert_id = alerts.id \
              WHERE r.user_id = $2 GROUP BY alerts.status \
             UNION ALL \
             SELECT 'responded', '', COUNT(DISTINCT alert_id) \
               FROM alert_responses WHERE responder_id = $3 \
             UNION ALL \
             SELECT 'responded_resolved', '', COUNT(DISTINCT r.alert_id) \
               FROM alert_responses r JOIN alerts a ON a.id = r.alert_id \
              WHERE r.responder_id = $4 AND a.status = 'RESOLVED'",
        )
        .bind(user_id)
        .bind(user_id)
        .bind(user_id)
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        let mut sent = HashMap::new();
        let mut received = HashMap::new();
        let mut responded_total = 0;
        let mut responded_resolved = 0;
        for row in rows {
            let kind: String = row.try_get("kind")?;
            let status: String = row.try_get("status")?;
            let total: i64 = row.try_get("total")?;
            match kind.as_str() {
                "sent" => {
                    sent.insert(status, total);
                }
                "received" => {
                    received.insert(status, total);
                }
                "responded" => responded_total = total,
                "responded_resolved" => responded_resolved = total,
                _ => {}
            }
        }

        Ok((sent, received, responded_total, responded_resolved))
    }

    async fn count_responders_batch(
        &self,
        alert_ids: &[String],
    ) -> AppResult<HashMap<String, i64>> {
        let mut out = HashMap::with_capacity(alert_ids.len());
        if alert_ids.is_empty() {
            return Ok(out);
        }
        let rows = sqlx::query(
            "SELECT alert_id, COUNT(DISTINCT responder_id) AS total \
             FROM alert_responses WHERE alert_id = ANY($1) GROUP BY alert_id",
        )
        .bind(alert_ids)
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let alert_id: String = row.try_get("alert_id")?;
            let total: i64 = row.try_get("total")?;
            out.insert(alert_id, total);
        }
        Ok(out)
    }

    async fn latest_breadcrumbs_batch(
        &self,
        alert_ids: &[String],
    ) -> AppResult<HashMap<String, AlertBreadcrumb>> {
        let mut out = HashMap::with_capacity(alert_ids.len());
        if alert_ids.is_empty() {
            return Ok(out);
        }
        let rows = sqlx::query_as::<_, AlertBreadcrumb>(
            "SELECT DISTINCT ON (alert_id) id, alert_id, latitude, longitude, speed_mps, \
                    heading_deg, altitude_meters, accuracy_meters, battery_percentage, recorded_at \
             FROM alert_breadcrumbs WHERE alert_id = ANY($1) \
             ORDER BY alert_id, recorded_at DESC, id DESC",
        )
        .bind(alert_ids)
        .fetch_all(&self.pool)
        .await?;
        for crumb in rows {
            out.insert(crumb.alert_id.clone(), crumb);
        }
        Ok(out)
    }

    async fn responded_ids(
        &self,
        user_id: &str,
        alert_ids: &[String],
    ) -> AppResult<HashSet<String>> {
        let mut out = HashSet::with_capacity(alert_ids.len());
        if alert_ids.is_empty() {
            return Ok(out);
        }
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT alert_id FROM alert_responses \
             WHERE responder_id = $1 AND alert_id = ANY($2)",
        )
        .bind(user_id)
        .bind(alert_ids)
        .fetch_all(&self.pool)
        .await?;
        out.extend(rows);
        Ok(out)
    }

    async fn responder_summaries_batch(
        &self,
        alert_ids: &[String],
    ) -> AppResult<HashMap<String, Vec<ResponderSummary>>> {
        let mut out: HashMap<String, Vec<ResponderSummary>> = HashMap::new();
        if alert_ids.is_empty() {
            return Ok(out);
        }

        let rows = sqlx::query(
            "SELECT r.alert_id, r.responder_id, COALESCE(u.name, '') AS name, \
                    r.distance_meters, r.eta_seconds, r.responded_at \
             FROM alert_responses r LEFT JOIN users u ON u.id = r.responder_id \
             WHERE r.alert_id = ANY($1) \
             ORDER BY r.alert_id ASC, r.responded_at ASC",
        )
        .bind(alert_ids)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let alert_id: String = row.try_get("alert_id")?;
            let distance: f64 = row.try_get("distance_meters")?;
            out.entry(alert_id).or_default().push(ResponderSummary {
                user_id: row.try_get("responder_id")?,
                name: row.try_get("name")?,
                distance_m: crate::geo::quantize_distance_meters(distance),
                eta_seconds: row.try_get::<i32, _>("eta_seconds")? as i64,
                responded_at: row.try_get("responded_at")?,
            });
        }
        Ok(out)
    }

    async fn update_triage(
        &self,
        alert_id: &str,
        emergency_type: &str,
        severity: Option<f64>,
        summary: &str,
    ) -> AppResult<()> {
        sqlx::query(
            "UPDATE alerts SET \
                 emergency_type = CASE WHEN $2 = '' THEN emergency_type ELSE $2 END, \
                 severity = COALESCE($3, severity), \
                 summary = CASE WHEN $4 = '' THEN summary ELSE $4 END \
             WHERE id = $1",
        )
        .bind(alert_id)
        .bind(emergency_type)
        .bind(severity)
        .bind(summary)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn create_media(&self, media: &AlertMedia) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO alert_media \
                (id, alert_id, filename, content_type, size_bytes, storage_path, uploaded_by, \
                 uploaded_at, transcript, transcription_status, transcribed_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(&media.id)
        .bind(&media.alert_id)
        .bind(&media.filename)
        .bind(&media.content_type)
        .bind(media.size_bytes)
        .bind(&media.storage_path)
        .bind(&media.uploaded_by)
        .bind(media.uploaded_at)
        .bind(&media.transcript)
        .bind(&media.transcription_status)
        .bind(media.transcribed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_media(&self, alert_id: &str) -> AppResult<Vec<AlertMedia>> {
        let rows = sqlx::query_as::<_, AlertMedia>(&format!(
            "SELECT {MEDIA_COLUMNS} FROM alert_media WHERE alert_id = $1 ORDER BY uploaded_at ASC"
        ))
        .bind(alert_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_media(&self, alert_id: &str, media_id: &str) -> AppResult<Option<AlertMedia>> {
        let row = sqlx::query_as::<_, AlertMedia>(&format!(
            "SELECT {MEDIA_COLUMNS} FROM alert_media WHERE id = $1 AND alert_id = $2"
        ))
        .bind(media_id)
        .bind(alert_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn set_media_transcription(
        &self,
        media_id: &str,
        transcript: Option<&str>,
        status: &str,
        transcribed_at: Option<i64>,
    ) -> AppResult<()> {
        sqlx::query(
            "UPDATE alert_media SET transcript = COALESCE($2, transcript), \
                 transcription_status = $3, transcribed_at = COALESCE($4, transcribed_at) \
             WHERE id = $1",
        )
        .bind(media_id)
        .bind(transcript)
        .bind(status)
        .bind(transcribed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

const MEDIA_COLUMNS: &str = "id, alert_id, filename, content_type, size_bytes, storage_path, \
     uploaded_by, uploaded_at, transcript, transcription_status, transcribed_at";
