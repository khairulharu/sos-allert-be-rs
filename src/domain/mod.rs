//! Persistence entities, one module per table.
//!
//! Columns match `migrations/` exactly (Postgres names). Timestamps are
//! millisecond `bigint`, the same convention the Go service used, so existing
//! data can be moved without a timezone conversion step.

pub mod address;
pub mod alert;
pub mod contact;
pub mod location;
pub mod push;
pub mod user;

pub use address::Address;
pub use alert::{
    Alert, AlertBreadcrumb, AlertMedia, AlertRecipient, AlertResponse, EmergencyContact,
};
pub use contact::Contact;
pub use location::Location;
pub use push::PushSubscription;
pub use user::User;
