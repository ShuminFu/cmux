//! Structured diagnostics shared by every stage of the pipeline.
//!
//! A diagnostic carries a stable `code`, a severity, a human message, an
//! optional JSON `subject` that names what it is about, and the fixes the
//! tool knows how to apply. Diagnostics are the only way a stage reports a
//! problem; nothing prints ad hoc text.

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
}

#[derive(Debug, Clone, Serialize)]
pub struct Diagnostic {
    pub code: String,
    pub severity: Severity,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<Value>,
    #[serde(rename = "supportedFixes", skip_serializing_if = "Vec::is_empty")]
    pub supported_fixes: Vec<String>,
}

impl Diagnostic {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            severity: Severity::Error,
            message: message.into(),
            subject: None,
            supported_fixes: Vec::new(),
        }
    }

    pub fn warning(code: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            ..Self::error(code, message)
        }
    }

    pub fn subject(mut self, subject: Value) -> Self {
        self.subject = Some(subject);
        self
    }

    pub fn fix(mut self, fix: impl Into<String>) -> Self {
        self.supported_fixes.push(fix.into());
        self
    }

    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }
}

/// One named artifact check in a validation receipt.
#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    pub details: Vec<String>,
}

impl Check {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ok: true,
            details: Vec::new(),
        }
    }

    pub fn fail(&mut self, detail: impl Into<String>) {
        self.ok = false;
        self.details.push(detail.into());
    }
}
