//! Typed diagram specifications and their schema-level validation.
//!
//! Every field mirrors the archify JSON contract: `additionalProperties:
//! false` is enforced through `deny_unknown_fields`, enums are real enums,
//! and everything the layout later relies on (unique ids, resolvable
//! references, column budget, view limits) is checked here so the layout
//! stages can assume a well-formed document.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::diag::Diagnostic;

pub const MAX_WORKFLOW_COL: u32 = 5;
pub const MAX_VIEWS: usize = 5;
pub const MAX_SOURCES: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagramType {
    Architecture,
    Workflow,
}

impl DiagramType {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "architecture" => Some(Self::Architecture),
            "workflow" => Some(Self::Workflow),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Architecture => "architecture",
            Self::Workflow => "workflow",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quality {
    Standard,
    Showcase,
}

impl Quality {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "standard" => Some(Self::Standard),
            "showcase" => Some(Self::Showcase),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Showcase => "showcase",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ComponentType {
    Frontend,
    Backend,
    Database,
    Cloud,
    Security,
    Messagebus,
    External,
}

impl ComponentType {
    pub const ALL: [ComponentType; 7] = [
        Self::Frontend,
        Self::Backend,
        Self::Database,
        Self::Cloud,
        Self::Security,
        Self::Messagebus,
        Self::External,
    ];

    pub fn key(self) -> &'static str {
        match self {
            Self::Frontend => "frontend",
            Self::Backend => "backend",
            Self::Database => "database",
            Self::Cloud => "cloud",
            Self::Security => "security",
            Self::Messagebus => "messagebus",
            Self::External => "external",
        }
    }

    pub fn default_label(self) -> &'static str {
        match self {
            Self::Frontend => "Frontend",
            Self::Backend => "Backend",
            Self::Database => "Database",
            Self::Cloud => "Cloud",
            Self::Security => "Security",
            Self::Messagebus => "Message bus",
            Self::External => "External",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Variant {
    #[default]
    Default,
    Emphasis,
    Security,
    Dashed,
}

impl Variant {
    pub fn key(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Emphasis => "emphasis",
            Self::Security => "security",
            Self::Dashed => "dashed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CardDot {
    Cyan,
    Emerald,
    Violet,
    Amber,
    Rose,
    Orange,
    Slate,
}

impl CardDot {
    pub fn key(self) -> &'static str {
        match self {
            Self::Cyan => "cyan",
            Self::Emerald => "emerald",
            Self::Violet => "violet",
            Self::Amber => "amber",
            Self::Rose => "rose",
            Self::Orange => "orange",
            Self::Slate => "slate",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Card {
    pub dot: CardDot,
    pub title: String,
    pub items: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct View {
    pub id: String,
    pub label: String,
    pub focus: Vec<String>,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LegendMode {
    #[default]
    Auto,
    All,
    Hidden,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegendEntry {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub visible: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Legend {
    #[serde(default)]
    pub mode: Option<LegendMode>,
    #[serde(default)]
    pub entries: Option<BTreeMap<String, LegendEntry>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Repository {
    pub url: String,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub link_mode: Option<String>,
    pub revision: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub path: String,
    #[serde(default)]
    pub line: Option<u32>,
    #[serde(default)]
    pub end_line: Option<u32>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Meta {
    pub title: String,
    #[serde(default)]
    pub locale: Option<String>,
    #[serde(default)]
    pub subtitle: Option<String>,
    #[serde(default)]
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub output: Option<String>,
    #[serde(default)]
    pub animation: Option<String>,
    #[serde(default)]
    pub visual_preset: Option<String>,
    #[serde(default)]
    pub quality_profile: Option<String>,
    #[serde(default)]
    pub engineering_profile: Option<String>,
    #[serde(default)]
    pub repository: Option<Repository>,
    #[serde(default)]
    pub views: Option<Vec<View>>,
    #[serde(default)]
    pub legend: Option<Legend>,
    #[serde(default, rename = "viewBox")]
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub view_box: Option<Value>,
}

// ---------------------------------------------------------------- architecture

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Component {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: ComponentType,
    pub label: String,
    #[serde(default)]
    pub sublabel: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub brand: Option<Value>,
    #[serde(default)]
    pub sources: Option<Vec<Source>>,
    #[serde(default)]
    pub row: Option<u32>,
    #[serde(default)]
    pub col: Option<u32>,
    #[serde(default)]
    pub pos: Option<[f64; 2]>,
    #[serde(default)]
    pub size: Option<[f64; 2]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum BoundaryKind {
    #[serde(rename = "region")]
    Region,
    #[serde(rename = "security-group")]
    SecurityGroup,
}

impl BoundaryKind {
    pub fn key(self) -> &'static str {
        match self {
            Self::Region => "region",
            Self::SecurityGroup => "security-group",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Boundary {
    pub kind: BoundaryKind,
    pub label: String,
    pub wraps: Vec<String>,
    #[serde(default)]
    pub pad: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    #[serde(default)]
    pub id: Option<String>,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub variant: Option<Variant>,
    #[serde(default, rename = "fromSide")]
    pub from_side: Option<String>,
    #[serde(default, rename = "toSide")]
    pub to_side: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
    #[serde(default)]
    pub via: Option<Vec<[f64; 2]>>,
    #[serde(default, rename = "labelAt")]
    pub label_at: Option<[f64; 2]>,
    #[serde(default, rename = "labelDx")]
    pub label_dx: Option<f64>,
    #[serde(default, rename = "labelDy")]
    pub label_dy: Option<f64>,
    #[serde(default, rename = "labelSegment")]
    pub label_segment: Option<usize>,
    #[serde(default)]
    pub width: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchitectureSpec {
    pub schema_version: u32,
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub diagram_type: String,
    pub meta: Meta,
    #[serde(default)]
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub layout: Option<Value>,
    pub components: Vec<Component>,
    #[serde(default)]
    pub boundaries: Option<Vec<Boundary>>,
    #[serde(default)]
    pub connections: Option<Vec<Connection>>,
    #[serde(default)]
    pub cards: Option<Vec<Card>>,
}

// -------------------------------------------------------------------- workflow

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lane {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Phase {
    pub id: String,
    pub label: String,
    #[serde(rename = "fromCol")]
    pub from_col: u32,
    #[serde(rename = "toCol")]
    pub to_col: u32,
    #[serde(default)]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub id: String,
    pub label: String,
    pub lane: String,
    #[serde(rename = "fromCol")]
    pub from_col: u32,
    #[serde(rename = "toCol")]
    pub to_col: u32,
    #[serde(default)]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowNode {
    pub id: String,
    pub lane: String,
    pub col: u32,
    #[serde(rename = "type")]
    pub kind: ComponentType,
    pub label: String,
    #[serde(default)]
    pub sublabel: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub brand: Option<Value>,
    #[serde(default)]
    pub width: Option<f64>,
    #[serde(default)]
    pub height: Option<f64>,
    #[serde(default, rename = "yOffset")]
    pub y_offset: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Main,
    Branch,
    Async,
    Return,
    Error,
}

impl Role {
    pub fn key(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Branch => "branch",
            Self::Async => "async",
            Self::Return => "return",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowEdge {
    #[serde(default)]
    pub id: Option<String>,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub variant: Option<Variant>,
    #[serde(default)]
    pub role: Option<Role>,
    #[serde(default, rename = "fromSide")]
    pub from_side: Option<String>,
    #[serde(default, rename = "toSide")]
    pub to_side: Option<String>,
    #[serde(default)]
    pub route: Option<String>,
    #[serde(default)]
    pub via: Option<Vec<[f64; 2]>>,
    #[serde(default, rename = "labelAt")]
    pub label_at: Option<[f64; 2]>,
    #[serde(default, rename = "labelDx")]
    pub label_dx: Option<f64>,
    #[serde(default, rename = "labelDy")]
    pub label_dy: Option<f64>,
    #[serde(default, rename = "labelSegment")]
    pub label_segment: Option<usize>,
    #[serde(default, rename = "channelX")]
    pub channel_x: Option<f64>,
    #[serde(default, rename = "channelY")]
    pub channel_y: Option<f64>,
    #[serde(default)]
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub bias: Option<f64>,
    #[serde(default)]
    pub width: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSpec {
    pub schema_version: u32,
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub diagram_type: String,
    pub meta: Meta,
    pub lanes: Vec<Lane>,
    #[serde(default)]
    pub phases: Option<Vec<Phase>>,
    #[serde(default)]
    pub groups: Option<Vec<Group>>,
    #[serde(default, rename = "mainPath")]
    pub main_path: Option<Vec<String>>,
    #[serde(default, rename = "semanticChecks")]
    #[allow(dead_code)] // accepted by the contract; not consumed by layout
    pub semantic_checks: Option<Value>,
    pub nodes: Vec<WorkflowNode>,
    #[serde(default)]
    pub edges: Option<Vec<WorkflowEdge>>,
    #[serde(default)]
    pub cards: Option<Vec<Card>>,
}

#[derive(Debug, Clone)]
pub enum Spec {
    Architecture(ArchitectureSpec),
    Workflow(WorkflowSpec),
}

impl Spec {
    pub fn meta(&self) -> &Meta {
        match self {
            Spec::Architecture(s) => &s.meta,
            Spec::Workflow(s) => &s.meta,
        }
    }

    pub fn diagram_type(&self) -> DiagramType {
        match self {
            Spec::Architecture(_) => DiagramType::Architecture,
            Spec::Workflow(_) => DiagramType::Workflow,
        }
    }

    /// Quality profile the document asks for, falling back to the CLI choice.
    pub fn quality(&self, cli: Option<Quality>) -> Quality {
        cli.or_else(|| {
            self.meta()
                .quality_profile
                .as_deref()
                .and_then(Quality::parse)
        })
        .unwrap_or(Quality::Standard)
    }

    pub fn declares_evidence(&self) -> bool {
        match self {
            Spec::Architecture(s) => {
                s.meta.repository.is_some()
                    || s.components
                        .iter()
                        .any(|c| c.sources.as_ref().is_some_and(|v| !v.is_empty()))
            }
            Spec::Workflow(_) => false,
        }
    }
}

/// Read, parse, and schema-validate a specification file.
pub fn load(path: &Path, expected: DiagramType) -> Result<(Spec, Vec<u8>), Vec<Diagnostic>> {
    let bytes = fs::read(path).map_err(|e| {
        vec![
            Diagnostic::error("io/read", format!("cannot read {}: {e}", path.display()))
                .fix("pass a readable specification path"),
        ]
    })?;
    let raw: Value = serde_json::from_slice(&bytes).map_err(|e| {
        vec![Diagnostic::error(
            "schema/json",
            format!("invalid JSON: {e}"),
        )]
    })?;
    let declared = raw
        .get("diagram_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if declared != expected.name() {
        return Err(vec![
            Diagnostic::error(
                "schema/diagram-type",
                format!(
                    "diagram_type is {declared:?}; this command validates {:?}",
                    expected.name()
                ),
            )
            .subject(json!({ "path": "/diagram_type" }))
            .fix("run the command for the declared diagram type or fix diagram_type"),
        ]);
    }
    let spec = match expected {
        DiagramType::Architecture => {
            serde_json::from_value::<ArchitectureSpec>(raw).map(Spec::Architecture)
        }
        DiagramType::Workflow => serde_json::from_value::<WorkflowSpec>(raw).map(Spec::Workflow),
    }
    .map_err(|e| {
        vec![
            Diagnostic::error(
                "schema/invalid",
                format!("{} schema validation failed: {e}", expected.name()),
            )
            .fix("compare the field against the schema for this diagram type"),
        ]
    })?;
    let diags = validate_semantics(&spec);
    if diags.iter().any(Diagnostic::is_error) {
        return Err(diags);
    }
    Ok((spec, bytes))
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
}

fn check_meta(
    meta: &Meta,
    allow_repository: bool,
    node_ids: &HashSet<&str>,
    out: &mut Vec<Diagnostic>,
) {
    if meta.title.trim().is_empty() {
        out.push(
            Diagnostic::error("schema/invalid", "meta.title must not be empty")
                .subject(json!({ "path": "/meta/title" })),
        );
    }
    if let Some(locale) = &meta.locale
        && locale != "en"
        && locale != "zh-CN"
    {
        out.push(
            Diagnostic::error(
                "schema/invalid",
                format!("meta.locale {locale:?} is not a supported viewer locale"),
            )
            .subject(json!({ "path": "/meta/locale" }))
            .fix("use \"en\" or \"zh-CN\", or omit meta.locale"),
        );
    }
    if let Some(a) = &meta.animation
        && a != "trace"
        && a != "none"
    {
        out.push(
            Diagnostic::error(
                "schema/invalid",
                format!("meta.animation {a:?} must be \"trace\" or \"none\""),
            )
            .subject(json!({ "path": "/meta/animation" })),
        );
    }
    if let Some(p) = &meta.visual_preset
        && !matches!(
            p.as_str(),
            "classic" | "signal-flow" | "blueprint" | "editorial"
        )
    {
        out.push(
            Diagnostic::error(
                "schema/invalid",
                format!("meta.visual_preset {p:?} is unknown"),
            )
            .subject(json!({ "path": "/meta/visual_preset" })),
        );
    }
    if let Some(q) = &meta.quality_profile
        && Quality::parse(q).is_none()
    {
        out.push(
            Diagnostic::error(
                "schema/invalid",
                format!("meta.quality_profile {q:?} must be \"standard\" or \"showcase\""),
            )
            .subject(json!({ "path": "/meta/quality_profile" })),
        );
    }
    if !allow_repository && meta.repository.is_some() {
        out.push(
            Diagnostic::error(
                "schema/invalid",
                "meta.repository is only supported on architecture diagrams",
            )
            .subject(json!({ "path": "/meta/repository" })),
        );
    }
    if !allow_repository && meta.engineering_profile.is_some() {
        out.push(
            Diagnostic::error(
                "schema/invalid",
                "meta.engineering_profile is only supported on architecture diagrams",
            )
            .subject(json!({ "path": "/meta/engineering_profile" })),
        );
    }
    if let Some(repo) = &meta.repository {
        let rev_ok =
            repo.revision.len() == 40 && repo.revision.chars().all(|c| c.is_ascii_hexdigit());
        if !rev_ok {
            out.push(
                Diagnostic::error(
                    "schema/invalid",
                    "meta.repository.revision must be a 40-hex commit id",
                )
                .subject(json!({ "path": "/meta/repository/revision" })),
            );
        }
        if let Some(p) = &repo.provider
            && p != "github"
            && p != "gitee"
        {
            out.push(
                Diagnostic::error(
                    "schema/invalid",
                    format!("meta.repository.provider {p:?} must be github or gitee"),
                )
                .subject(json!({ "path": "/meta/repository/provider" })),
            );
        }
        if let Some(m) = &repo.link_mode
            && m != "web"
            && m != "local-only"
        {
            out.push(
                Diagnostic::error(
                    "schema/invalid",
                    format!("meta.repository.link_mode {m:?} must be web or local-only"),
                )
                .subject(json!({ "path": "/meta/repository/link_mode" })),
            );
        }
    }
    if let Some(views) = &meta.views {
        if views.len() > MAX_VIEWS {
            out.push(
                Diagnostic::error(
                    "schema/invalid",
                    format!(
                        "meta.views holds {} chapters; at most {MAX_VIEWS} are allowed",
                        views.len()
                    ),
                )
                .subject(json!({ "path": "/meta/views" })),
            );
        }
        let mut seen = HashSet::new();
        for (i, v) in views.iter().enumerate() {
            if !seen.insert(v.id.as_str()) {
                out.push(
                    Diagnostic::error("schema/invalid", format!("duplicate view id {:?}", v.id))
                        .subject(json!({ "path": format!("/meta/views/{i}/id") })),
                );
            }
            if v.focus.is_empty() {
                out.push(
                    Diagnostic::error("schema/invalid", format!("view {:?} focuses nothing", v.id))
                        .subject(json!({ "path": format!("/meta/views/{i}/focus") })),
                );
            }
            for (j, f) in v.focus.iter().enumerate() {
                if !node_ids.contains(f.as_str()) {
                    out.push(
                        Diagnostic::error(
                            "schema/reference",
                            format!("view {:?} focuses unknown node {f:?}", v.id),
                        )
                        .subject(json!({ "path": format!("/meta/views/{i}/focus/{j}") }))
                        .fix("reference an existing node id"),
                    );
                }
            }
        }
    }
    if let Some(legend) = &meta.legend
        && let Some(entries) = &legend.entries
    {
        for key in entries.keys() {
            if !ComponentType::ALL.iter().any(|t| t.key() == key) {
                out.push(
                    Diagnostic::error(
                        "schema/invalid",
                        format!("meta.legend.entries.{key} is not a component type"),
                    )
                    .subject(json!({ "path": format!("/meta/legend/entries/{key}") })),
                );
            }
        }
    }
}

fn check_side(value: &Option<String>, path: String, out: &mut Vec<Diagnostic>) {
    if let Some(s) = value
        && crate::geom::Side::parse(s).is_none()
    {
        out.push(
            Diagnostic::error(
                "schema/invalid",
                format!("{path} must be left, right, top, or bottom (got {s:?})"),
            )
            .subject(json!({ "path": path })),
        );
    }
}

fn check_cards(cards: &Option<Vec<Card>>, out: &mut Vec<Diagnostic>) {
    if let Some(cards) = cards {
        for (i, c) in cards.iter().enumerate() {
            if c.title.trim().is_empty() {
                out.push(
                    Diagnostic::error("schema/invalid", "card title must not be empty")
                        .subject(json!({ "path": format!("/cards/{i}/title") })),
                );
            }
        }
    }
}

fn check_sources(sources: &Option<Vec<Source>>, base: &str, out: &mut Vec<Diagnostic>) {
    if let Some(sources) = sources {
        if sources.is_empty() || sources.len() > MAX_SOURCES {
            out.push(
                Diagnostic::error(
                    "schema/invalid",
                    format!("{base}/sources must hold 1..{MAX_SOURCES} entries"),
                )
                .subject(json!({ "path": format!("{base}/sources") })),
            );
        }
        for (i, s) in sources.iter().enumerate() {
            if s.path.is_empty()
                || s.path.len() > 240
                || s.path.starts_with('/')
                || s.path.contains("..")
            {
                out.push(
                    Diagnostic::error(
                        "schema/invalid",
                        format!("{base}/sources/{i}/path must be a relative repository path"),
                    )
                    .subject(json!({ "path": format!("{base}/sources/{i}/path") })),
                );
            }
            if let Some(l) = &s.label
                && (l.is_empty() || l.len() > 48)
            {
                out.push(
                    Diagnostic::error(
                        "schema/invalid",
                        format!("{base}/sources/{i}/label must be 1..48 chars"),
                    )
                    .subject(json!({ "path": format!("{base}/sources/{i}/label") })),
                );
            }
            if let (Some(a), Some(b)) = (s.line, s.end_line)
                && b < a
            {
                out.push(
                    Diagnostic::error(
                        "schema/invalid",
                        format!("{base}/sources/{i}/end_line precedes line"),
                    )
                    .subject(json!({ "path": format!("{base}/sources/{i}/end_line") })),
                );
            }
        }
    }
}

pub fn validate_semantics(spec: &Spec) -> Vec<Diagnostic> {
    let mut out = Vec::new();
    match spec {
        Spec::Architecture(s) => {
            if s.schema_version != 1 {
                out.push(
                    Diagnostic::error("schema/invalid", "architecture schema_version must be 1")
                        .subject(json!({ "path": "/schema_version" })),
                );
            }
            let mut ids: HashSet<&str> = HashSet::new();
            for (i, c) in s.components.iter().enumerate() {
                if !valid_id(&c.id) {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("component id {:?} is not a valid id", c.id),
                        )
                        .subject(json!({ "path": format!("/components/{i}/id") })),
                    );
                }
                if !ids.insert(c.id.as_str()) {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("duplicate component id {:?}", c.id),
                        )
                        .subject(json!({ "path": format!("/components/{i}/id") })),
                    );
                }
                if c.label.trim().is_empty() {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("component {:?} has an empty label", c.id),
                        )
                        .subject(json!({ "path": format!("/components/{i}/label") })),
                    );
                }
                if let Some(sz) = c.size
                    && (sz[0] <= 0.0 || sz[1] <= 0.0)
                {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("component {:?} size must be positive", c.id),
                        )
                        .subject(json!({ "path": format!("/components/{i}/size") })),
                    );
                }
                check_sources(&c.sources, &format!("/components/{i}"), &mut out);
            }
            if s.components.is_empty() {
                out.push(
                    Diagnostic::error("schema/invalid", "components must hold at least one entry")
                        .subject(json!({ "path": "/components" })),
                );
            }
            check_meta(&s.meta, true, &ids, &mut out);
            if let Some(bs) = &s.boundaries {
                for (i, b) in bs.iter().enumerate() {
                    if b.wraps.is_empty() {
                        out.push(
                            Diagnostic::error(
                                "schema/invalid",
                                format!("boundary {:?} wraps nothing", b.label),
                            )
                            .subject(json!({ "path": format!("/boundaries/{i}/wraps") })),
                        );
                    }
                    for (j, w) in b.wraps.iter().enumerate() {
                        if !ids.contains(w.as_str()) {
                            out.push(
                                Diagnostic::error(
                                    "schema/reference",
                                    format!("boundary {:?} wraps unknown component {w:?}", b.label),
                                )
                                .subject(json!({ "path": format!("/boundaries/{i}/wraps/{j}") })),
                            );
                        }
                    }
                }
            }
            let mut conn_ids: HashSet<&str> = HashSet::new();
            if let Some(cs) = &s.connections {
                for (i, c) in cs.iter().enumerate() {
                    if let Some(id) = &c.id
                        && !conn_ids.insert(id.as_str())
                    {
                        out.push(
                            Diagnostic::error(
                                "schema/invalid",
                                format!("duplicate connection id {id:?}"),
                            )
                            .subject(json!({ "path": format!("/connections/{i}/id") })),
                        );
                    }
                    for (field, v) in [("from", &c.from), ("to", &c.to)] {
                        if !ids.contains(v.as_str()) {
                            out.push(
                                Diagnostic::error(
                                    "schema/reference",
                                    format!(
                                        "connection {i} {field} references unknown component {v:?}"
                                    ),
                                )
                                .subject(json!({ "path": format!("/connections/{i}/{field}") })),
                            );
                        }
                    }
                    if c.from == c.to {
                        out.push(
                            Diagnostic::error(
                                "schema/invalid",
                                format!("connection {i} connects {:?} to itself", c.from),
                            )
                            .subject(json!({ "path": format!("/connections/{i}") })),
                        );
                    }
                    check_side(&c.from_side, format!("/connections/{i}/fromSide"), &mut out);
                    check_side(&c.to_side, format!("/connections/{i}/toSide"), &mut out);
                    if let Some(r) = &c.route
                        && !matches!(
                            r.as_str(),
                            "auto" | "straight" | "orthogonal-h" | "orthogonal-v"
                        )
                    {
                        out.push(
                            Diagnostic::error(
                                "schema/invalid",
                                format!("connection {i} route {r:?} is unknown"),
                            )
                            .subject(json!({ "path": format!("/connections/{i}/route") })),
                        );
                    }
                }
            }
            check_cards(&s.cards, &mut out);
        }
        Spec::Workflow(s) => {
            if s.schema_version != 1 && s.schema_version != 2 {
                out.push(
                    Diagnostic::error("schema/invalid", "workflow schema_version must be 1 or 2")
                        .subject(json!({ "path": "/schema_version" })),
                );
            }
            let mut lane_ids: HashSet<&str> = HashSet::new();
            for (i, l) in s.lanes.iter().enumerate() {
                if !lane_ids.insert(l.id.as_str()) {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("duplicate lane id {:?}", l.id),
                        )
                        .subject(json!({ "path": format!("/lanes/{i}/id") })),
                    );
                }
                if let Some(v) = &l.variant
                    && !matches!(v.as_str(), "default" | "exception" | "emphasis")
                {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("lane {:?} variant {v:?} is unknown", l.id),
                        )
                        .subject(json!({ "path": format!("/lanes/{i}/variant") })),
                    );
                }
            }
            if s.lanes.is_empty() {
                out.push(
                    Diagnostic::error("schema/invalid", "lanes must hold at least one entry")
                        .subject(json!({ "path": "/lanes" })),
                );
            }
            let mut ids: HashSet<&str> = HashSet::new();
            let mut slots: HashSet<(String, u32)> = HashSet::new();
            for (i, n) in s.nodes.iter().enumerate() {
                if !valid_id(&n.id) {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("node id {:?} is not a valid id", n.id),
                        )
                        .subject(json!({ "path": format!("/nodes/{i}/id") })),
                    );
                }
                if !ids.insert(n.id.as_str()) {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("duplicate node id {:?}", n.id),
                        )
                        .subject(json!({ "path": format!("/nodes/{i}/id") })),
                    );
                }
                if !lane_ids.contains(n.lane.as_str()) {
                    out.push(
                        Diagnostic::error(
                            "schema/reference",
                            format!("node {:?} sits in unknown lane {:?}", n.id, n.lane),
                        )
                        .subject(json!({ "path": format!("/nodes/{i}/lane") })),
                    );
                }
                if n.col > MAX_WORKFLOW_COL {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!(
                                "/nodes/{i}/col (id/label: {:?}) must be <= {MAX_WORKFLOW_COL}",
                                n.id
                            ),
                        )
                        .subject(
                            json!({ "path": format!("/nodes/{i}/col"), "limit": MAX_WORKFLOW_COL }),
                        )
                        .fix("fold a step into a neighbour or move it to another lane"),
                    );
                }
                if !slots.insert((n.lane.clone(), n.col)) {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!(
                                "node {:?} shares lane {:?} column {} with another node",
                                n.id, n.lane, n.col
                            ),
                        )
                        .subject(json!({ "path": format!("/nodes/{i}") })),
                    );
                }
                if n.label.trim().is_empty() {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("node {:?} has an empty label", n.id),
                        )
                        .subject(json!({ "path": format!("/nodes/{i}/label") })),
                    );
                }
                if n.width.is_some_and(|w| w <= 0.0) || n.height.is_some_and(|h| h <= 0.0) {
                    out.push(
                        Diagnostic::error(
                            "schema/invalid",
                            format!("node {:?} width/height must be positive", n.id),
                        )
                        .subject(json!({ "path": format!("/nodes/{i}") })),
                    );
                }
            }
            if s.nodes.is_empty() {
                out.push(
                    Diagnostic::error("schema/invalid", "nodes must hold at least one entry")
                        .subject(json!({ "path": "/nodes" })),
                );
            }
            check_meta(&s.meta, false, &ids, &mut out);
            if let Some(ps) = &s.phases {
                for (i, p) in ps.iter().enumerate() {
                    if p.from_col > p.to_col || p.to_col > MAX_WORKFLOW_COL {
                        out.push(Diagnostic::error("schema/invalid", format!("/phases/{i} (id/label: {:?}) fromCol..toCol must be ordered and <= {MAX_WORKFLOW_COL}", p.id)).subject(json!({ "path": format!("/phases/{i}"), "limit": MAX_WORKFLOW_COL })));
                    }
                }
                for a in 0..ps.len() {
                    for b in a + 1..ps.len() {
                        if ps[a].from_col <= ps[b].to_col && ps[b].from_col <= ps[a].to_col {
                            out.push(
                                Diagnostic::error(
                                    "schema/invalid",
                                    format!(
                                        "phases {:?} and {:?} overlap in columns",
                                        ps[a].id, ps[b].id
                                    ),
                                )
                                .subject(json!({ "path": format!("/phases/{b}") })),
                            );
                        }
                    }
                }
            }
            if let Some(gs) = &s.groups {
                for (i, g) in gs.iter().enumerate() {
                    if !lane_ids.contains(g.lane.as_str()) {
                        out.push(
                            Diagnostic::error(
                                "schema/reference",
                                format!("group {:?} sits in unknown lane {:?}", g.id, g.lane),
                            )
                            .subject(json!({ "path": format!("/groups/{i}/lane") })),
                        );
                    }
                    if g.from_col > g.to_col || g.to_col > MAX_WORKFLOW_COL {
                        out.push(Diagnostic::error("schema/invalid", format!("group {:?} fromCol..toCol must be ordered and <= {MAX_WORKFLOW_COL}", g.id)).subject(json!({ "path": format!("/groups/{i}") })));
                    }
                }
            }
            let mut edge_pairs: HashSet<(&str, &str)> = HashSet::new();
            if let Some(es) = &s.edges {
                for (i, e) in es.iter().enumerate() {
                    for (field, v) in [("from", &e.from), ("to", &e.to)] {
                        if !ids.contains(v.as_str()) {
                            out.push(
                                Diagnostic::error(
                                    "schema/reference",
                                    format!("edge {i} {field} references unknown node {v:?}"),
                                )
                                .subject(json!({ "path": format!("/edges/{i}/{field}") })),
                            );
                        }
                    }
                    if e.from == e.to {
                        out.push(
                            Diagnostic::error(
                                "schema/invalid",
                                format!("edge {i} connects {:?} to itself", e.from),
                            )
                            .subject(json!({ "path": format!("/edges/{i}") })),
                        );
                    }
                    edge_pairs.insert((e.from.as_str(), e.to.as_str()));
                    check_side(&e.from_side, format!("/edges/{i}/fromSide"), &mut out);
                    check_side(&e.to_side, format!("/edges/{i}/toSide"), &mut out);
                    if let Some(r) = &e.route
                        && !matches!(
                            r.as_str(),
                            "auto"
                                | "straight"
                                | "drop"
                                | "outside-right"
                                | "return-left"
                                | "bottom-channel"
                                | "up-channel"
                        )
                    {
                        out.push(
                            Diagnostic::error(
                                "schema/invalid",
                                format!("edge {i} route {r:?} is unknown"),
                            )
                            .subject(json!({ "path": format!("/edges/{i}/route") })),
                        );
                    }
                }
            }
            if let Some(mp) = &s.main_path {
                for (i, id) in mp.iter().enumerate() {
                    if !ids.contains(id.as_str()) {
                        out.push(
                            Diagnostic::error(
                                "schema/reference",
                                format!("mainPath names unknown node {id:?}"),
                            )
                            .subject(json!({ "path": format!("/mainPath/{i}") })),
                        );
                    }
                }
                for (i, w) in mp.windows(2).enumerate() {
                    if !edge_pairs.contains(&(w[0].as_str(), w[1].as_str())) {
                        out.push(
                            Diagnostic::error(
                                "schema/reference",
                                format!("mainPath step {:?} -> {:?} has no edge", w[0], w[1]),
                            )
                            .subject(json!({ "path": format!("/mainPath/{}", i + 1) }))
                            .fix("add the connecting edge or reorder mainPath"),
                        );
                    }
                }
            }
            check_cards(&s.cards, &mut out);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arch(json: &str) -> Vec<Diagnostic> {
        let raw: Value = serde_json::from_str(json).unwrap();
        let spec: ArchitectureSpec = serde_json::from_value(raw).unwrap();
        validate_semantics(&Spec::Architecture(spec))
    }

    #[test]
    fn architecture_rejects_dangling_connection_references() {
        let d = arch(
            r#"{"schema_version":1,"diagram_type":"architecture","meta":{"title":"t"},
          "components":[{"id":"a","type":"backend","label":"A"}],
          "connections":[{"from":"a","to":"ghost"}]}"#,
        );
        assert!(d.iter().any(|x| x.code == "schema/reference"));
    }

    #[test]
    fn unknown_fields_are_schema_errors() {
        let raw: Value = serde_json::from_str(
            r#"{"schema_version":1,"diagram_type":"architecture","meta":{"title":"t"},
          "components":[{"id":"a","type":"backend","label":"A","colour":"red"}]}"#,
        )
        .unwrap();
        assert!(serde_json::from_value::<ArchitectureSpec>(raw).is_err());
    }

    #[test]
    fn workflow_column_budget_and_main_path_edges_are_enforced() {
        let raw: Value = serde_json::from_str(
            r#"{"schema_version":2,"diagram_type":"workflow","meta":{"title":"t"},
          "lanes":[{"id":"l","label":"L"}],
          "mainPath":["a","b"],
          "nodes":[{"id":"a","lane":"l","col":0,"type":"backend","label":"A"},
                   {"id":"b","lane":"l","col":6,"type":"backend","label":"B"}]}"#,
        )
        .unwrap();
        let spec: WorkflowSpec = serde_json::from_value(raw).unwrap();
        let d = validate_semantics(&Spec::Workflow(spec));
        assert!(d.iter().any(|x| x.message.contains("must be <= 5")));
        assert!(d.iter().any(|x| x.message.contains("has no edge")));
    }
}
