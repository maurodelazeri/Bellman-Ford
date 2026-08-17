//! Event-trail completeness verifier.
//!
//! This is the part that answers the real requirement: *"we cannot have event
//! one and two and then no three and four."* It reads everything the sink
//! wrote and proves, per transaction, that the trail is a legal walk through
//! the stage machine ending in `Settled`.
//!
//! It also counts duplicates, because duplicates are the *expected* signature
//! of the design: when the service crashes and Restate replays a handler, the
//! trail is re-emitted with identical `(tx_id, seq)` keys. Seeing duplicates
//! but no gaps is exactly the result that proves the "emit is not journaled,
//! the sink dedups" trade is sound.

use std::collections::HashMap;

use spray_core::events::LifecycleEvent;
use spray_core::types::Stage;

#[derive(Debug, Default)]
pub struct VerifyReport {
    pub files: usize,
    pub lines: usize,
    pub parse_errors: usize,
    pub transactions: usize,
    /// Trails that pass every rule.
    pub complete: usize,
    /// Trails missing one of the mandatory early stages.
    pub missing_prefix: Vec<String>,
    /// Trails with no chain-terminal stage.
    pub missing_terminal: Vec<String>,
    /// Trails that never reached `Settled`.
    pub unsettled: Vec<String>,
    /// Trails with more than one chain-terminal stage: a genuine bug.
    pub conflicting_terminal: Vec<String>,
    /// Duplicate (tx_id, seq) pairs. Expected after a crash; must be
    /// byte-identical in stage, which is what makes sink-side dedup safe.
    pub duplicate_events: usize,
    /// Duplicates whose payload disagreed with the first copy. Must be zero, or
    /// the "emit is idempotent" premise is false.
    pub inconsistent_duplicates: Vec<String>,
    pub outcome_counts: HashMap<String, usize>,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.missing_prefix.is_empty()
            && self.missing_terminal.is_empty()
            && self.unsettled.is_empty()
            && self.conflicting_terminal.is_empty()
            && self.inconsistent_duplicates.is_empty()
    }

    pub fn to_json(&self) -> serde_json::Value {
        fn head(v: &[String]) -> Vec<&String> {
            v.iter().take(10).collect()
        }
        serde_json::json!({
            "ok": self.ok(),
            "files": self.files,
            "lines": self.lines,
            "parse_errors": self.parse_errors,
            "transactions": self.transactions,
            "complete": self.complete,
            "duplicate_events": self.duplicate_events,
            "inconsistent_duplicates": self.inconsistent_duplicates.len(),
            "missing_prefix": { "count": self.missing_prefix.len(), "sample": head(&self.missing_prefix) },
            "missing_terminal": { "count": self.missing_terminal.len(), "sample": head(&self.missing_terminal) },
            "unsettled": { "count": self.unsettled.len(), "sample": head(&self.unsettled) },
            "conflicting_terminal": { "count": self.conflicting_terminal.len(), "sample": head(&self.conflicting_terminal) },
            "outcomes": self.outcome_counts,
        })
    }
}

#[derive(Default)]
struct Trail {
    stages: HashMap<u32, Stage>,
    dup_count: usize,
    inconsistent: bool,
}

/// Verify every `*.jsonl` shard matching `prefix`.
pub fn verify(prefix: &str) -> anyhow::Result<VerifyReport> {
    let mut report = VerifyReport::default();
    let path = std::path::Path::new(prefix);
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let base = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let dir = dir.unwrap_or_else(|| std::path::Path::new("."));

    let mut trails: HashMap<String, Trail> = HashMap::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(&base) || !name.ends_with(".jsonl") {
            continue;
        }
        report.files += 1;
        let content = std::fs::read_to_string(entry.path())?;
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            report.lines += 1;
            let ev: LifecycleEvent = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(_) => {
                    report.parse_errors += 1;
                    continue;
                }
            };
            let trail = trails.entry(ev.tx_id.clone()).or_default();
            match trail.stages.insert(ev.seq, ev.stage) {
                None => {}
                Some(prev) => {
                    trail.dup_count += 1;
                    if prev != ev.stage {
                        trail.inconsistent = true;
                    }
                }
            }
        }
    }

    report.transactions = trails.len();
    for (tx_id, trail) in trails {
        report.duplicate_events += trail.dup_count;
        if trail.inconsistent {
            report.inconsistent_duplicates.push(tx_id.clone());
        }

        let has = |s: Stage| trail.stages.get(&(s as u32)) == Some(&s);

        let mut problem = false;
        if !(has(Stage::Accepted) && has(Stage::Validated) && has(Stage::Spraying)) {
            report.missing_prefix.push(tx_id.clone());
            problem = true;
        }

        let terminals: Vec<Stage> = [
            Stage::Confirmed,
            Stage::Expired,
            Stage::FailedOnChain,
            Stage::Dropped,
        ]
        .into_iter()
        .filter(|s| has(*s))
        .collect();

        match terminals.len() {
            0 => {
                report.missing_terminal.push(tx_id.clone());
                problem = true;
            }
            1 => {
                *report
                    .outcome_counts
                    .entry(terminals[0].as_str().to_string())
                    .or_insert(0) += 1;
            }
            _ => {
                report.conflicting_terminal.push(tx_id.clone());
                problem = true;
            }
        }

        if !has(Stage::Settled) {
            report.unsettled.push(tx_id.clone());
            problem = true;
        }

        if !problem {
            report.complete += 1;
        }
    }

    Ok(report)
}
