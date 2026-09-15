mod plan;

use std::fmt;

use xshell::{Shell, cmd};

use plan::{Seed, TopicCount, TopicName, TopicPlan};

#[derive(clap::Args)]
pub struct Args {
    /// How many topics to ensure from the local seed sequence.
    #[arg(long, default_value_t = TopicCount::DEFAULT, value_parser = TopicCount::parse_cli)]
    count: TopicCount,

    /// Passed to rpk as `-X brokers=`.
    #[arg(long, default_value = "127.0.0.1:9092", value_parser = BootstrapServers::parse)]
    brokers: BootstrapServers,
}

#[derive(Clone)]
struct BootstrapServers(String);

impl BootstrapServers {
    fn parse(raw: &str) -> Result<Self, Error> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(Error::BrokersEmpty);
        }
        Ok(Self(trimmed.to_owned()))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

struct Report {
    created: Vec<TopicName>,
    already_existed: Vec<TopicName>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CreateOutcome {
    Created,
    AlreadyExists,
}

#[derive(Debug)]
pub enum Error {
    BrokersEmpty,
    RpkMissing,
    CreateFailed { detail: String },
    ResponseMismatch { missing: Vec<String> },
    Shell(xshell::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BrokersEmpty => write!(f, "--brokers is empty"),
            Self::RpkMissing => write!(f, "rpk is not on PATH"),
            Self::CreateFailed { detail } => write!(f, "{detail}"),
            Self::ResponseMismatch { missing } => {
                write!(f, "rpk exited 0 but did not report: {}", missing.join(", "))
            }
            Self::Shell(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Shell(err) => Some(err),
            _ => None,
        }
    }
}

impl From<xshell::Error> for Error {
    fn from(err: xshell::Error) -> Self {
        if err.to_string().starts_with("command not found:") {
            Self::RpkMissing
        } else {
            Self::Shell(err)
        }
    }
}

pub fn run(sh: &Shell, args: Args) -> Result<(), Error> {
    let plan = TopicPlan::generate(args.count, Seed::LOCAL);
    let report = ensure(sh, &args.brokers, &plan)?;
    println!(
        "{} created, {} already existed",
        report.created.len(),
        report.already_existed.len()
    );
    Ok(())
}

fn ensure(sh: &Shell, brokers: &BootstrapServers, plan: &TopicPlan) -> Result<Report, Error> {
    let brokers = brokers.as_str();
    let names: Vec<&str> = plan.names().iter().map(TopicName::as_str).collect();
    let output = cmd!(
        sh,
        "rpk topic create --if-not-exists -p 1 -r 1 -X brokers={brokers} {names...}"
    )
    .ignore_status()
    .output()?;

    if !output.status.success() {
        return Err(Error::CreateFailed {
            detail: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let rows = parse_status_table(&stdout)?;
    reconcile(plan, &rows)
}

fn parse_status_table(stdout: &str) -> Result<Vec<(&str, CreateOutcome)>, Error> {
    let mut rows = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((name, status)) = split_name_status(line) else {
            return Err(Error::CreateFailed {
                detail: format!("unreadable status line: {line}"),
            });
        };
        if name.eq_ignore_ascii_case("TOPIC") && status.eq_ignore_ascii_case("STATUS") {
            continue;
        }
        let outcome = match status {
            "OK" => CreateOutcome::Created,
            "OK (topic already exists)" => CreateOutcome::AlreadyExists,
            other => {
                return Err(Error::CreateFailed {
                    detail: format!("unexpected status for {name}: {other}"),
                });
            }
        };
        rows.push((name, outcome));
    }
    Ok(rows)
}

fn split_name_status(line: &str) -> Option<(&str, &str)> {
    let name_end = line.find(char::is_whitespace)?;
    let status = line[name_end..].trim();
    if status.is_empty() {
        return None;
    }
    Some((&line[..name_end], status))
}

fn reconcile(plan: &TopicPlan, rows: &[(&str, CreateOutcome)]) -> Result<Report, Error> {
    let mut created = Vec::new();
    let mut already_existed = Vec::new();
    let mut missing = Vec::new();
    for name in plan.names() {
        match rows.iter().find(|(reported, _)| *reported == name.as_str()) {
            Some((_, CreateOutcome::Created)) => created.push(name.clone()),
            Some((_, CreateOutcome::AlreadyExists)) => already_existed.push(name.clone()),
            None => missing.push(name.as_str().to_owned()),
        }
    }
    if !missing.is_empty() {
        return Err(Error::ResponseMismatch { missing });
    }
    Ok(Report {
        created,
        already_existed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_created_and_already_exists_table() {
        let captured = "\
TOPIC          STATUS
dev.orders.v1  OK
dev.billing    OK (topic already exists)
";
        assert_eq!(
            parse_status_table(captured).unwrap(),
            vec![
                ("dev.orders.v1", CreateOutcome::Created),
                ("dev.billing", CreateOutcome::AlreadyExists),
            ]
        );
    }

    #[test]
    fn parse_if_not_exists_recreate_table() {
        let captured = "\
TOPIC  STATUS
a      OK (topic already exists)
c      OK
";
        assert_eq!(
            parse_status_table(captured).unwrap(),
            vec![
                ("a", CreateOutcome::AlreadyExists),
                ("c", CreateOutcome::Created),
            ]
        );
    }
}
