use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;
use tempfile::tempdir;

const KNOWN_SKILLS: [&str; 4] = ["dataq", "dataq-rules-recipes", "feat-add", "rev-pass"];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillFrontmatter {
    name: String,
    description: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenAiMetadata {
    interface: OpenAiInterface,
    policy: OpenAiPolicy,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenAiInterface {
    display_name: String,
    short_description: String,
    default_prompt: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenAiPolicy {
    allow_implicit_invocation: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingInventory {
    version: u64,
    cases: Vec<RoutingCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingCase {
    id: String,
    prompt: String,
    expect: Vec<String>,
    reject: Vec<String>,
    reason: String,
}

#[test]
fn frontmatter_metadata_and_invocation_matrix_are_aligned() {
    let root = repository_root();
    let agents = read_text(root.join("AGENTS.md"));
    let matrix = parse_invocation_matrix(&agents);
    let known = known_skill_set();

    assert_eq!(matrix, expected_invocation_policy());
    assert_eq!(matrix.keys().cloned().collect::<BTreeSet<_>>(), known);
    assert_eq!(local_skill_directories(&root), known);

    for skill in KNOWN_SKILLS {
        let skill_root = root.join(".agents/skills").join(skill);
        let skill_text = read_text(skill_root.join("SKILL.md"));
        let frontmatter = parse_frontmatter(&skill_text);
        let metadata: OpenAiMetadata =
            serde_yaml::from_str(&read_text(skill_root.join("agents/openai.yaml")))
                .expect("parse agents/openai.yaml");

        assert_eq!(frontmatter.name, skill);
        assert!(
            frontmatter.description.starts_with("Use when ")
                || frontmatter.description.starts_with("Use only when "),
            "{skill}: trigger description must start with a routing boundary"
        );
        assert!(
            frontmatter.description.contains("Do not use")
                || frontmatter.description.contains("Do not trigger")
                || frontmatter.description.starts_with("Use only when "),
            "{skill}: trigger description must state a non-trigger boundary"
        );
        assert!(
            frontmatter.description.chars().count() <= 300,
            "{skill}: trigger description exceeds 300 characters"
        );
        assert!(!metadata.interface.display_name.trim().is_empty());
        assert!(!metadata.interface.short_description.trim().is_empty());
        let expected_skill_token = format!("${skill}");
        assert_eq!(
            metadata.interface.default_prompt.split_whitespace().nth(1),
            Some(expected_skill_token.as_str()),
            "{skill}: default prompt must start by naming its skill"
        );
        assert_eq!(
            metadata.policy.allow_implicit_invocation, matrix[skill],
            "{skill}: metadata policy differs from AGENTS.md matrix"
        );
    }
}

#[test]
fn distributed_skill_markdown_matches_repository_local_sources() {
    let root = repository_root();

    for skill in ["dataq", "dataq-rules-recipes"] {
        assert_eq!(
            fs::read(root.join(".agents/skills").join(skill).join("SKILL.md"))
                .expect("read repository-local skill"),
            fs::read(root.join("skills").join(skill).join("SKILL.md"))
                .expect("read distributed skill mirror"),
            "{skill}: distributed SKILL.md differs from repository-local source"
        );
    }
}

#[test]
fn routing_cases_have_valid_schema_and_complete_positive_and_reject_coverage() {
    let root = repository_root();
    let inventory: RoutingInventory = serde_json::from_str(&read_text(
        root.join(".agents/references/routing-cases.json"),
    ))
    .expect("parse routing cases");
    let known = known_skill_set();

    assert_eq!(inventory.version, 1);
    assert!(inventory.cases.len() >= 4);

    let mut ids = BTreeSet::new();
    let mut positive = BTreeSet::new();
    let mut rejected = BTreeSet::new();

    for case in inventory.cases {
        assert!(!case.id.trim().is_empty());
        assert!(
            ids.insert(case.id.clone()),
            "duplicate case id: {}",
            case.id
        );
        assert!(!case.prompt.trim().is_empty(), "{}: empty prompt", case.id);
        assert!(!case.reason.trim().is_empty(), "{}: empty reason", case.id);
        assert!(!case.expect.is_empty(), "{}: empty expect list", case.id);
        assert!(!case.reject.is_empty(), "{}: empty reject list", case.id);

        let expected_count = case.expect.len();
        let rejected_count = case.reject.len();
        let expected = case.expect.into_iter().collect::<BTreeSet<_>>();
        let rejected_by_case = case.reject.into_iter().collect::<BTreeSet<_>>();
        assert_eq!(
            expected.len(),
            expected_count,
            "{}: duplicate expected skill",
            case.id
        );
        assert_eq!(
            rejected_by_case.len(),
            rejected_count,
            "{}: duplicate rejected skill",
            case.id
        );
        assert!(
            expected.is_disjoint(&rejected_by_case),
            "{}: expected and rejected skills overlap",
            case.id
        );
        assert!(
            expected.is_subset(&known),
            "{}: expected skill is not installed",
            case.id
        );
        assert!(
            rejected_by_case.is_subset(&known),
            "{}: rejected skill is not installed",
            case.id
        );

        positive.extend(expected);
        rejected.extend(rejected_by_case);
    }

    assert_eq!(positive, known, "missing positive routing coverage");
    assert_eq!(rejected, known, "missing near-miss reject coverage");
}

#[test]
fn runtime_installer_keeps_the_single_skill_distribution_boundary() {
    let root = repository_root();
    let destination_root = tempdir().expect("destination root");
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["codex", "install-skill", "--dest"])
        .arg(destination_root.path())
        .output()
        .expect("run codex install-skill");

    assert_eq!(output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("parse installer stdout");
    assert_eq!(payload["skill_name"], Value::from("dataq"));
    assert_eq!(
        payload["copied_files"],
        serde_json::json!(["SKILL.md", "agents/openai.yaml"])
    );

    assert_eq!(
        relative_entries(destination_root.path()),
        vec![
            "dataq/".to_string(),
            "dataq/SKILL.md".to_string(),
            "dataq/agents/".to_string(),
            "dataq/agents/openai.yaml".to_string(),
        ]
    );
    assert_eq!(
        fs::read_to_string(destination_root.path().join("dataq/SKILL.md"))
            .expect("read installed SKILL.md"),
        read_text(root.join(".agents/skills/dataq/SKILL.md"))
    );
    assert_eq!(
        fs::read_to_string(destination_root.path().join("dataq/agents/openai.yaml"))
            .expect("read installed metadata"),
        read_text(root.join(".agents/skills/dataq/agents/openai.yaml"))
    );

    assert!(
        root.join(".agents/skills/dataq-rules-recipes/SKILL.md")
            .is_file(),
        "repository-local rules/recipes skill must remain present"
    );
    assert!(
        root.join("skills/dataq-rules-recipes/SKILL.md").is_file(),
        "plugin/distribution mirror for rules/recipes must remain present"
    );
    assert!(!destination_root.path().join("dataq-rules-recipes").exists());
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_text(path: impl AsRef<Path>) -> String {
    let path = path.as_ref();
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn parse_frontmatter(skill_text: &str) -> SkillFrontmatter {
    let body = skill_text
        .strip_prefix("---\n")
        .and_then(|text| text.split_once("\n---\n").map(|(body, _)| body))
        .expect("SKILL.md must start with YAML frontmatter");
    serde_yaml::from_str(body).expect("parse SKILL.md frontmatter")
}

fn parse_invocation_matrix(agents: &str) -> BTreeMap<String, bool> {
    let table = agents
        .lines()
        .skip_while(|line| line.trim() != "### Skill invocation matrix")
        .skip(1)
        .skip_while(|line| !line.trim_start().starts_with('|'))
        .take_while(|line| line.trim_start().starts_with('|'))
        .collect::<Vec<_>>();

    assert_eq!(table.len(), KNOWN_SKILLS.len() + 2);
    assert_eq!(
        markdown_cells(table[0]),
        vec!["Skill", "Owned job", "allow_implicit_invocation"]
    );

    table
        .into_iter()
        .skip(2)
        .map(|row| {
            let cells = markdown_cells(row);
            assert_eq!(cells.len(), 3);
            let skill = cells[0].trim_start_matches('$').to_string();
            let implicit = cells[2]
                .parse::<bool>()
                .expect("matrix policy must be true or false");
            (skill, implicit)
        })
        .collect()
}

fn markdown_cells(row: &str) -> Vec<&str> {
    row.trim()
        .trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().trim_matches('`'))
        .collect()
}

fn known_skill_set() -> BTreeSet<String> {
    KNOWN_SKILLS.into_iter().map(str::to_string).collect()
}

fn expected_invocation_policy() -> BTreeMap<String, bool> {
    BTreeMap::from([
        ("dataq".to_string(), true),
        ("dataq-rules-recipes".to_string(), true),
        ("feat-add".to_string(), false),
        ("rev-pass".to_string(), false),
    ])
}

fn local_skill_directories(root: &Path) -> BTreeSet<String> {
    fs::read_dir(root.join(".agents/skills"))
        .expect("read local skills directory")
        .map(|entry| entry.expect("read local skill entry"))
        .filter(|entry| entry.file_type().expect("read skill entry type").is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}

fn relative_entries(root: &Path) -> Vec<String> {
    fn visit(root: &Path, directory: &Path, entries: &mut Vec<String>) {
        for entry in fs::read_dir(directory).expect("read installed directory") {
            let entry = entry.expect("read installed entry");
            let path = entry.path();
            let relative = path.strip_prefix(root).expect("entry below root");
            let mut display = relative.to_string_lossy().replace('\\', "/");
            if entry
                .file_type()
                .expect("read installed entry type")
                .is_dir()
            {
                display.push('/');
                entries.push(display);
                visit(root, &path, entries);
            } else {
                entries.push(display);
            }
        }
    }

    let mut entries = Vec::new();
    visit(root, root, &mut entries);
    entries.sort();
    entries
}
