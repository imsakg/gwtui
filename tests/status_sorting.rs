#![forbid(unsafe_code)]
use gwtui::core::status::{
    GitStatus, WorktreeState, WorktreeStatus, filter_statuses, sort_statuses,
};

fn row(branch: &str, state: WorktreeState) -> WorktreeStatus {
    WorktreeStatus {
        path: format!("/work/{branch}"),
        branch: branch.into(),
        repository: "repo".into(),
        status: state,
        git_status: GitStatus::default(),
        last_activity: "unknown".into(),
        is_current: false,
    }
}

#[test]
fn unknown_filter_matches_state_not_incidental_branch_names() {
    let rows = vec![
        row("main", WorktreeState::Unknown),
        row("unknown-fix", WorktreeState::Clean),
    ];
    let found = filter_statuses(rows, " UNKNOWN ");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].branch, "main");
}

#[test]
fn every_sort_mode_has_deterministic_tie_breakers() {
    let mut a = row("same", WorktreeState::Clean);
    a.path = "/work/a".into();
    let mut b = a.clone();
    b.path = "/work/b".into();
    let mut c = a.clone();
    c.repository = "z-repo".into();
    let mut d = row("z-branch", WorktreeState::Clean);
    d.path = "/work/0".into();
    let expected = vec![a, b, c, d];
    for key in [
        "branch", " name ", "status", "changes", "modified", "activity", "time", "ahead", "behind",
    ] {
        let mut actual = expected.clone();
        actual.reverse();
        sort_statuses(&mut actual, key);
        assert_eq!(actual, expected, "{key}");
    }
}

#[test]
fn activity_sort_compares_instants_not_rfc3339_text() {
    let mut older = row("older", WorktreeState::Clean);
    older.last_activity = "2025-01-02T00:00:00+03:00".into();
    let mut newer = row("newer", WorktreeState::Clean);
    newer.last_activity = "2025-01-01T22:00:00Z".into();
    let mut newest = row("newest", WorktreeState::Clean);
    newest.last_activity = "2025-01-01T22:00:00.5Z".into();
    let unknown = row("unknown", WorktreeState::Unknown);
    let mut invalid = row("invalid", WorktreeState::Unknown);
    invalid.last_activity = "zz-invalid".into();
    let mut rows = vec![older, unknown, newer, invalid, newest];
    sort_statuses(&mut rows, "activity");
    assert_eq!(
        rows.iter().map(|r| r.branch.as_str()).collect::<Vec<_>>(),
        ["newest", "newer", "older", "invalid", "unknown"]
    );
}
