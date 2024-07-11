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
