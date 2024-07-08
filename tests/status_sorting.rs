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
