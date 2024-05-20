mod common;

use common::TestRepo;
use gwtui::core::git::Git;

#[test]
fn history_records_preserve_pipe_subjects_and_author_names() {
    let repo = TestRepo::new();
    repo.git(&["config", "user.name", "History | Writer"]);
    repo.git(&["commit", "--allow-empty", "-m", "one | two | three"]);
    let commits = Git::new(repo.path.clone())
        .get_recent_commits(&repo.path, 2)
        .unwrap();
    assert_eq!(commits.len(), 2);
    assert_eq!(commits[0].message, "one | two | three");
    assert_eq!(commits[0].author, "History | Writer");
    assert_eq!(commits[0].hash, repo.git(&["rev-parse", "HEAD"]));
    assert_eq!(commits[1].message, "initial");
    assert!(!commits[0].date_iso.is_empty());
}

#[test]
fn remote_identity_comes_from_full_refs_not_display_name_prefixes() {
    let repo = TestRepo::new();
    repo.git(&["branch", "upstream/topic"]);
    repo.git(&["update-ref", "refs/remotes/upstream/topic", "HEAD"]);
    repo.git(&[
        "symbolic-ref",
        "refs/remotes/upstream/HEAD",
        "refs/remotes/upstream/topic",
    ]);
    let git = Git::new(repo.path.clone());
    let all = git.list_branches(true).unwrap();
    let remote = all
        .iter()
        .find(|branch| branch.is_remote)
        .expect("remote ref recognized");
    assert_eq!(remote.name, "upstream/topic");
    assert!(!remote.is_current);
    assert_eq!(all.len(), 3, "symbolic remote HEAD must not be a branch");
    let local = git.list_branches(false).unwrap();
    assert_eq!(local.len(), 2);
    assert!(local.iter().all(|branch| !branch.is_remote));
    assert!(local.iter().any(|branch| branch.name == "upstream/topic"));
}

#[test]
fn branch_records_preserve_pipe_characters_in_all_metadata() {
    let repo = TestRepo::new();
    repo.git(&["config", "user.name", "Writer | Name"]);
    repo.git(&["commit", "--allow-empty", "-m", "left | middle | right"]);
    repo.git(&["branch", "topic|one"]);
    let branches = Git::new(repo.path.clone()).list_branches(false).unwrap();
    let topic = branches
        .iter()
        .find(|branch| branch.name == "topic|one")
        .unwrap();
    assert!(!topic.is_current);
    assert!(!topic.is_remote);
    assert_eq!(topic.last_commit.message, "left | middle | right");
    assert_eq!(topic.last_commit.author, "Writer | Name");
    assert_eq!(topic.last_commit.hash, repo.git(&["rev-parse", "HEAD"]));
    assert!(
        branches
            .iter()
            .find(|branch| branch.name == "main")
            .unwrap()
            .is_current
    );
}
