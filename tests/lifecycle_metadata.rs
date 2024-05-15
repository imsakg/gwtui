mod common;

use common::TestRepo;
use gwtui::core::git::Git;

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
