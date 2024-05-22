mod common;
use common::TestRepo;
use gwtui::core::git::Git;

#[test]
fn branch_mutations_reject_invalid_or_option_shaped_names() {
    let repo = TestRepo::new();
    let git = Git::new(repo.path.clone());
    let destination = repo.child("invalid");
    for branch in ["", "--force", "HEAD", "bad..name", "@{-1}", "bad\0name"] {
        assert!(
            git.add_worktree(&destination, branch, true)
                .unwrap_err()
                .to_string()
                .contains("invalid branch")
        );
        assert!(
            git.add_worktree_from_base(&destination, branch, None)
                .unwrap_err()
                .to_string()
                .contains("invalid branch")
        );
        assert!(
            git.delete_branch(branch, true)
                .unwrap_err()
                .to_string()
                .contains("invalid branch")
        );
        assert!(!destination.exists());
    }
    assert_eq!(repo.git(&["branch", "--format=%(refname:short)"]), "main");
}
