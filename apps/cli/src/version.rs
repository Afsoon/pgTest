pub fn display() -> String {
    let version = option_env!("PGTEST_VERSION").filter(|value| !value.is_empty()).unwrap_or("dev");
    let commit =
        option_env!("PGTEST_COMMIT_SHA").filter(|value| !value.is_empty()).unwrap_or("dev");
    format!("{version} (commit {commit})")
}
