use hotpath::{Format, HotpathGuardBuilder, Section};
use pgtest_database_operations::{manager::PostgresManager, testcontainer::pg_container_config};
use tracing_subscriber::{EnvFilter, prelude::*};

// This separate test binary owns Hotpath's process-wide collector and
// subscriber.
#[tokio::test]
async fn tokio_postgres_queries_reach_the_sql_report_with_console_logging_disabled() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("sql-profile.json");
    let guard = HotpathGuardBuilder::new("sql_profiling")
        .format(Format::Json)
        .sections(vec![Section::Sql])
        .output_path(&output)
        .build();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::new("off")))
        .with(pgtest_database_operations::sql_tracing_layer())
        .init();

    let manager = PostgresManager::start(pg_container_config().await).await.unwrap();
    let first = manager.create_ddl_database().await.unwrap();
    let mut created = std::collections::BTreeMap::new();
    manager
        .create_ddl_databases(2, |index, result| {
            assert!(created.insert(index, result.unwrap()).is_none());
        })
        .await;
    let second = created.remove(&0).unwrap();
    let third = created.remove(&1).unwrap();
    manager.drop_ddl_database(&first).await.unwrap();
    let mut completions = Vec::new();
    manager
        .drop_ddl_databases(&[&second, "postgres"], |index, result| {
            completions.push((index, result));
        })
        .await
        .unwrap();
    assert_eq!(completions.len(), 2);
    assert!(completions.iter().any(|(index, result)| *index == 0 && result.is_ok()));
    // The connected database cannot be dropped; failed executions must count
    // too.
    assert!(completions.iter().any(|(index, result)| *index == 1 && result.is_err()));
    manager.drop_ddl_templates_like().await.unwrap();
    drop(manager);
    drop(guard);

    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
    let sql = &report["sql"];
    assert_eq!(sql["total_calls"], 10);
    let entries = sql["data"].as_array().unwrap();
    let count = |prefix: &str| -> u64 {
        entries
            .iter()
            .filter(|entry| entry["query"].as_str().unwrap().starts_with(prefix))
            .map(|entry| entry["count"].as_u64().unwrap())
            .sum()
    };
    assert_eq!(count("SELECT datname"), 2);
    assert_eq!(count("SELECT current_setting"), 1);
    assert_eq!(count("CREATE DATABASE"), 3);
    assert_eq!(count("DROP DATABASE"), 4);
    for prefix in ["CREATE DATABASE", "DROP DATABASE"] {
        assert_eq!(
            entries
                .iter()
                .filter(|entry| { entry["query"].as_str().unwrap().starts_with(prefix) })
                .count(),
            1,
            "generated database names must share a SQL bucket"
        );
    }
    let sql_text = sql.to_string();
    for name in [&first, &second, &third] {
        assert!(!sql_text.contains(name.as_ref()), "SQL labels must omit generated identifiers");
    }
    assert!(entries.iter().all(|entry| entry["source"].as_str().is_some()));
}
