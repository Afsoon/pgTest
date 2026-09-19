#[cfg(all(test, feature = "stable_ids"))]
use std::sync::atomic::AtomicUsize;

#[cfg(not(all(test, feature = "stable_ids")))]
use rand::{RngExt, rng};

#[cfg(not(all(test, feature = "stable_ids")))]
const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
abcdefghijklmnopqrstuvwxyz";

/// Length of the random identifier appended to generated database names.
const GENERATED_ID_LEN: usize = 32;

pub struct PostgresDatabaseName {
    name: String,
    prefix_template: String,
    #[cfg(all(test, feature = "stable_ids"))]
    seq: AtomicUsize,
}

impl PostgresDatabaseName {
    pub fn new(database_name: String) -> Self {
        let prefix_template = PostgresDatabaseName::truncate(&database_name);

        Self {
            name: database_name,
            prefix_template,
            #[cfg(all(test, feature = "stable_ids"))]
            seq: AtomicUsize::new(0),
        }
    }

    #[cfg(not(all(test, feature = "stable_ids")))]
    fn generate_parallel_test_id() -> String {
        let mut random_gen = rng();
        let template_name_len: usize = 4;

        let mut template_identifier = String::with_capacity(template_name_len);

        for _ in 0..template_name_len {
            let idx = random_gen.random_range(0..CHARSET.len());
            template_identifier.push(CHARSET[idx] as char);
        }

        template_identifier
    }

    #[cfg(not(all(test, feature = "stable_ids")))]
    fn generate_id(&self) -> String {
        let mut random_gen = rng();

        let mut template_identifier = String::with_capacity(GENERATED_ID_LEN);

        for _ in 0..GENERATED_ID_LEN {
            let idx = random_gen.random_range(0..CHARSET.len());
            template_identifier.push(CHARSET[idx] as char);
        }

        template_identifier
    }

    #[cfg(all(test, feature = "stable_ids"))]
    fn generate_id(&self) -> String {
        (self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1).to_string()
    }

    #[cfg(all(test, feature = "stable_ids"))]
    fn truncate(database_name: &str) -> String {
        // Postgres identifiers are limited to 63 bytes; generated names are
        // `{prefix}_{id}`, so the prefix may use at most 63 - id - 1 bytes.
        const MAX_PREFIX_LEN: usize = 63 - GENERATED_ID_LEN - 1;

        if database_name.len() <= MAX_PREFIX_LEN {
            return database_name.to_string();
        }

        let mut cut = MAX_PREFIX_LEN;
        while !database_name.is_char_boundary(cut) {
            cut -= 1;
        }

        database_name[..cut].to_string()
    }

    #[cfg(not(all(test, feature = "stable_ids")))]
    fn truncate(database_name: &str) -> String {
        // Postgres identifiers are limited to 63 bytes; generated names are
        // `{prefix}_{id}`, so the prefix may use at most 63 - id - 1 bytes.
        const MAX_PREFIX_LEN: usize = 63 - GENERATED_ID_LEN - 1 - 4 - 1;

        if database_name.len() <= MAX_PREFIX_LEN {
            return database_name.to_string();
        }

        let mut cut = MAX_PREFIX_LEN;
        while !database_name.is_char_boundary(cut) {
            cut -= 1;
        }

        format!(
            "{}_{}",
            database_name[..cut].to_string(),
            PostgresDatabaseName::generate_parallel_test_id()
        )
    }

    pub fn generate_database_name(&self) -> String {
        let identifier = self.generate_id();

        format!("{}_{}", self.prefix_template, identifier)
    }

    pub fn template_name<'a>(&'a self) -> &'a str {
        &self.name
    }

    pub fn quote_ident(ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod database_name_test {

    use super::PostgresDatabaseName;

    #[test]
    #[ignore = "Until we have a new feature condition for test"]
    fn truncate_never_underflows_and_fits_identifier() {
        let long_name = "a".repeat(40);
        let truncated = PostgresDatabaseName::truncate(&long_name);
        assert_eq!(truncated.len(), 30); // 63 - 32-char id - 1 underscore

        assert_eq!(PostgresDatabaseName::truncate("pgtest"), "pgtest");

        let boundary_name = "b".repeat(30);
        assert_eq!(PostgresDatabaseName::truncate(&boundary_name), boundary_name);
    }
}
