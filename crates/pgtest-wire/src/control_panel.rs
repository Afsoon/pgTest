use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use futures::{Sink, SinkExt, StreamExt, stream};
use pest::Parser;
use pest_derive::Parser;
use pgtest::{
    worker_engine::{
        core::{LeaseId, is_valid_lease_id},
        errors::ReleaseError,
    },
    worker_manager::WorkerEngineManager,
};
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, DEFAULT_NAME, PgWireServerHandlers, Type,
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response},
        stmt::{QueryParser, StoredStatement},
        store::{Entry, PortalStore},
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage,
        extendedquery::{Bind, BindComplete, Parse, ParseComplete},
    },
};

#[derive(Clone)]
pub struct PgTestControlPanel {
    manager: Arc<WorkerEngineManager>,
}

impl PgTestControlPanel {
    pub fn new(manager: Arc<WorkerEngineManager>) -> Self {
        Self { manager }
    }

    async fn release(&self, lease: &str, format: FieldFormat) -> PgWireResult<Response> {
        self.manager.release(LeaseId::from(lease)).await.map_err(Self::release_error)?;
        let schema =
            Arc::new(vec![FieldInfo::new("pgtest_release".into(), None, None, Type::BOOL, format)]);
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field(&true)?;
        let row = encoder.take_row();
        Ok(Response::Query(QueryResponse::new(schema, stream::once(async { Ok(row) }))))
    }

    fn release_error(error: ReleaseError) -> PgWireError {
        let code = match error {
            ReleaseError::InvalidLeaseId => "22023",
            ReleaseError::LeaseRecordLimitReached => "53400",
            ReleaseError::EngineUnavailable => "08006",
            ReleaseError::ReplyTimedOut => "57014",
            ReleaseError::UnexpectedReply => "XX000",
        };
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".into(),
            code.into(),
            error.to_string(),
        )))
    }

    fn resolve_lease_id(
        lease_argument: &LeaseArgument,
        portal: &Portal<PgTestQueryTypeControlStatement>,
    ) -> PgWireResult<String> {
        let lease = match lease_argument {
            LeaseArgument::Literal(literal) => literal.clone(),
            LeaseArgument::Parameter { index } => {
                let Some(parameter) = portal.parameter::<String>(*index, &Type::TEXT)? else {
                    let error_info = ErrorInfo::new(
                        "ERROR".into(),
                        "22004".into(),
                        "Lease ID cannot be NULL".into(),
                    );
                    return Err(PgWireError::UserError(Box::new(error_info)));
                };

                parameter
            }
        };
        if !is_valid_lease_id(&lease) {
            return Err(Self::release_error(ReleaseError::InvalidLeaseId));
        }
        Ok(lease)
    }
}

#[derive(Parser)]
#[grammar = "./control_panel/grammar.pest"]
struct PgTestQueryControlPanelParser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgTestQueryTypeControlStatement {
    Release(LeaseArgument),
    Ping,
}

impl PgTestQueryTypeControlStatement {
    fn parse(query: &str) -> Result<PgTestQueryTypeControlStatement, ()> {
        let mut value = match PgTestQueryControlPanelParser::parse(Rule::query, query) {
            Ok(value) => value,
            Err(_) => {
                return Err(());
            }
        };

        let first_pair = value.next().and_then(|pair| match pair.as_rule() {
            Rule::ping => Some(PgTestQueryTypeControlStatement::Ping),
            Rule::release => {
                let text_literal_pair_child = pair.into_inner().next()?;

                match text_literal_pair_child.as_rule() {
                    Rule::text_literal => {
                        let literal = text_literal_pair_child
                            .as_str()
                            .strip_prefix('\'')
                            .and_then(|partial_normalized| partial_normalized.strip_suffix('\''))
                            .map(|unquote| unquote.replace("''", "'"))?;

                        Some(PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(
                            literal,
                        )))
                    }
                    Rule::parameter => {
                        Some(PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter {
                            index: 0,
                        }))
                    }
                    _ => None,
                }
            }
            _ => None,
        });

        first_pair.ok_or(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseArgument {
    Literal(String),
    Parameter { index: usize },
}

impl PgWireServerHandlers for PgTestControlPanel {
    fn simple_query_handler(&self) -> Arc<impl pgwire::api::query::SimpleQueryHandler> {
        Arc::new(self.clone())
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        Arc::new(self.clone())
    }
}

#[async_trait]
impl SimpleQueryHandler for PgTestControlPanel {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        tracing::debug!("SimpleQueryHandler query {:?}", query);
        match PgTestQueryTypeControlStatement::parse(query) {
            Ok(PgTestQueryTypeControlStatement::Ping) => {
                let response_ping =
                    FieldInfo::new("?column?".into(), None, None, Type::INT4, FieldFormat::Text);
                let ping_schema = Arc::new(vec![response_ping]);

                let data = vec![(Some(1_i32))];
                let schema_ref = ping_schema.clone();
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                let data_row_stream = stream::iter(data).map(move |r| {
                    encoder.encode_field(&r)?;

                    Ok(encoder.take_row())
                });

                Ok(vec![Response::Query(QueryResponse::new(ping_schema, data_row_stream))])
            }
            Ok(PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(lease_id))) => {
                Ok(vec![self.release(&lease_id, FieldFormat::Text).await?])
            }
            Ok(PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter { .. })) => {
                let error_info = ErrorInfo::new(
                    "ERROR".into(),
                    "42P02".into(),
                    "parameters require the extended query protocol".into(),
                );
                Err(PgWireError::UserError(Box::new(error_info)))
            }
            Err(_) => {
                let error_info = ErrorInfo::new(
                    "ERROR".into(),
                    "0A000".into(),
                    "command not support by the control panel".into(),
                );
                Err(PgWireError::UserError(Box::new(error_info)))
            }
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgTestControlPanel {
    type QueryParser = PgTestControlPanel;
    type Statement = PgTestQueryTypeControlStatement;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(self.clone())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement = &portal.statement.statement;
        let expected_statement_parameters = self.get_parameter_types(statement)?.len();

        if portal.parameter_len() != expected_statement_parameters {
            let error_info = ErrorInfo::new(
                "ERROR".into(),
                "08P01".into(),
                format!(
                    "Expected {expected_statement_parameters} bound parameters, received {}",
                    portal.parameter_len()
                ),
            );
            return Err(PgWireError::UserError(Box::new(error_info)));
        }

        match statement {
            PgTestQueryTypeControlStatement::Ping => {
                let ping_schema = Arc::new(
                    self.get_result_schema(&statement, Some(&portal.result_column_format))?,
                );

                let data = vec![(Some(1_i32))];
                let schema_ref = ping_schema.clone();
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                let data_row_stream = stream::iter(data).map(move |r| {
                    encoder.encode_field(&r)?;

                    Ok(encoder.take_row())
                });

                Ok(Response::Query(QueryResponse::new(ping_schema, data_row_stream)))
            }
            PgTestQueryTypeControlStatement::Release(lease_argument) => {
                let lease_id = Self::resolve_lease_id(lease_argument, portal)?;
                self.release(&lease_id, portal.result_column_format.format_for(0)).await
            }
        }
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if message.type_oids.iter().any(|oid| *oid != 0 && *oid != Type::TEXT.oid()) {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "42804".into(),
                "pgtest_release requires a TEXT parameter".into(),
            ))));
        }
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match StoredStatement::parse(client, &message, self.query_parser()).await? {
            Some(statement) => client.portal_store().put_statement(Arc::new(statement)),
            None => client.portal_store().put_empty_statement(name),
        }
        client.send(PgWireBackendMessage::ParseComplete(ParseComplete::new())).await?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if message.parameter_format_codes.len() > 1 {
            let error_info = ErrorInfo::new(
                "ERROR".into(),
                "08P01".into(),
                format!(
                    "Expected 1 or 0 parameter format codes, received {}",
                    message.parameter_format_codes.len()
                ),
            );
            return Err(PgWireError::UserError(Box::new(error_info)));
        }

        if message.parameter_format_codes.get(0).is_some_and(|codec| codec != &1 && codec != &0) {
            let error_info = ErrorInfo::new(
                "ERROR".into(),
                "08P01".into(),
                String::from("Expected parameter format code to be text or binary"),
            );
            return Err(PgWireError::UserError(Box::new(error_info)));
        }

        if message.result_column_format_codes.len() > 1 {
            let error_info = ErrorInfo::new(
                "ERROR".into(),
                "08P01".into(),
                format!(
                    "Expected 1 or 0 result column format codes, received {}",
                    message.result_column_format_codes.len()
                ),
            );
            return Err(PgWireError::UserError(Box::new(error_info)));
        }

        if message.result_column_format_codes.get(0).is_some_and(|codec| codec != &1 && codec != &0)
        {
            let error_info = ErrorInfo::new(
                "ERROR".into(),
                "08P01".into(),
                String::from("Expected result column format code to be text or binary"),
            );
            return Err(PgWireError::UserError(Box::new(error_info)));
        }

        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        let portal_name = message.portal_name.as_deref().unwrap_or(DEFAULT_NAME);

        match client.portal_store().get_statement(statement_name) {
            Some(Entry::Value(statement)) => {
                let expected_statement_parameters =
                    self.get_parameter_types(&statement.statement)?.len();

                if message.parameters.len() != expected_statement_parameters {
                    let error_info = ErrorInfo::new(
                        "ERROR".into(),
                        "08P01".into(),
                        format!(
                            "Expected {expected_statement_parameters} bind parameters, received {}",
                            message.parameters.len()
                        ),
                    );
                    return Err(PgWireError::UserError(Box::new(error_info)));
                }

                let portal = Portal::try_new(&message, statement.clone())?;

                if let PgTestQueryTypeControlStatement::Release(lease) = &statement.statement {
                    PgTestControlPanel::resolve_lease_id(lease, &portal)?;
                }

                client.portal_store().put_portal(Arc::new(portal));
            }
            Some(Entry::Empty) => {
                if !message.parameters.is_empty() {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "08P01".to_owned(),
                        format!(
                            "bind message supplies {} parameters, but prepared statement {:?} \
                             requires 0",
                            message.parameters.len(),
                            statement_name
                        ),
                    ))));
                }
                client.portal_store().put_empty_portal(portal_name);
            }
            None => return Err(PgWireError::StatementNotFound(statement_name.to_owned())),
        }

        client.send(PgWireBackendMessage::BindComplete(BindComplete::new())).await?;

        Ok(())
    }
}

#[async_trait]
impl QueryParser for PgTestControlPanel {
    type Statement = PgTestQueryTypeControlStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Option<Self::Statement>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        tracing::debug!("SimpleQueryHandler parse sql {:?}", sql);
        let Ok(statement) = PgTestQueryTypeControlStatement::parse(sql) else {
            let error_info = ErrorInfo::new(
                "ERROR".into(),
                "0A000".into(),
                "command not support by the control panel".into(),
            );
            return Err(PgWireError::UserError(Box::new(error_info)));
        };
        match statement {
            PgTestQueryTypeControlStatement::Ping => {
                if !types.is_empty() {
                    let error_info = ErrorInfo::new(
                        "ERROR".into(),
                        "42804".into(),
                        "Parameters not expect for ping command".into(),
                    );
                    return Err(PgWireError::UserError(Box::new(error_info)));
                }

                let expected_statement_parameters = self.get_parameter_types(&statement)?.len();

                if types.len() > expected_statement_parameters {
                    let error_info = ErrorInfo::new(
                        "ERROR".into(),
                        "0A000".into(),
                        format!(
                            "Expected {expected_statement_parameters} bound parameters, received \
                             {}",
                            types.len()
                        ),
                    );
                    return Err(PgWireError::UserError(Box::new(error_info)));
                }

                Ok(Some(statement))
            }
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(_)) => {
                if !types.is_empty() {
                    let error_info = ErrorInfo::new(
                        "ERROR".into(),
                        "42804".into(),
                        "Parameters not expect for ping command".into(),
                    );
                    return Err(PgWireError::UserError(Box::new(error_info)));
                }

                let expected_statement_parameters = self.get_parameter_types(&statement)?.len();

                if types.len() > expected_statement_parameters {
                    let error_info = ErrorInfo::new(
                        "ERROR".into(),
                        "0A000".into(),
                        format!(
                            "Expected {expected_statement_parameters} bound parameters, received \
                             {}",
                            types.len()
                        ),
                    );
                    return Err(PgWireError::UserError(Box::new(error_info)));
                }

                Ok(Some(statement))
            }
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter { index }) => {
                let Some(provided_literal_type) = types.get(index) else {
                    return Ok(Some(statement));
                };

                let expected_statement_parameters = self.get_parameter_types(&statement)?.len();

                if types.len() > expected_statement_parameters {
                    let error_info = ErrorInfo::new(
                        "ERROR".into(),
                        "0A000".into(),
                        format!(
                            "Expected {expected_statement_parameters} bound parameters, received \
                             {}",
                            types.len()
                        ),
                    );
                    return Err(PgWireError::UserError(Box::new(error_info)));
                }

                if provided_literal_type
                    .as_ref()
                    .is_some_and(|provided_type| provided_type != &Type::TEXT)
                {
                    let error_info = ErrorInfo::new(
                        "ERROR".into(),
                        "42804".into(),
                        format!("pgtest_release expected {} argument", Type::TEXT),
                    );
                    return Err(PgWireError::UserError(Box::new(error_info)));
                }

                Ok(Some(statement))
            }
        }
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        match stmt {
            PgTestQueryTypeControlStatement::Ping => Ok(vec![]),
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(_)) => Ok(vec![]),
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter { .. }) => {
                Ok(vec![Type::TEXT])
            }
        }
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let output_format = match column_format {
            None => FieldFormat::Text,
            Some(format) => format.format_for(0),
        };

        let (column, data_type) = match stmt {
            PgTestQueryTypeControlStatement::Ping => ("?column?", Type::INT4),
            PgTestQueryTypeControlStatement::Release(_) => ("pgtest_release", Type::BOOL),
        };

        let field_info = FieldInfo::new(String::from(column), None, None, data_type, output_format);

        Ok(vec![field_info])
    }
}

#[cfg(test)]
mod pgtest_control_panel_test {
    use crate::control_panel::{
        LeaseArgument, PgTestControlPanel, PgTestQueryTypeControlStatement,
    };

    #[test]
    fn parse_valid_ping_queries() {
        assert_eq!(
            PgTestQueryTypeControlStatement::parse("SELECT 1").expect("query successfully parsed"),
            PgTestQueryTypeControlStatement::Ping
        );
    }

    #[test]
    fn parse_valid_ping_queries_with_trailing_semicolon() {
        assert_eq!(
            PgTestQueryTypeControlStatement::parse("select 1;").expect("query successfully parsed"),
            PgTestQueryTypeControlStatement::Ping
        );
    }

    #[test]
    fn parse_valid_ping_queries_with_intercalate_new_line() {
        assert_eq!(
            PgTestQueryTypeControlStatement::parse(
                "SELECT
            1"
            )
            .expect("query successfully parsed"),
            PgTestQueryTypeControlStatement::Ping
        );
    }

    #[test]
    fn parse_valid_simple_release_command() {
        assert_eq!(
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(String::from(
                "test-42"
            ))),
            PgTestQueryTypeControlStatement::parse("SELECT pgtest_release('test-42')")
                .expect("query successfully parsed")
        );
    }

    #[test]
    fn parse_valid_middle_quote_characters_release_command() {
        assert_eq!(
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(String::from(
                "suite's test"
            ))),
            PgTestQueryTypeControlStatement::parse("SELECT pgtest_release('suite''s test')")
                .expect("query successfully parsed")
        );
    }

    #[test]
    fn parse_valid_ping_whitespace_and_case_variations() {
        for query in [" SeLeCt\t1 ", "\tSELECT\r\n1 ;\n", "SELECT 1\t;\r\n"] {
            assert_eq!(
                PgTestQueryTypeControlStatement::parse(query),
                Ok(PgTestQueryTypeControlStatement::Ping),
                "expected a ping for {query:?}"
            );
        }
    }

    #[test]
    fn parse_valid_release_literals_preserves_and_decodes_the_value() {
        // Empty IDs are syntactically valid; lease validation is a separate
        // step.
        let cases = [
            ("SELECT pgtest_release('')", ""),
            ("SELECT pgtest_release('$1')", "$1"),
            ("SELECT pgtest_release('MiXeD Case')", "MiXeD Case"),
            ("SELECT pgtest_release('  test-42  ')", "  test-42  "),
            ("SELECT pgtest_release('prueba-ñ-🦀')", "prueba-ñ-🦀"),
            ("SELECT pgtest_release('line\nbreak')", "line\nbreak"),
            ("SELECT pgtest_release('a''b''c')", "a'b'c"),
            ("SELECT pgtest_release('''test''')", "'test'"),
            ("SELECT pgtest_release('''''')", "''"),
            (
                "SELECT pgtest_release('SELECT 1; DROP DATABASE example;')",
                "SELECT 1; DROP DATABASE example;",
            ),
        ];

        for (query, expected) in cases {
            assert_eq!(
                PgTestQueryTypeControlStatement::parse(query),
                Ok(PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(
                    expected.to_owned()
                ))),
                "incorrect literal value for {query:?}"
            );
        }
    }

    #[test]
    fn parse_valid_release_whitespace_and_case_variations() {
        for query in [
            " select PGTEST_RELEASE('test-42'); ",
            "SELECT\npgtest_release(\t'test-42'\r\n) ;",
            "SELECT pgtest_release ('test-42')",
            "\tSeLeCt\r\nPgTeSt_ReLeAsE \t( 'test-42' ) ;\n",
        ] {
            assert_eq!(
                PgTestQueryTypeControlStatement::parse(query),
                Ok(PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(
                    "test-42".to_owned()
                ))),
                "expected a literal release for {query:?}"
            );
        }
    }

    #[test]
    fn parse_valid_release_parameters_use_zero_based_index() {
        for query in [
            "SELECT pgtest_release($1)",
            "SELECT pgtest_release($1::text)",
            "select PGTEST_RELEASE($1::TEXT);",
            "SELECT pgtest_release( $1 :: text )",
            "SELECT pgtest_release($1\n::\tTeXt);",
            "SELECT pgtest_release ( $1::text );",
        ] {
            assert_eq!(
                PgTestQueryTypeControlStatement::parse(query),
                Ok(PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter { index: 0 })),
                "expected the first parameter for {query:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_invalid_ping_queries() {
        for query in [
            "",
            " \t\r\n",
            ";",
            "SELECT",
            "SELECT1",
            "SELECT 10",
            "SELECT 01",
            "SELECT 1.0",
            "SELECT 1 extra",
            "SELECT 1 + 1",
            "SELECT 1, 1",
            "SELECT '1'",
            "SELECT 1;;",
        ] {
            assert!(
                PgTestQueryTypeControlStatement::parse(query).is_err(),
                "unexpectedly accepted {query:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_malformed_release_literals() {
        for query in [
            "SELECT pgtest_release()",
            "SELECT pgtest_release(test-42)",
            "SELECT pgtest_release(\"test-42\")",
            "SELECT pgtest_release(NULL)",
            "SELECT pgtest_release(1)",
            "SELECT pgtest_release('unfinished)",
            "SELECT pgtest_release('suite's test')",
            "SELECT pgtest_release('test-42''')'",
            "SELECT pgtest_release('test-42'",
            "SELECT pgtest_release 'test-42')",
            "SELECT pgtest_release('a', 'b')",
            "SELECT pgtest_release('a' 'b')",
            "SELECT pgtest_release('test-42'))",
            "SELECT pgtest_release('test-42');;",
        ] {
            assert!(
                PgTestQueryTypeControlStatement::parse(query).is_err(),
                "unexpectedly accepted {query:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_unsupported_parameters_and_casts() {
        for query in [
            "SELECT pgtest_release($0)",
            "SELECT pgtest_release($2)",
            "SELECT pgtest_release($10)",
            "SELECT pgtest_release($01)",
            "SELECT pgtest_release($-1)",
            "SELECT pgtest_release($ 1)",
            "SELECT pgtest_release($1::)",
            "SELECT pgtest_release($1::varchar)",
            "SELECT pgtest_release($1::integer)",
            "SELECT pgtest_release($1::text_extra)",
            "SELECT pgtest_release($1::text::text)",
            "SELECT pgtest_release($1, $2)",
        ] {
            assert!(
                PgTestQueryTypeControlStatement::parse(query).is_err(),
                "unexpectedly accepted {query:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_other_commands_and_trailing_sql() {
        for query in [
            "BEGIN",
            "COMMIT",
            "DROP DATABASE example",
            "SELECT other_function('test-42')",
            "SELECTpgtest_release('test-42')",
            "SELECT pgtest_release_extra('test-42')",
            "SELECT 1; SELECT 1",
            "SELECT pgtest_release('test-42'); SELECT 1",
            "SELECT pgtest_release($1::text); SELECT 1",
            "SELECT pgtest_release('test-42') trailing",
        ] {
            assert!(
                PgTestQueryTypeControlStatement::parse(query).is_err(),
                "unexpectedly accepted {query:?}"
            );
        }
    }
}
