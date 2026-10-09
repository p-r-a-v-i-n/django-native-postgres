use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PlaceholderCountMismatch {
    pub(crate) placeholders: usize,
    pub(crate) parameters: usize,
}

enum SqlState {
    Normal,
    SingleQuoted,
    EscapeSingleQuoted,
    DoubleQuoted,
    LineComment,
    BlockComment { depth: usize },
    DollarQuoted { delimiter: String },
}

fn dollar_quote_delimiter(input: &str) -> Option<&str> {
    let remainder = input.strip_prefix('$')?;
    let closing_dollar = remainder.find('$')?;
    let tag = &remainder[..closing_dollar];

    let valid_tag = if tag.is_empty() {
        true
    } else {
        let mut characters = tag.chars();
        let first = characters.next()?;

        (first == '_' || first.is_ascii_alphabetic())
            && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    };

    if valid_tag {
        // Include both surrounding dollar signs.
        Some(&input[..closing_dollar + 2])
    } else {
        None
    }
}

fn rewrite_placeholders(
    sql: &str,
    mut replacement: impl FnMut(&str) -> Option<(usize, String)>,
) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut characters = sql.char_indices().peekable();
    let mut state = SqlState::Normal;

    while let Some((index, character)) = characters.next() {
        state = match state {
            SqlState::Normal => match character {
                'E' | 'e' if characters.peek().is_some_and(|(_, next)| *next == '\'') => {
                    output.push(character);
                    output.push(characters.next().expect("peeked character must exist").1);
                    SqlState::EscapeSingleQuoted
                }
                '\'' => {
                    output.push(character);
                    SqlState::SingleQuoted
                }
                '"' => {
                    output.push(character);
                    SqlState::DoubleQuoted
                }
                '-' if characters.peek().is_some_and(|(_, next)| *next == '-') => {
                    output.push_str("--");
                    characters.next();
                    SqlState::LineComment
                }
                '/' if characters.peek().is_some_and(|(_, next)| *next == '*') => {
                    output.push_str("/*");
                    characters.next();
                    SqlState::BlockComment { depth: 1 }
                }
                '$' => {
                    if let Some(delimiter) = dollar_quote_delimiter(&sql[index..]) {
                        output.push_str(delimiter);

                        // The first '$' was already consumed.
                        for _ in 1..delimiter.chars().count() {
                            characters.next();
                        }

                        SqlState::DollarQuoted {
                            delimiter: delimiter.to_string(),
                        }
                    } else {
                        output.push(character);
                        SqlState::Normal
                    }
                }
                '%' if characters.peek().is_some_and(|(_, next)| *next == '%') => {
                    output.push('%');
                    characters.next();
                    SqlState::Normal
                }
                '%' => match replacement(&sql[index..]) {
                    Some((length, value)) => {
                        output.push_str(&value);

                        // The first character was already consumed.
                        for _ in 1..length {
                            characters.next();
                        }

                        SqlState::Normal
                    }
                    None => {
                        output.push(character);
                        SqlState::Normal
                    }
                },
                _ => {
                    output.push(character);
                    SqlState::Normal
                }
            },

            SqlState::SingleQuoted => {
                output.push(character);

                if character == '\'' {
                    if characters.peek().is_some_and(|(_, next)| *next == '\'') {
                        output.push(characters.next().expect("peeked character must exist").1);
                        SqlState::SingleQuoted
                    } else {
                        SqlState::Normal
                    }
                } else {
                    SqlState::SingleQuoted
                }
            }

            SqlState::EscapeSingleQuoted => {
                output.push(character);

                if character == '\\' {
                    if let Some((_, escaped)) = characters.next() {
                        output.push(escaped);
                    }
                    SqlState::EscapeSingleQuoted
                } else if character == '\'' {
                    if characters.peek().is_some_and(|(_, next)| *next == '\'') {
                        output.push(characters.next().expect("peeked character must exist").1);
                        SqlState::EscapeSingleQuoted
                    } else {
                        SqlState::Normal
                    }
                } else {
                    SqlState::EscapeSingleQuoted
                }
            }

            SqlState::DoubleQuoted => {
                output.push(character);

                if character == '"' {
                    if characters.peek().is_some_and(|(_, next)| *next == '"') {
                        output.push(characters.next().expect("peeked character must exist").1);
                        SqlState::DoubleQuoted
                    } else {
                        SqlState::Normal
                    }
                } else {
                    SqlState::DoubleQuoted
                }
            }

            SqlState::LineComment => {
                output.push(character);

                if character == '\n' {
                    SqlState::Normal
                } else {
                    SqlState::LineComment
                }
            }

            SqlState::BlockComment { mut depth } => {
                output.push(character);

                if character == '/' && characters.peek().is_some_and(|(_, next)| *next == '*') {
                    output.push('*');
                    characters.next();
                    depth += 1;
                } else if character == '*'
                    && characters.peek().is_some_and(|(_, next)| *next == '/')
                {
                    output.push('/');
                    characters.next();
                    depth -= 1;
                }

                if depth == 0 {
                    SqlState::Normal
                } else {
                    SqlState::BlockComment { depth }
                }
            }

            SqlState::DollarQuoted { delimiter } => {
                if character == '$' && sql[index..].starts_with(delimiter.as_str()) {
                    output.push_str(&delimiter);

                    for _ in 1..delimiter.chars().count() {
                        characters.next();
                    }

                    SqlState::Normal
                } else {
                    output.push(character);
                    SqlState::DollarQuoted { delimiter }
                }
            }
        };
    }

    output
}

pub(crate) fn rewrite_django_placeholders(
    sql: &str,
    parameter_count: usize,
) -> Result<String, PlaceholderCountMismatch> {
    let mut placeholder_count = 0;
    let output = rewrite_placeholders(sql, |input| {
        if input.starts_with("%s") {
            placeholder_count += 1;
            Some((2, format!("${placeholder_count}")))
        } else {
            None
        }
    });

    if placeholder_count != parameter_count {
        return Err(PlaceholderCountMismatch {
            placeholders: placeholder_count,
            parameters: parameter_count,
        });
    }

    Ok(output)
}

pub(crate) fn rewrite_django_named_placeholders(sql: &str) -> (String, Vec<String>) {
    let mut parameter_indexes = HashMap::new();
    let mut parameter_names = Vec::new();
    let output = rewrite_placeholders(sql, |input| {
        let remainder = input.strip_prefix("%(")?;
        let closing_parenthesis = remainder.find(')')?;
        let name = &remainder[..closing_parenthesis];

        if name.is_empty() || !remainder[closing_parenthesis..].starts_with(")s") {
            return None;
        }

        let index = match parameter_indexes.get(name) {
            Some(index) => *index,
            None => {
                let index = parameter_names.len() + 1;
                parameter_names.push(name.to_string());
                parameter_indexes.insert(name.to_string(), index);
                index
            }
        };
        let length = input[..closing_parenthesis + 4].chars().count();
        Some((length, format!("${index}")))
    });

    (output, parameter_names)
}

impl std::fmt::Display for PlaceholderCountMismatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SQL contains {} placeholders but received {} parameters",
            self.placeholders, self.parameters,
        )
    }
}

impl std::error::Error for PlaceholderCountMismatch {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_single_placeholder() {
        assert_eq!(
            rewrite_django_placeholders("SELECT %s::TEXT", 1),
            Ok("SELECT $1::TEXT".to_string()),
        );
    }

    #[test]
    fn numbers_multiple_placeholders() {
        assert_eq!(
            rewrite_django_placeholders("SELECT %s::TEXT, %s::TEXT", 2),
            Ok("SELECT $1::TEXT, $2::TEXT".to_string()),
        );
    }

    #[test]
    fn ignores_placeholder_text_inside_quotes() {
        assert_eq!(
            rewrite_django_placeholders("SELECT '%s', %s::TEXT", 1),
            Ok("SELECT '%s', $1::TEXT".to_string()),
        );
    }

    #[test]
    fn rejects_parameter_count_mismatch() {
        assert_eq!(
            rewrite_django_placeholders("SELECT %s::TEXT", 2),
            Err(PlaceholderCountMismatch {
                placeholders: 1,
                parameters: 2,
            }),
        );
    }

    #[test]
    fn preserves_escaped_positional_placeholder_as_literal_text() {
        assert_eq!(
            rewrite_django_placeholders("SELECT %%s, %s::TEXT", 1),
            Ok("SELECT %s, $1::TEXT".to_string()),
        );
    }

    #[test]
    fn ignores_placeholder_text_inside_double_quotes() {
        assert_eq!(
            rewrite_django_placeholders(r#"SELECT "%s", %s::TEXT"#, 1),
            Ok(r#"SELECT "%s", $1::TEXT"#.to_string()),
        );
    }

    #[test]
    fn ignores_placeholder_text_inside_line_comments() {
        assert_eq!(
            rewrite_django_placeholders("SELECT %s::TEXT -- preserve %s\n", 1),
            Ok("SELECT $1::TEXT -- preserve %s\n".to_string()),
        );
    }

    #[test]
    fn ignores_placeholder_text_inside_block_comments() {
        assert_eq!(
            rewrite_django_placeholders("SELECT /* preserve %s */ %s::TEXT", 1),
            Ok("SELECT /* preserve %s */ $1::TEXT".to_string()),
        );
    }

    #[test]
    fn ignores_placeholder_text_inside_dollar_quotes() {
        assert_eq!(
            rewrite_django_placeholders("SELECT $$preserve %s$$, %s::TEXT", 1),
            Ok("SELECT $$preserve %s$$, $1::TEXT".to_string()),
        );
    }

    #[test]
    fn handles_tagged_dollar_quotes() {
        assert_eq!(
            rewrite_django_placeholders("SELECT $body$preserve %s$body$, %s::TEXT", 1,),
            Ok("SELECT $body$preserve %s$body$, $1::TEXT".to_string()),
        );
    }

    #[test]
    fn handles_nested_block_comments() {
        assert_eq!(
            rewrite_django_placeholders("SELECT /* outer %s /* inner %s */ outer */ %s::TEXT", 1,),
            Ok("SELECT /* outer %s /* inner %s */ outer */ $1::TEXT".to_string(),),
        );
    }

    #[test]
    fn handles_doubled_single_quotes() {
        assert_eq!(
            rewrite_django_placeholders("SELECT 'it''s %s', %s::TEXT", 1),
            Ok("SELECT 'it''s %s', $1::TEXT".to_string()),
        );
    }

    #[test]
    fn handles_postgres_escape_strings() {
        assert_eq!(
            rewrite_django_placeholders(r"SELECT E'it\'s %s', %s::TEXT", 1,),
            Ok(r"SELECT E'it\'s %s', $1::TEXT".to_string()),
        );
    }

    #[test]
    fn rewrites_named_placeholders_in_sql_order() {
        assert_eq!(
            rewrite_django_named_placeholders(
                "SELECT %(second)s::TEXT, %(first)s::TEXT, %(second)s::TEXT",
            ),
            (
                "SELECT $1::TEXT, $2::TEXT, $1::TEXT".to_string(),
                vec!["second".to_string(), "first".to_string()],
            ),
        );
    }

    #[test]
    fn ignores_named_placeholder_text_outside_normal_sql() {
        assert_eq!(
            rewrite_django_named_placeholders(
                "SELECT '%(quoted)s', \"%(identifier)s\", %(value)s::TEXT \
                 -- %(line_comment)s\n/* %(block_comment)s */ $$%(dollar)s$$",
            ),
            (
                "SELECT '%(quoted)s', \"%(identifier)s\", $1::TEXT \
                 -- %(line_comment)s\n/* %(block_comment)s */ $$%(dollar)s$$"
                    .to_string(),
                vec!["value".to_string()],
            ),
        );
    }

    #[test]
    fn preserves_escaped_named_placeholder_as_literal_text() {
        assert_eq!(
            rewrite_django_named_placeholders("SELECT %%(name)s, %(name)s::TEXT"),
            (
                "SELECT %(name)s, $1::TEXT".to_string(),
                vec!["name".to_string()],
            ),
        );
    }
}
