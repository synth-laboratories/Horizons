use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq)]
enum Token {
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Colon,
    Comma,
    Dot,
    SlashSlash,
    Ident(String),
    String(String),
    Number(String),
    True,
    False,
    Null,
}

#[derive(Debug, Clone)]
enum Expr {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    List(Vec<Expr>),
    Dict(Vec<(String, Expr)>),
    Var(String),
    Attr(Box<Expr>, String),
    Call(Box<Expr>, String, Vec<Expr>),
    IntDiv(Box<Expr>, Box<Expr>),
}

#[derive(Debug)]
pub struct EvalError(pub String);

pub fn eval_mapping_expression(
    expr: &str,
    state: &Value,
    node_output: Option<&Value>,
) -> Result<Value, EvalError> {
    let tokens = tokenize(expr)?;
    let mut parser = Parser::new(tokens);
    let parsed = parser.parse_expr()?;
    if !parser.is_done() {
        return Err(EvalError("unexpected trailing tokens".to_string()));
    }
    let ctx = EvalContext { state, node_output };
    eval_expr(&parsed, &ctx)
}

struct EvalContext<'a> {
    state: &'a Value,
    node_output: Option<&'a Value>,
}

fn eval_expr(expr: &Expr, ctx: &EvalContext<'_>) -> Result<Value, EvalError> {
    match expr {
        Expr::Null => Ok(Value::Null),
        Expr::Bool(value) => Ok(Value::Bool(*value)),
        Expr::Number(raw) => parse_number(raw),
        Expr::String(value) => Ok(Value::String(value.clone())),
        Expr::List(items) => {
            let mut result = Vec::new();
            for item in items {
                result.push(eval_expr(item, ctx)?);
            }
            Ok(Value::Array(result))
        }
        Expr::Dict(pairs) => {
            let mut map = Map::new();
            for (key, value_expr) in pairs {
                map.insert(key.clone(), eval_expr(value_expr, ctx)?);
            }
            Ok(Value::Object(map))
        }
        Expr::Var(name) => match name.as_str() {
            "state" => Ok(ctx.state.clone()),
            "node_output" => Ok(ctx.node_output.cloned().unwrap_or(Value::Null)),
            other => Err(EvalError(format!("unknown identifier '{other}'"))),
        },
        Expr::Attr(target, name) => {
            let base = eval_expr(target, ctx)?;
            match base {
                Value::Object(map) => Ok(map.get(name).cloned().unwrap_or(Value::Null)),
                Value::Null => Ok(Value::Null),
                _ => Err(EvalError(format!(
                    "cannot access attribute '{name}' on non-object"
                ))),
            }
        }
        Expr::Call(target, name, args) => {
            let base = eval_expr(target, ctx)?;
            match name.as_str() {
                "get" => eval_get(&base, args, ctx),
                "strip" => eval_string_method(&base, |s| s.trim().to_string()),
                "lower" => eval_string_method(&base, |s| s.to_lowercase()),
                "upper" => eval_string_method(&base, |s| s.to_uppercase()),
                other => Err(EvalError(format!("unsupported method '{other}'"))),
            }
        }
        Expr::IntDiv(left, right) => {
            let lhs = eval_expr(left, ctx)?;
            let rhs = eval_expr(right, ctx)?;
            let lhs = to_f64(&lhs)
                .ok_or_else(|| EvalError("left operand for // must be numeric".to_string()))?;
            let rhs = to_f64(&rhs)
                .ok_or_else(|| EvalError("right operand for // must be numeric".to_string()))?;
            if rhs == 0.0 {
                return Err(EvalError("division by zero".to_string()));
            }
            let result = (lhs / rhs).floor() as i64;
            Ok(Value::Number(result.into()))
        }
    }
}

fn eval_get(base: &Value, args: &[Expr], ctx: &EvalContext<'_>) -> Result<Value, EvalError> {
    let key_expr = args
        .first()
        .ok_or_else(|| EvalError("get() requires a key argument".to_string()))?;
    let key_value = eval_expr(key_expr, ctx)?;
    let key = key_value
        .as_str()
        .ok_or_else(|| EvalError("get() key must be a string".to_string()))?;
    let default_value = if args.len() > 1 {
        eval_expr(&args[1], ctx)?
    } else {
        Value::Null
    };
    match base {
        Value::Object(map) => Ok(map.get(key).cloned().unwrap_or(default_value)),
        Value::Null => Ok(default_value),
        _ => Ok(default_value),
    }
}

fn eval_string_method<F>(base: &Value, op: F) -> Result<Value, EvalError>
where
    F: FnOnce(&str) -> String,
{
    match base {
        Value::String(value) => Ok(Value::String(op(value))),
        Value::Null => Ok(Value::String(String::new())),
        other => Ok(Value::String(op(&other.to_string()))),
    }
}

fn parse_number(raw: &str) -> Result<Value, EvalError> {
    if raw.contains('.') {
        let value: f64 =
            raw.parse().map_err(|_| EvalError(format!("invalid number literal '{raw}'")))?;
        let json = serde_json::Number::from_f64(value)
            .ok_or_else(|| EvalError(format!("invalid float literal '{raw}'")))?;
        Ok(Value::Number(json))
    } else {
        let value: i64 =
            raw.parse().map_err(|_| EvalError(format!("invalid number literal '{raw}'")))?;
        Ok(Value::Number(value.into()))
    }
}

fn to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, index: 0 }
    }

    fn parse_expr(&mut self) -> Result<Expr, EvalError> {
        self.parse_int_div()
    }

    fn parse_int_div(&mut self) -> Result<Expr, EvalError> {
        let mut expr = self.parse_postfix()?;
        while self.match_token(&Token::SlashSlash) {
            let rhs = self.parse_postfix()?;
            expr = Expr::IntDiv(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_postfix(&mut self) -> Result<Expr, EvalError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.match_token(&Token::Dot) {
                let name = self.expect_ident()?;
                if self.match_token(&Token::LParen) {
                    let args = self.parse_args()?;
                    expr = Expr::Call(Box::new(expr), name, args);
                } else {
                    expr = Expr::Attr(Box::new(expr), name);
                }
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_args(&mut self) -> Result<Vec<Expr>, EvalError> {
        let mut args = Vec::new();
        if self.match_token(&Token::RParen) {
            return Ok(args);
        }
        loop {
            let expr = self.parse_expr()?;
            args.push(expr);
            if self.match_token(&Token::Comma) {
                continue;
            }
            self.expect_token(&Token::RParen)?;
            break;
        }
        Ok(args)
    }

    fn parse_primary(&mut self) -> Result<Expr, EvalError> {
        match self.next_token() {
            Some(Token::LBrace) => self.parse_dict(),
            Some(Token::LBracket) => self.parse_list(),
            Some(Token::LParen) => {
                let expr = self.parse_expr()?;
                self.expect_token(&Token::RParen)?;
                Ok(expr)
            }
            Some(Token::String(value)) => Ok(Expr::String(value)),
            Some(Token::Number(raw)) => Ok(Expr::Number(raw)),
            Some(Token::True) => Ok(Expr::Bool(true)),
            Some(Token::False) => Ok(Expr::Bool(false)),
            Some(Token::Null) => Ok(Expr::Null),
            Some(Token::Ident(name)) => Ok(Expr::Var(name)),
            other => Err(EvalError(format!("unexpected token {:?}", other))),
        }
    }

    fn parse_dict(&mut self) -> Result<Expr, EvalError> {
        let mut pairs = Vec::new();
        if self.match_token(&Token::RBrace) {
            return Ok(Expr::Dict(pairs));
        }
        loop {
            let key = match self.next_token() {
                Some(Token::String(value)) => value,
                Some(Token::Ident(value)) => value,
                other => {
                    return Err(EvalError(format!(
                        "expected string key in dict, got {:?}",
                        other
                    )))
                }
            };
            self.expect_token(&Token::Colon)?;
            let value = self.parse_expr()?;
            pairs.push((key, value));
            if self.match_token(&Token::Comma) {
                continue;
            }
            self.expect_token(&Token::RBrace)?;
            break;
        }
        Ok(Expr::Dict(pairs))
    }

    fn parse_list(&mut self) -> Result<Expr, EvalError> {
        let mut items = Vec::new();
        if self.match_token(&Token::RBracket) {
            return Ok(Expr::List(items));
        }
        loop {
            items.push(self.parse_expr()?);
            if self.match_token(&Token::Comma) {
                continue;
            }
            self.expect_token(&Token::RBracket)?;
            break;
        }
        Ok(Expr::List(items))
    }

    fn match_token(&mut self, token: &Token) -> bool {
        if self.tokens.get(self.index) == Some(token) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect_token(&mut self, token: &Token) -> Result<(), EvalError> {
        if self.match_token(token) {
            Ok(())
        } else {
            Err(EvalError(format!("expected token {:?}", token)))
        }
    }

    fn expect_ident(&mut self) -> Result<String, EvalError> {
        match self.next_token() {
            Some(Token::Ident(name)) => Ok(name),
            other => Err(EvalError(format!("expected identifier, got {:?}", other))),
        }
    }

    fn next_token(&mut self) -> Option<Token> {
        if self.index >= self.tokens.len() {
            return None;
        }
        let token = self.tokens[self.index].clone();
        self.index += 1;
        Some(token)
    }

    fn is_done(&self) -> bool {
        self.index >= self.tokens.len()
    }
}

fn tokenize(input: &str) -> Result<Vec<Token>, EvalError> {
    let mut chars = input.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(&ch) = chars.peek() {
        match ch {
            '{' => {
                chars.next();
                tokens.push(Token::LBrace);
            }
            '}' => {
                chars.next();
                tokens.push(Token::RBrace);
            }
            '[' => {
                chars.next();
                tokens.push(Token::LBracket);
            }
            ']' => {
                chars.next();
                tokens.push(Token::RBracket);
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            ':' => {
                chars.next();
                tokens.push(Token::Colon);
            }
            ',' => {
                chars.next();
                tokens.push(Token::Comma);
            }
            '.' => {
                chars.next();
                tokens.push(Token::Dot);
            }
            '/' => {
                chars.next();
                if chars.peek() == Some(&'/') {
                    chars.next();
                    tokens.push(Token::SlashSlash);
                } else {
                    return Err(EvalError("unexpected '/'".to_string()));
                }
            }
            '#' => {
                chars.next();
                while let Some(&next) = chars.peek() {
                    if next == '\n' {
                        break;
                    }
                    chars.next();
                }
            }
            '"' | '\'' => {
                tokens.push(Token::String(parse_string(&mut chars)?));
            }
            ch if ch.is_ascii_whitespace() => {
                chars.next();
            }
            ch if ch.is_ascii_digit() || ch == '-' => {
                tokens.push(Token::Number(parse_number_token(&mut chars)));
            }
            _ => {
                if is_ident_start(ch) {
                    let ident = parse_ident(&mut chars);
                    let token = match ident.as_str() {
                        "true" => Token::True,
                        "false" => Token::False,
                        "null" | "None" => Token::Null,
                        _ => Token::Ident(ident),
                    };
                    tokens.push(token);
                } else {
                    return Err(EvalError(format!("unexpected character '{ch}'")));
                }
            }
        }
    }
    Ok(tokens)
}

fn parse_string<I>(chars: &mut std::iter::Peekable<I>) -> Result<String, EvalError>
where
    I: Iterator<Item = char>,
{
    let quote = chars.next().ok_or_else(|| EvalError("unterminated string".to_string()))?;
    let mut result = String::new();
    while let Some(ch) = chars.next() {
        if ch == quote {
            return Ok(result);
        }
        if ch == '\\' {
            let escaped = chars
                .next()
                .ok_or_else(|| EvalError("unterminated escape sequence".to_string()))?;
            match escaped {
                'n' => result.push('\n'),
                't' => result.push('\t'),
                'r' => result.push('\r'),
                '\\' => result.push('\\'),
                '\'' => result.push('\''),
                '"' => result.push('"'),
                other => result.push(other),
            }
        } else {
            result.push(ch);
        }
    }
    Err(EvalError("unterminated string".to_string()))
}

fn parse_number_token<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    let mut result = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() || ch == '.' || ch == '-' {
            result.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    result
}

fn parse_ident<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    let mut result = String::new();
    while let Some(&ch) = chars.peek() {
        if is_ident_part(ch) {
            result.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    result
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_ident_part(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}
