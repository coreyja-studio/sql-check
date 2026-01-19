//! Type mappings between PostgreSQL and Rust.

use std::fmt;

/// PostgreSQL data types we support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresType {
    // Numeric types
    SmallInt,        // int2
    Integer,         // int4
    BigInt,          // int8
    Real,            // float4
    DoublePrecision, // float8
    Numeric,         // numeric/decimal

    // Character types
    Text,
    Varchar(Option<u32>),
    Char(Option<u32>),

    // Binary
    Bytea,

    // Boolean
    Boolean,

    // Date/Time
    Timestamp,
    TimestampTz,
    Date,
    Time,
    TimeTz,
    Interval,

    // UUID
    Uuid,

    // JSON
    Json,
    Jsonb,

    // Network
    Inet,
    Cidr,
    MacAddr,

    // Arrays (element type)
    Array(Box<PostgresType>),

    // Custom/unknown types
    Custom(String),
}

impl PostgresType {
    /// Parse a PostgreSQL type name into a PostgresType.
    pub fn from_sql_name(name: &str) -> Self {
        let name_lower = name.to_lowercase();
        let name_lower = name_lower.trim();

        // Handle array types first
        if name_lower.ends_with("[]") {
            let element_type = Self::from_sql_name(&name_lower[..name_lower.len() - 2]);
            return PostgresType::Array(Box::new(element_type));
        }

        // Handle "ARRAY" suffix
        if let Some(base) = name_lower.strip_suffix(" array") {
            let element_type = Self::from_sql_name(base);
            return PostgresType::Array(Box::new(element_type));
        }

        match &*name_lower {
            // Numeric
            "smallint" | "int2" => PostgresType::SmallInt,
            "integer" | "int" | "int4" => PostgresType::Integer,
            "bigint" | "int8" => PostgresType::BigInt,
            "real" | "float4" => PostgresType::Real,
            "double precision" | "float8" => PostgresType::DoublePrecision,
            "numeric" | "decimal" => PostgresType::Numeric,

            // Character
            "text" => PostgresType::Text,
            "character varying" | "varchar" => PostgresType::Varchar(None),
            "character" | "char" => PostgresType::Char(None),

            // Binary
            "bytea" => PostgresType::Bytea,

            // Boolean
            "boolean" | "bool" => PostgresType::Boolean,

            // Date/Time
            "timestamp without time zone" | "timestamp" => PostgresType::Timestamp,
            "timestamp with time zone" | "timestamptz" => PostgresType::TimestampTz,
            "date" => PostgresType::Date,
            "time without time zone" | "time" => PostgresType::Time,
            "time with time zone" | "timetz" => PostgresType::TimeTz,
            "interval" => PostgresType::Interval,

            // UUID
            "uuid" => PostgresType::Uuid,

            // JSON
            "json" => PostgresType::Json,
            "jsonb" => PostgresType::Jsonb,

            // Network
            "inet" => PostgresType::Inet,
            "cidr" => PostgresType::Cidr,
            "macaddr" => PostgresType::MacAddr,

            // Handle varchar(n), char(n)
            s if s.starts_with("character varying") || s.starts_with("varchar") => {
                PostgresType::Varchar(parse_length(s))
            }
            s if s.starts_with("character") || s.starts_with("char") => {
                PostgresType::Char(parse_length(s))
            }

            // Unknown/custom
            other => PostgresType::Custom(other.to_string()),
        }
    }

    /// Get the corresponding Rust type for this PostgreSQL type.
    pub fn to_rust_type(&self) -> RustType {
        match self {
            PostgresType::SmallInt => RustType::I16,
            PostgresType::Integer => RustType::I32,
            PostgresType::BigInt => RustType::I64,
            PostgresType::Real => RustType::F32,
            PostgresType::DoublePrecision => RustType::F64,
            PostgresType::Numeric => RustType::Decimal,

            PostgresType::Text | PostgresType::Varchar(_) | PostgresType::Char(_) => {
                RustType::String
            }

            PostgresType::Bytea => RustType::VecU8,

            PostgresType::Boolean => RustType::Bool,

            PostgresType::Timestamp | PostgresType::TimestampTz => RustType::DateTime,
            PostgresType::Date => RustType::Date,
            PostgresType::Time | PostgresType::TimeTz => RustType::Time,
            PostgresType::Interval => RustType::Duration,

            PostgresType::Uuid => RustType::Uuid,

            PostgresType::Json | PostgresType::Jsonb => RustType::JsonValue,

            PostgresType::Inet | PostgresType::Cidr => RustType::IpAddr,
            PostgresType::MacAddr => RustType::String,

            PostgresType::Array(elem) => RustType::Vec(Box::new(elem.to_rust_type())),

            PostgresType::Custom(name) => RustType::Custom(name.clone()),
        }
    }
}

/// Parse length from types like "varchar(255)" or "char(10)"
fn parse_length(s: &str) -> Option<u32> {
    if let Some(start) = s.find('(') {
        if let Some(end) = s.find(')') {
            return s[start + 1..end].parse().ok();
        }
    }
    None
}

/// Rust types that we generate for query results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RustType {
    // Numeric
    I16,
    I32,
    I64,
    F32,
    F64,
    Decimal,

    // String/bytes
    String,
    VecU8,

    // Boolean
    Bool,

    // Date/Time
    DateTime,
    Date,
    Time,
    Duration,

    // UUID
    Uuid,

    // JSON
    JsonValue,

    // Network
    IpAddr,

    // Collections
    Vec(Box<RustType>),

    // Optional wrapper (for nullable columns)
    Option(Box<RustType>),

    // Custom/unknown
    Custom(String),
}

impl RustType {
    /// Wrap this type in Option if nullable.
    pub fn nullable(self) -> Self {
        RustType::Option(Box::new(self))
    }

    /// Returns the Rust type path for code generation.
    pub fn type_path(&self) -> String {
        match self {
            RustType::I16 => "i16".to_string(),
            RustType::I32 => "i32".to_string(),
            RustType::I64 => "i64".to_string(),
            RustType::F32 => "f32".to_string(),
            RustType::F64 => "f64".to_string(),
            RustType::Decimal => "rust_decimal::Decimal".to_string(),
            RustType::String => "String".to_string(),
            RustType::VecU8 => "Vec<u8>".to_string(),
            RustType::Bool => "bool".to_string(),
            RustType::DateTime => "chrono::DateTime<chrono::Utc>".to_string(),
            RustType::Date => "chrono::NaiveDate".to_string(),
            RustType::Time => "chrono::NaiveTime".to_string(),
            RustType::Duration => "chrono::Duration".to_string(),
            RustType::Uuid => "uuid::Uuid".to_string(),
            RustType::JsonValue => "serde_json::Value".to_string(),
            RustType::IpAddr => "std::net::IpAddr".to_string(),
            RustType::Vec(inner) => format!("Vec<{}>", inner.type_path()),
            RustType::Option(inner) => format!("Option<{}>", inner.type_path()),
            RustType::Custom(name) => name.clone(),
        }
    }
}

impl fmt::Display for RustType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.type_path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_type_parsing() {
        assert_eq!(
            PostgresType::from_sql_name("integer"),
            PostgresType::Integer
        );
        assert_eq!(PostgresType::from_sql_name("int4"), PostgresType::Integer);
        assert_eq!(PostgresType::from_sql_name("text"), PostgresType::Text);
        assert_eq!(PostgresType::from_sql_name("jsonb"), PostgresType::Jsonb);
        assert_eq!(PostgresType::from_sql_name("uuid"), PostgresType::Uuid);
        assert_eq!(
            PostgresType::from_sql_name("timestamp with time zone"),
            PostgresType::TimestampTz
        );
    }

    #[test]
    fn test_array_types() {
        assert_eq!(
            PostgresType::from_sql_name("text[]"),
            PostgresType::Array(Box::new(PostgresType::Text))
        );
        assert_eq!(
            PostgresType::from_sql_name("integer[]"),
            PostgresType::Array(Box::new(PostgresType::Integer))
        );
    }

    #[test]
    fn test_rust_type_mapping() {
        assert_eq!(PostgresType::Integer.to_rust_type(), RustType::I32);
        assert_eq!(PostgresType::Text.to_rust_type(), RustType::String);
        assert_eq!(PostgresType::Jsonb.to_rust_type(), RustType::JsonValue);
        assert_eq!(PostgresType::Uuid.to_rust_type(), RustType::Uuid);
    }

    #[test]
    fn test_rust_type_path() {
        assert_eq!(RustType::I32.type_path(), "i32");
        assert_eq!(RustType::String.type_path(), "String");
        assert_eq!(RustType::JsonValue.type_path(), "serde_json::Value");
        assert_eq!(RustType::Uuid.type_path(), "uuid::Uuid");
        assert_eq!(
            RustType::Option(Box::new(RustType::String)).type_path(),
            "Option<String>"
        );
    }
}
