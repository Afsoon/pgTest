use std::{borrow::Borrow, fmt, ops::Deref, sync::Arc};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReadString(Arc<str>);

impl ReadString {
    pub fn new(value: String) -> Self {
        ReadString(Arc::from(value))
    }
}

impl From<String> for ReadString {
    fn from(value: String) -> Self {
        ReadString::new(value)
    }
}

impl From<&str> for ReadString {
    fn from(value: &str) -> Self {
        ReadString(Arc::from(value))
    }
}

impl Deref for ReadString {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ReadString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for ReadString {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ReadString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for ReadString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}
