use crate::error::NativeError;

#[derive(Clone, Copy)]
pub(crate) enum SessionReset {
    Fast,
    Clean,
}

impl SessionReset {
    const FAST: &'static str = "fast";
    const CLEAN: &'static str = "clean";

    pub(crate) fn parse(value: String) -> Result<Self, NativeError> {
        match value.as_str() {
            Self::FAST => Ok(Self::Fast),
            Self::CLEAN => Ok(Self::Clean),
            _ => Err(NativeError::InvalidSessionReset(value)),
        }
    }

    pub(crate) fn cleanup_query(self) -> Option<&'static str> {
        match self {
            Self::Fast => None,
            Self::Clean => Some(
                "CLOSE ALL; \
                 SET SESSION AUTHORIZATION DEFAULT; \
                 RESET ALL; \
                 UNLISTEN *; \
                 SELECT pg_advisory_unlock_all(); \
                 DISCARD TEMP; \
                 DISCARD SEQUENCES;",
            ),
        }
    }
}
