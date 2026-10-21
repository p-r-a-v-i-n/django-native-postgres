use crate::error::NativeError;
use deadpool_postgres::RecyclingMethod;

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

    pub(crate) fn recycling_method(self) -> RecyclingMethod {
        match self {
            Self::Fast => RecyclingMethod::Fast,
            Self::Clean => RecyclingMethod::Clean,
        }
    }

    pub(crate) fn restores_session(self) -> bool {
        matches!(self, Self::Clean)
    }
}
