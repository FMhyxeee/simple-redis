use crate::{RespArray, RespFrame};

use super::{extract_args, validate_command, CommandError, CommandExecutor, SAdd, SIsMember};

impl CommandExecutor for SAdd {
    fn execute(self, backend: &crate::Backend) -> crate::RespFrame {
        let response = self
            .members
            .into_iter()
            .map(|f| backend.sadd(self.key.clone(), f))
            .map(|b| RespFrame::Integer(b as i64))
            .collect();
        RespFrame::Array(RespArray(response))
    }
}


impl CommandExecutor for SIsMember {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        RespFrame::Integer(backend.sismember(&self.key, &self.member) as i64)
    }
}

impl TryFrom<RespArray> for SAdd {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let len = value.len();
        match len {
            0 => {
                return Err(CommandError::InvalidCommand(
                    "sadd command does not accept null array".to_string(),
                ))
            }
            1..=2 => {
                return Err(CommandError::InvalidCommand(format!(
                    "sadd command needs at least 2 argument, got {len}",
                )))
            }
            _ => validate_command(&value, &["sadd"], len - 1)?,
        }

        let mut args = extract_args(value, 1)?.into_iter();
        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => return Err(CommandError::InvalidArgument("Invalid key".to_string())),
        };
        let mut members = vec![];
        loop {
            match args.next() {
                Some(RespFrame::BulkString(key)) => members.push(String::from_utf8(key.0)?),
                None => break,
                _ => return Err(CommandError::InvalidArgument("Invalid key".to_string())),
            };
        }
        Ok(SAdd { key, members })
    }
}

impl TryFrom<RespArray> for SIsMember {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["sismember"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(member))) => {
                Ok(SIsMember {
                    key: String::from_utf8(key.0)?,
                    member: String::from_utf8(member.0)?,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or value".to_string(),
            )),
        }
    }
}