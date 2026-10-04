//! Keep every endpoint error and its original source through scenario cleanup.
use super::*;
use cellule_host::fleet::FleetAttemptFailure;

#[derive(Debug)]
struct EndpointFailures {
    phase: &'static str,
    pass: usize,
    failures: Vec<FleetAttemptFailure>,
}
impl std::fmt::Display for EndpointFailures {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            output,
            "{} failed at pass {}: {:?}",
            self.phase, self.pass, self.failures
        )
    }
}
impl std::error::Error for EndpointFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failures
            .first()
            .map(|failure| failure.error.as_ref() as _)
    }
}
pub(super) fn endpoints(
    phase: &'static str,
    pass: usize,
    failures: Vec<FleetAttemptFailure>,
) -> JournalError {
    Box::new(EndpointFailures {
        phase,
        pass,
        failures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellule_runtime::{
        Error,
        fleet::operations::{AttemptId, OperationId},
    };
    use std::error::Error as _;

    #[test]
    fn endpoint_failure_retains_all_attempts_and_original_source_through_boxing() {
        let first = Arc::new(Error::Facility {
            name: "original-first",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "first original cause",
            )),
        });
        let second = Arc::new(Error::Facility {
            name: "original-second",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "second original cause",
            )),
        });
        let first_id = AttemptId {
            operation: OperationId::from_bytes([217; 16]).unwrap(),
            sequence: 1,
        };
        let second_id = AttemptId {
            operation: OperationId::from_bytes([218; 16]).unwrap(),
            sequence: 2,
        };
        let error = endpoints(
            "real movement",
            3,
            vec![
                FleetAttemptFailure {
                    attempt: first_id,
                    error: first.clone(),
                },
                FleetAttemptFailure {
                    attempt: second_id,
                    error: second.clone(),
                },
            ],
        );
        let retained = error.downcast_ref::<EndpointFailures>().unwrap();
        assert_eq!(retained.failures[0].attempt, first_id);
        assert_eq!(retained.failures[1].attempt, second_id);
        assert!(Arc::ptr_eq(&retained.failures[0].error, &first));
        assert!(Arc::ptr_eq(&retained.failures[1].error, &second));
        let original = error.source().unwrap().downcast_ref::<Error>().unwrap();
        assert!(std::ptr::eq(original, first.as_ref()));
        assert_eq!(
            original
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::ConnectionReset
        );
        let message = error.to_string();
        assert!(message.contains("real movement failed at pass 3"));
        assert!(message.contains("first original cause"));
        assert!(message.contains("second original cause"));
    }
}
