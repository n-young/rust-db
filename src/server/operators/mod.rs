pub mod select;
pub mod process;

use serde::{Deserialize, Serialize};
use crate::server::record::Record;
pub use select::Select;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Op {
    Select(select::Select),
    Write(Record),
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;

    #[test]
    fn test_deser_record() {
        let data = r#"{ "Write" : {
                "name": "cpu",
                "labels": {
                    "hostname": "host_0",
                    "region": "us-west-1",
                    "service": "9"
                },
                "variables": {
                    "usage_user": 58.0,
                    "usage_system": 2.0
                },
                "timestamp": "2016-06-13T17:43:50.1004002+00:00"
        }}"#;

        let mut labels = HashMap::new();
        labels.insert("hostname".to_string(), "host_0".to_string());
        labels.insert("region".to_string(), "us-west-1".to_string());
        labels.insert("service".to_string(), "9".to_string());
        let mut variables = HashMap::new();
        variables.insert("usage_user".to_string(), 58.0);
        variables.insert("usage_system".to_string(), 2.0);
        let timestamp = DateTime::parse_from_rfc3339("2016-06-13T17:43:50.1004002+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let d: Op = serde_json::from_str(data).unwrap();
        let exp = Op::Write(Record::new("cpu".to_string(), labels, variables, timestamp));
        assert_eq!(d, exp);
    }
}
