extern crate bincode;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// Record struct.
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Record {
    name: String,
    labels: HashMap<String, String>,
    variables: HashMap<String, f64>,
    timestamp: DateTime<Utc>,
}

impl Record {
    // Constructor.
    pub fn new(
        name: String,
        labels: HashMap<String, String>,
        variables: HashMap<String, f64>,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Record {
            name,
            labels,
            variables,
            timestamp,
        }
    }

    // Get key.
    pub fn get_key(&self) -> String {
        // Start with name.
        let mut temp_key: String = self.name.clone();

        // Sort labels and add to key.
        let mut sorted_labels: Vec<_> = self.labels.iter().collect();
        sorted_labels.sort_by_key(|x| x.0);
        for (key, value) in sorted_labels.iter() {
            temp_key.push_str(key);
            temp_key.push_str(value);
        }

        // Sort variables and add to key.
        let mut sorted_variables: Vec<_> = self.get_variables().clone();
        sorted_variables.sort();
        for variable in sorted_variables.iter() {
            temp_key.push_str(variable);
        }

        // Return.
        temp_key
    }

    // Get name,
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    // Get populated labels.
    pub fn get_populated_labels(&self) -> HashMap<String, String> {
        self.labels.clone()
    }

    // Get labels.
    pub fn get_labels(&self) -> Vec<String> {
        self.labels
            .iter()
            .map(|(x, y)| format!("{}={}", x, y))
            .collect()
    }

    // Get populated variables.
    pub fn get_populated_variables(&self) -> HashMap<String, f64> {
        self.variables.clone()
    }

    // Get variables.
    pub fn get_variables(&self) -> Vec<String> {
        self.variables
            .iter()
            .map(|(x, y)| format!("{}={}", x, y))
            .collect()
    }

    // Get a particular metric.
    pub fn get_metric(&self, key: String) -> Option<&f64> {
        self.variables.get(&key)
    }

    // Get metrics.
    pub fn get_metrics(&self) -> Vec<String> {
        self.variables
            .iter()
            .map(|(x, _y)| format!("{}", x))
            .collect()
    }

    // Get timestamp.
    pub fn get_timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    // Convert to bytes.
    pub fn into_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    // Convert from bytes.
    pub fn from_bytes(data: &[u8]) -> Self {
        bincode::deserialize(data).unwrap()
    }
}
impl fmt::Display for Record {
    // Formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{name: {}, timestamp: {}, labels: {:#?}, variables: {:#?}}}",
            self.name, self.timestamp, self.labels, self.variables
        )
    }
}
impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.get_labels().hash(state);
        self.get_variables().hash(state);
        self.timestamp.hash(state);
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use chrono::{DateTime, Utc};

    #[test]
    fn test_deser_record() {
        let data = r#"{
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
        }"#;
        let d: Record = serde_json::from_str(data).unwrap();
        let mut labels = HashMap::new();
        labels.insert("hostname".to_string(), "host_0".to_string());
        labels.insert("region".to_string(), "us-west-1".to_string());
        labels.insert("service".to_string(), "9".to_string());

        let mut variables = HashMap::new();
        variables.insert("usage_user".to_string(), 58.0);
        variables.insert("usage_system".to_string(), 2.0);
        let exp = Record {
            name: "cpu".to_string(),
            labels: labels,
            variables: variables,
            timestamp: DateTime::parse_from_rfc3339("2016-06-13T17:43:50.1004002+00:00")
                .unwrap()
                .with_timezone(&Utc),
        };

        assert_eq!(exp, d);
    }
}
