use crate::server::record::Record;
use crate::server::store::Block;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Select {
    name: String,
    predicate: Predicate,
}

impl Select {
    pub fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> ResultSet {
        self.predicate.eval(shared_block)
    }
}

#[derive(Debug, Clone)]
pub struct ResultSet {
    // Assumed sorted by timestamp; must maintain invariant.
    data: Vec<Record>
}

impl ResultSet {
    // Assumes both are sorted by timestamp.
    pub fn union(&self, other: ResultSet) -> ResultSet {
        let mut res = Vec::with_capacity(self.data.len() + other.data.len());
        let mut i = 0;
        let mut j = 0;
        while i < self.data.len() && j < other.data.len() {
            if self.data[i].get_timestamp() < other.data[j].get_timestamp() {
                res.push(self.data[i].clone());
                i += 1;
            } else if self.data[i].get_timestamp() > other.data[j].get_timestamp() {
                res.push(other.data[i].clone());
                j += 1;
            } else if self.data[i] == other.data[j] {
                res.push(self.data[i].clone());
                i += 1;
                j += 1;
            }
                
        }
        res.append(&mut Vec::from(self.data.get(i..).unwrap()));
        res.append(&mut Vec::from(other.data.get(j..).unwrap()));
        ResultSet { data: res }
    }

    // Assumes both are sorted by timestamp.
    pub fn intersection(&self, other: ResultSet) -> ResultSet {
        let mut res = Vec::with_capacity(self.data.len() + other.data.len());
        let mut i = 0;
        let mut j = 0;
        while i < self.data.len() && j < other.data.len() {
            if self.data[i].get_timestamp() < other.data[j].get_timestamp() {
                i += 1;
            } else if self.data[i].get_timestamp() > other.data[j].get_timestamp() {
                j += 1;
            } else if self.data[i] == other.data[j] {
                res.push(self.data[i].clone());
                i += 1;
                j += 1;
            }
                
        }
        ResultSet { data: res }
    }

    pub fn into_vec(&self) -> Vec<Record> {
        self.data.clone()
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Predicate {
    name: String,
    condition: Conditions,
}

impl Predicate {
    fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> ResultSet {
        self.condition.eval(shared_block)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Conditions {
    Leaf(Condition),
    And(Box<Conditions>, Box<Conditions>),
    Or(Box<Conditions>, Box<Conditions>),
}

impl Conditions {
    fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> ResultSet {
        match self {
            Conditions::Leaf(cond) => cond.eval(shared_block),
            Conditions::And(b1, b2) => {
                let results1 = (*b1).eval(shared_block);
                let results2 = (*b2).eval(shared_block);
                results1.union(results2)
            }
            Conditions::Or(b1, b2) => {
                let results1 = (*b1).eval(shared_block);
                let results2 = (*b2).eval(shared_block);
                results1.intersection(results2)
            }
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Condition {
    lhs: Type,
    rhs: Type,
    op: Op,
}

impl Condition {
    fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> ResultSet {
        match self.op {
            Op::Eq => {
                let label = format!("{}={}", self.lhs.eval(), self.rhs.eval());
                let mut results: Vec<Record> = vec![];
                let block = shared_block.read().expect("RwLock poisoned");
                if let Some(rb) = block.index.get(&label) {
                    for id in rb.iter() {
                        let series = block.storage[id as usize]
                            .records
                            .read()
                            .expect("RwLock poisoned");
                        results.append(&mut series.clone());
                    }
                }
                ResultSet { data: results }
            }
            _ => ResultSet { data: vec![] },
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Type {
    LabelKey(String),
    LabelValue(String),
    Variable(String),
    Metric(f64),
}

impl Type {
    fn eval(&self) -> String {
        match self {
            Type::LabelKey(s) => String::from(s),
            Type::LabelValue(s) => String::from(s),
            Type::Variable(s) => String::from(s),
            Type::Metric(v) => format!("{}", v),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Op {
    Eq,
    NEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn deserialize_condition() {
        let data = r#"
        {
            "lhs": {"LabelKey": "Key"},
            "rhs": {"LabelValue": "Value"},
            "op": "Eq"
        }
        "#;
        let d: Condition = serde_json::from_str(data).unwrap();
        let exp = Condition {
            lhs: Type::LabelKey(String::from("Key")),
            rhs: Type::LabelValue(String::from("Value")),
            op: Op::Eq,
        };
        assert_eq!(d, exp);
    }

    #[test]
    fn deserialize_conditions() {
        let data = r#"
        {
            "Leaf": {
                "lhs": {"Variable": "Var"},
                "rhs": {"Metric": 6.0},
                "op": "Gt"
            }
        }
        "#;

        let d: Conditions = serde_json::from_str(data).unwrap();
        let exp = Conditions::Leaf(Condition {
            lhs: Type::Variable(String::from("Var")),
            rhs: Type::Metric(6.0),
            op: Op::Gt,
        });
        assert_eq!(d, exp);

        let data = r#"
        {
            "And": [
                {
                    "Leaf": { "lhs": {"LabelKey": "Key"}, "rhs": {"LabelValue": "Value"}, "op": "Eq" }
                },
                {
                    "Leaf": { "lhs": {"Variable": "Var"}, "rhs": {"Metric": 6.0}, "op": "Gt" }
                }
            ]
        }
        "#;
        let d: Conditions = serde_json::from_str(data).unwrap();
        let exp = Conditions::And(
            Box::new(Conditions::Leaf(Condition {
                lhs: Type::LabelKey(String::from("Key")),
                rhs: Type::LabelValue(String::from("Value")),
                op: Op::Eq,
            })),
            Box::new(Conditions::Leaf(Condition {
                lhs: Type::Variable(String::from("Var")),
                rhs: Type::Metric(6.0),
                op: Op::Gt,
            })),
        );
        assert_eq!(d, exp);
    }
}
