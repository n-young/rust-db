use crate::server::record::Record;
use crate::server::store::Block;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::{Arc, RwLock};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Select {
    name: String,
    predicate: Predicate,
}

impl Select {
    pub fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> Vec<Record> {
        self.predicate.eval(shared_block)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Predicate {
    name: String,
    condition: Conditions,
}

impl Predicate {
    fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> Vec<Record> {
        self.condition.eval(shared_block).into_iter().collect()
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Conditions {
    Leaf(Condition),
    And(Box<Conditions>, Box<Conditions>),
    Or(Box<Conditions>, Box<Conditions>),
}

impl Conditions {
    fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> HashSet<Record> {
        match self {
            Conditions::Leaf(cond) => cond.eval(shared_block),
            Conditions::And(b1, b2) => {
                let results1 = (*b1).eval(shared_block);
                let results2 = (*b2).eval(shared_block);
                results1.union(&results2).cloned().collect()
            }
            Conditions::Or(b1, b2) => {
                let results1 = (*b1).eval(shared_block);
                let results2 = (*b2).eval(shared_block);
                results1.intersection(&results2).cloned().collect()
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
    fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> HashSet<Record> {
        match self.op {
            Op::Eq => {
                let label = format!("{}={}", self.lhs.eval(), self.rhs.eval());
                let mut results: HashSet<Record> = HashSet::new();
                let block = shared_block.read().expect("RwLock poisoned");
                if let Some(rb) = block.index.get(&label) {
                    for id in rb.iter() {
                        let series = block.storage[id as usize]
                            .records
                            .read()
                            .expect("RwLock poisoned");
                        let additional_results: HashSet<Record> =
                            HashSet::from_iter(series.iter().cloned());
                        results.extend(additional_results);
                    }
                }
                results
            }
            _ => HashSet::new(),
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
