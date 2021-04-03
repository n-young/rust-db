use crate::server::record::Record;
use crate::server::store::Block;
use priority_queue::PriorityQueue;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

// Predicate Struct.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Predicate {
    name: String,
    condition: Conditions,
}

// Select Struct.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Select {
    name: String,
    predicate: Predicate,
}
impl Select {
    pub fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> ResultSet {
        self.predicate.condition.eval(shared_block)
    }
}

// Type Enum.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Type {
    LabelKey(String),
    LabelValue(String),
    Variable(String),
    Metric(f64),
}
impl Type {
    fn to_string(&self) -> String {
        match self {
            Type::LabelKey(s) => String::from(s),
            Type::LabelValue(s) => String::from(s),
            Type::Variable(s) => String::from(s),
            Type::Metric(v) => format!("{}", v),
        }
    }

    fn extract_metric(&self) -> f64 {
        match self {
            Type::Metric(v) => *v,
            _ => panic!(),
        }
    }

    fn is_labelkey(&self) -> bool {
        match self {
            Type::LabelKey(_) => true,
            _ => false,
        }
    }

    fn is_labelvalue(&self) -> bool {
        match self {
            Type::LabelValue(_) => true,
            _ => false,
        }
    }

    fn is_variable(&self) -> bool {
        match self {
            Type::Variable(_) => true,
            _ => false,
        }
    }

    fn is_metric(&self) -> bool {
        match self {
            Type::Metric(_) => true,
            _ => false,
        }
    }
}

// Op Enum.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Op {
    Eq,
    NEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
}

fn make_filter(op: Box<dyn Fn(f64, f64) -> bool>, val: f64) -> Box<dyn Fn(f64) -> bool> {
    Box::new(move |x| op(x, val))
}

impl Op {
    // Returns a function that performs the given comparison.
    fn get_op(&self) -> Box<dyn Fn(f64, f64) -> bool> {
        match self {
            Op::Eq => Box::new(move |a, b| a == b),
            Op::NEq => Box::new(move |a, b| a != b),
            Op::Gt => Box::new(move |a, b| a > b),
            Op::Lt => Box::new(move |a, b| a < b),
            Op::GtEq => Box::new(move |a, b| a >= b),
            Op::LtEq => Box::new(move |a, b| a <= b),
        }
    }
}

// Conditions Enum.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Conditions {
    Leaf(Condition),
    And(Box<Conditions>, Box<Conditions>),
    Or(Box<Conditions>, Box<Conditions>),
}
impl Conditions {
    fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> ResultSet {
        match self {
            // If a Leaf, return results.
            Conditions::Leaf(cond) => cond.eval(shared_block),
            // If an And, intersect the results.
            Conditions::And(b1, b2) => {
                let mut r1 = (*b1).eval(shared_block);
                let r2 = (*b2).eval(shared_block);
                r1.intersection(r2, shared_block);
                r1.unpack(shared_block);
                r1
            }
            // If an or, union the results.
            Conditions::Or(b1, b2) => {
                let mut r1 = (*b1).eval(shared_block);
                let r2 = (*b2).eval(shared_block);
                r1.union(r2, shared_block);
                r1.unpack(shared_block);
                r1
            }
        }
    }
}

// Condition Struct.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Condition {
    lhs: Type,
    rhs: Type,
    op: Op,
}

impl Condition {
    fn eval(&self, shared_block: &Arc<RwLock<Block>>) -> ResultSet {
        // In the label=value case, the only operation is equality.
        if self.lhs.is_labelkey() && self.rhs.is_labelvalue() {
            match self.op {
                // Equality case.
                Op::Eq => {
                    // Get label and block.
                    let label = format!("{}={}", self.lhs.to_string(), self.rhs.to_string());
                    let block = shared_block.read().expect("RwLock poisoned");

                    // Append all relevant series given the label key and value.
                    if let Some(rb) = block.search_index(label) {
                        return ResultSet {
                            unpacked: false,
                            data: vec![],
                            series: rb.clone(),
                            filters: vec![],
                        };
                    }
                    return ResultSet {
                        unpacked: false,
                        data: vec![],
                        series: RoaringBitmap::new(),
                        filters: vec![],
                    };
                }
                // No other cases are permitted.
                _ => ResultSet {
                    unpacked: false,
                    data: vec![],
                    series: RoaringBitmap::new(),
                    filters: vec![],
                },
            }
        }
        // In the variable=metric case, we extract the operation and iterate.
        else if self.lhs.is_variable() && self.rhs.is_metric() {
            // Get block.
            let metric = self.lhs.to_string();
            let block = shared_block.read().expect("RwLock poisoned");

            // Return all blocks that contain the given metric
            if let Some(rb) = block.search_index(metric.clone()) {
                let op = self.op.get_op();
                let filter = make_filter(op, self.rhs.extract_metric());
                return ResultSet {
                    unpacked: false,
                    data: vec![],
                    series: rb.clone(),
                    // Delay filtering until the set is unpacked
                    filters: vec![(metric, filter)],
                };
            }
            return ResultSet {
                unpacked: false,
                data: vec![],
                series: RoaringBitmap::new(),
                filters: vec![],
            };
        }
        // We disallow everything that isn't label=value or variable=metric.
        else {
            panic!();
        }
    }
}

// ResultSet Struct.
pub struct ResultSet {
    unpacked: bool,
    data: Vec<Record>, // Assumed sorted.
    series: RoaringBitmap,
    filters: Vec<(String, Box<dyn Fn(f64) -> bool>)>,
}

fn pass_filters(record: &Record, filters: &Vec<(String, Box<dyn Fn(f64) -> bool>)>) -> bool {
    for (metric, filter) in filters.iter() {
        match record.get_metric(metric.to_string()) {
            Some(val) => {
                if !filter(*val) {
                    return false;
                }
            }
            None => return false,
        }
    }
    return true;
}

impl ResultSet {
    pub fn unpack(&mut self, shared_block: &Arc<RwLock<Block>>) {
        // Unpacking happens only once
        if self.unpacked {
            return;
        }
        let block = shared_block.read().expect("RwLock poisoned");
        let mut data: Vec<Record> = vec![];
        let mut all_series: Vec<Vec<Record>> = vec![];
        let mut positions: Vec<usize> = vec![];
        let mut pq = PriorityQueue::new();
        let mut counter: usize = 0;
        // Get pointers to the start of all relevant series
        for id in self.series.iter() {
            let series = block.get_storage()[id as usize]
                .records
                .read()
                .expect("RwLock poisoned");
            all_series.push(series.clone());
            positions.push(0);
            pq.push(counter, series[0].get_timestamp());
            counter += 1;
        }
        // Perform a timestamp-sorted multi-merge of all series
        while !pq.is_empty() {
            match pq.pop() {
                Some((i, _)) => {
                    let pos = positions[i];
                    let entry = &all_series[i][pos];
                    // Apply filters as we go
                    if pass_filters(&entry, &self.filters) {
                        // Check that we haven't seen this data point before
                        if data.len() == 0 || *entry != data[data.len() - 1] {
                            data.push(entry.clone());
                        }
                    }
                    // Advance the pointer for this series
                    positions[i] += 1;
                    if pos + 1 >= all_series[i].len() {
                        continue;
                    }
                    pq.push(i, all_series[i][pos + 1].get_timestamp());
                }
                None => continue,
            }
        }
        // TODO: does this mutate ourself?
        self.data = data;
        self.unpacked = true;
    }

    // Union two RSs. Assumes both are sorted by timestamp.
    pub fn union(&mut self, mut other: ResultSet, shared_block: &Arc<RwLock<Block>>) {
        // Check if both sets are unpacked and filterless
        if !self.unpacked && !other.unpacked && self.filters.len() == 0 && other.filters.len() == 0
        {
            self.series.union_with(&other.series);
            return;
        }
        // Unpack both sets
        self.unpack(shared_block);
        other.unpack(shared_block);
        let mut res = Vec::with_capacity(self.data.len() + other.data.len());
        let mut i = 0;
        let mut j = 0;

        // Until one of our iterators reaches the end.
        while i < self.data.len() && j < other.data.len() {
            if self.data[i].get_timestamp() < other.data[j].get_timestamp() {
                res.push(self.data[i].clone());
                i += 1;
            } else if self.data[i].get_timestamp() > other.data[j].get_timestamp() {
                res.push(other.data[j].clone());
                j += 1;
            } else if self.data[i] == other.data[j] {
                res.push(self.data[i].clone());
                i += 1;
                j += 1;
            }
        }

        // Handle residual, return.
        res.append(&mut Vec::from(self.data.get(i..).unwrap()));
        res.append(&mut Vec::from(other.data.get(j..).unwrap()));
        self.data = res;
    }

    // Intersect two RSs. Assumes both are sorted by timestamp.
    pub fn intersection(&mut self, mut other: ResultSet, shared_block: &Arc<RwLock<Block>>) {
        // Check if both result sets are unpacked
        if !self.unpacked && !other.unpacked {
            self.series.intersect_with(&other.series);
            self.filters.append(&mut Vec::from(other.filters));
            return;
        }
        // If either result set is unpacked, unpack the other
        self.unpack(shared_block);
        other.unpack(shared_block);
        let mut res = Vec::with_capacity(self.data.len() + other.data.len());
        let mut i = 0;
        let mut j = 0;

        // Until one of our iterators reaches the end.
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
        self.data = res;
    }

    // Converts a ResultSet into a Vector.
    pub fn into_vec(&self) -> Vec<Record> {
        self.data.clone()
    }
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
