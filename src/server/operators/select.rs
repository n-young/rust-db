use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Select {
    name: String,
    predicate: Predicate,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Predicate {
    name: String,
    condition: Conditions,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Conditions {
    Leaf(Condition),
    And(Box<Conditions>, Box<Conditions>),
    Or(Box<Conditions>, Box<Conditions>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Condition {
    lhs: Type,
    rhs: Type,
    op: Op,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Type {
    LabelKey(String),
    LabelValue(String),
    Variable(String),
    Metric(f64),
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
