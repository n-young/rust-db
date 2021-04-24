use crate::server::operators::select::*;

// Wrapper for DNF converter; converts a Select to DNF.
pub fn dnf(s: Select) -> Select {
    let new_cond = dnf_helper(s.predicate.condition);
    let new_pred = Predicate {
        name: s.predicate.name,
        condition: new_cond,
    };
    Select {
        name: s.name,
        predicate: new_pred,
    }
}

// Converts a predicate to DNF.
fn dnf_helper(f: Conditions) -> Conditions {
    // If the subtree has no Ors, terminate.
    if is_all_and(f.clone()) {
        f
    }
    // Otherwise, we need to do pushdown.
    else {
        match f {
            // If this is a Leaf, return.
            Conditions::Leaf(_) => f,

            // If this is an Or, then evaluate the children; we can safely skip this.
            Conditions::Or(l, r) => {
                Conditions::Or(Box::new(dnf_helper(*l)), Box::new(dnf_helper(*r)))
            }

            // If this is an And...
            Conditions::And(l, r) => {
                // If either left or right is an Or, then we distribute.
                if is_or((*l).clone()) {
                    let (lp, rp) = pushdown_disjunction(*r, *l);
                    Conditions::Or(Box::new(dnf_helper(lp)), Box::new(dnf_helper(rp)))
                } else if is_or((*r).clone()) {
                    let (lp, rp) = pushdown_disjunction(*l, *r);
                    Conditions::Or(Box::new(dnf_helper(lp)), Box::new(dnf_helper(rp)))
                }
                // If neither child of the And is an Or, then we need to revisit later.
                else {
                    dnf_helper(Conditions::And(
                        Box::new(dnf_helper(*l)),
                        Box::new(dnf_helper(*r)),
                    ))
                }
            }
        }
    }
}

// Generates the left and right after distributing x over or.
fn pushdown_disjunction(x: Conditions, or: Conditions) -> (Conditions, Conditions) {
    match or {
        Conditions::Or(orl, orr) => (
            Conditions::And(orl, Box::new(x.clone())),
            Conditions::And(orr, Box::new(x.clone())),
        ),
        _ => panic!("ERROR: Non-or passed into pushdown disjunction."),
    }
}

// Returns true if f is an Or.
fn is_or(f: Conditions) -> bool {
    match f {
        Conditions::Or(_, _) => true,
        _ => false,
    }
}

// Returns true if the subtree doesn't contain any Ors.
fn is_all_and(f: Conditions) -> bool {
    match f {
        Conditions::Leaf(_) => true,
        Conditions::And(l, r) => is_all_and(*l) && is_all_and(*r),
        Conditions::Or(_, _) => false,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_leaf() {
        let x = Condition {
            lhs: Type::LabelKey(String::from("X")),
            rhs: Type::LabelValue(String::from("X")),
            op: Op::Eq,
        };
        let leaf = Conditions::Leaf(x);
        assert_eq!(leaf.clone(), dnf_helper(leaf));
    }

    #[test]
    fn test_unit_and() {
        let x = Condition {
            lhs: Type::LabelKey(String::from("X")),
            rhs: Type::LabelValue(String::from("X")),
            op: Op::Eq,
        };
        let y = Condition {
            lhs: Type::LabelKey(String::from("Y")),
            rhs: Type::LabelValue(String::from("Y")),
            op: Op::Eq,
        };
        let cond = Conditions::And(Box::new(Conditions::Leaf(x)), Box::new(Conditions::Leaf(y)));
        assert_eq!(cond.clone(), dnf_helper(cond));
    }

    #[test]
    fn test_unit_or() {
        let x = Condition {
            lhs: Type::LabelKey(String::from("X")),
            rhs: Type::LabelValue(String::from("X")),
            op: Op::Eq,
        };
        let y = Condition {
            lhs: Type::LabelKey(String::from("Y")),
            rhs: Type::LabelValue(String::from("Y")),
            op: Op::Eq,
        };
        let cond = Conditions::Or(Box::new(Conditions::Leaf(x)), Box::new(Conditions::Leaf(y)));
        assert_eq!(cond.clone(), dnf_helper(cond));
    }

    #[test]
    fn test_nested_ands() {
        let a = Condition {
            lhs: Type::LabelKey(String::from("A")),
            rhs: Type::LabelValue(String::from("A")),
            op: Op::Eq,
        };
        let b = Condition {
            lhs: Type::LabelKey(String::from("B")),
            rhs: Type::LabelValue(String::from("B")),
            op: Op::Eq,
        };
        let c = Condition {
            lhs: Type::LabelKey(String::from("C")),
            rhs: Type::LabelValue(String::from("C")),
            op: Op::Eq,
        };
        let d = Condition {
            lhs: Type::LabelKey(String::from("D")),
            rhs: Type::LabelValue(String::from("D")),
            op: Op::Eq,
        };
        let e = Condition {
            lhs: Type::LabelKey(String::from("E")),
            rhs: Type::LabelValue(String::from("E")),
            op: Op::Eq,
        };
        let f = Condition {
            lhs: Type::LabelKey(String::from("F")),
            rhs: Type::LabelValue(String::from("F")),
            op: Op::Eq,
        };

        let and1 = Conditions::And(Box::new(Conditions::Leaf(a)), Box::new(Conditions::Leaf(b)));
        let and2 = Conditions::And(Box::new(Conditions::Leaf(c)), Box::new(Conditions::Leaf(d)));
        let and3 = Conditions::And(Box::new(Conditions::Leaf(e)), Box::new(Conditions::Leaf(f)));

        let and12 = Conditions::And(Box::new(and1), Box::new(and2));
        let and123 = Conditions::And(Box::new(and12), Box::new(and3));

        assert_eq!(and123.clone(), dnf_helper(and123));
    }

    #[test]
    fn test_nested_ors() {
        let a = Condition {
            lhs: Type::LabelKey(String::from("A")),
            rhs: Type::LabelValue(String::from("A")),
            op: Op::Eq,
        };
        let b = Condition {
            lhs: Type::LabelKey(String::from("B")),
            rhs: Type::LabelValue(String::from("B")),
            op: Op::Eq,
        };
        let c = Condition {
            lhs: Type::LabelKey(String::from("C")),
            rhs: Type::LabelValue(String::from("C")),
            op: Op::Eq,
        };
        let d = Condition {
            lhs: Type::LabelKey(String::from("D")),
            rhs: Type::LabelValue(String::from("D")),
            op: Op::Eq,
        };
        let e = Condition {
            lhs: Type::LabelKey(String::from("E")),
            rhs: Type::LabelValue(String::from("E")),
            op: Op::Eq,
        };
        let f = Condition {
            lhs: Type::LabelKey(String::from("F")),
            rhs: Type::LabelValue(String::from("F")),
            op: Op::Eq,
        };

        let or1 = Conditions::Or(Box::new(Conditions::Leaf(a)), Box::new(Conditions::Leaf(b)));
        let or2 = Conditions::Or(Box::new(Conditions::Leaf(c)), Box::new(Conditions::Leaf(d)));
        let or3 = Conditions::Or(Box::new(Conditions::Leaf(e)), Box::new(Conditions::Leaf(f)));

        let or12 = Conditions::Or(Box::new(or1), Box::new(or2));
        let or123 = Conditions::Or(Box::new(or12), Box::new(or3));

        assert_eq!(or123.clone(), dnf_helper(or123));
    }

    #[test]
    fn test_one_and_or() {
        let a = Condition {
            lhs: Type::LabelKey(String::from("A")),
            rhs: Type::LabelValue(String::from("A")),
            op: Op::Eq,
        };
        let b = Condition {
            lhs: Type::LabelKey(String::from("B")),
            rhs: Type::LabelValue(String::from("B")),
            op: Op::Eq,
        };
        let c = Condition {
            lhs: Type::LabelKey(String::from("C")),
            rhs: Type::LabelValue(String::from("C")),
            op: Op::Eq,
        };

        let or = Conditions::Or(
            Box::new(Conditions::Leaf(a.clone())),
            Box::new(Conditions::Leaf(b.clone())),
        );
        let cond = Conditions::And(Box::new(or), Box::new(Conditions::Leaf(c.clone())));

        let and1 = Conditions::And(
            Box::new(Conditions::Leaf(a.clone())),
            Box::new(Conditions::Leaf(c.clone())),
        );
        let and2 = Conditions::And(
            Box::new(Conditions::Leaf(b.clone())),
            Box::new(Conditions::Leaf(c.clone())),
        );
        let res = Conditions::Or(Box::new(and1), Box::new(and2));

        assert_eq!(res, dnf_helper(cond));
    }

    #[test]
    fn test_two_and_or() {
        let a = Condition {
            lhs: Type::LabelKey(String::from("A")),
            rhs: Type::LabelValue(String::from("A")),
            op: Op::Eq,
        };
        let b = Condition {
            lhs: Type::LabelKey(String::from("B")),
            rhs: Type::LabelValue(String::from("B")),
            op: Op::Eq,
        };
        let c = Condition {
            lhs: Type::LabelKey(String::from("C")),
            rhs: Type::LabelValue(String::from("C")),
            op: Op::Eq,
        };
        let d = Condition {
            lhs: Type::LabelKey(String::from("D")),
            rhs: Type::LabelValue(String::from("D")),
            op: Op::Eq,
        };

        let or1 = Conditions::Or(
            Box::new(Conditions::Leaf(a.clone())),
            Box::new(Conditions::Leaf(b.clone())),
        );
        let or2 = Conditions::Or(
            Box::new(Conditions::Leaf(c.clone())),
            Box::new(Conditions::Leaf(d.clone())),
        );
        let cond = Conditions::And(Box::new(or1), Box::new(or2));

        let and1 = Conditions::And(
            Box::new(Conditions::Leaf(c.clone())),
            Box::new(Conditions::Leaf(a.clone())),
        );
        let and2 = Conditions::And(
            Box::new(Conditions::Leaf(d.clone())),
            Box::new(Conditions::Leaf(a.clone())),
        );
        let and3 = Conditions::And(
            Box::new(Conditions::Leaf(c.clone())),
            Box::new(Conditions::Leaf(b.clone())),
        );
        let and4 = Conditions::And(
            Box::new(Conditions::Leaf(d.clone())),
            Box::new(Conditions::Leaf(b.clone())),
        );
        let res = Conditions::Or(
            Box::new(Conditions::Or(Box::new(and1), Box::new(and2))),
            Box::new(Conditions::Or(Box::new(and3), Box::new(and4))),
        );

        assert_eq!(res, dnf_helper(cond));
    }

    #[test]
    fn test_deep_or() {
        let a = Condition {
            lhs: Type::LabelKey(String::from("A")),
            rhs: Type::LabelValue(String::from("A")),
            op: Op::Eq,
        };
        let b = Condition {
            lhs: Type::LabelKey(String::from("B")),
            rhs: Type::LabelValue(String::from("B")),
            op: Op::Eq,
        };
        let c = Condition {
            lhs: Type::LabelKey(String::from("C")),
            rhs: Type::LabelValue(String::from("C")),
            op: Op::Eq,
        };
        let d = Condition {
            lhs: Type::LabelKey(String::from("D")),
            rhs: Type::LabelValue(String::from("D")),
            op: Op::Eq,
        };
        let e = Condition {
            lhs: Type::LabelKey(String::from("E")),
            rhs: Type::LabelValue(String::from("E")),
            op: Op::Eq,
        };
        let f = Condition {
            lhs: Type::LabelKey(String::from("F")),
            rhs: Type::LabelValue(String::from("F")),
            op: Op::Eq,
        };

        let or1 = Conditions::Or(Box::new(Conditions::Leaf(a)), Box::new(Conditions::Leaf(b)));
        let and2 = Conditions::And(Box::new(Conditions::Leaf(c)), Box::new(Conditions::Leaf(d)));
        let and3 = Conditions::And(Box::new(Conditions::Leaf(e)), Box::new(Conditions::Leaf(f)));

        let and12 = Conditions::And(Box::new(or1), Box::new(and2));
        let and123 = Conditions::And(Box::new(and12), Box::new(and3));
        // TODO: Test this??
    }

    #[test]
    fn test_deep_and() {
        let a = Condition {
            lhs: Type::LabelKey(String::from("A")),
            rhs: Type::LabelValue(String::from("A")),
            op: Op::Eq,
        };
        let b = Condition {
            lhs: Type::LabelKey(String::from("B")),
            rhs: Type::LabelValue(String::from("B")),
            op: Op::Eq,
        };
        let c = Condition {
            lhs: Type::LabelKey(String::from("C")),
            rhs: Type::LabelValue(String::from("C")),
            op: Op::Eq,
        };
        let d = Condition {
            lhs: Type::LabelKey(String::from("D")),
            rhs: Type::LabelValue(String::from("D")),
            op: Op::Eq,
        };
        let e = Condition {
            lhs: Type::LabelKey(String::from("E")),
            rhs: Type::LabelValue(String::from("E")),
            op: Op::Eq,
        };
        let f = Condition {
            lhs: Type::LabelKey(String::from("F")),
            rhs: Type::LabelValue(String::from("F")),
            op: Op::Eq,
        };

        let and1 = Conditions::And(Box::new(Conditions::Leaf(a)), Box::new(Conditions::Leaf(b)));
        let or2 = Conditions::Or(Box::new(Conditions::Leaf(c)), Box::new(Conditions::Leaf(d)));
        let or3 = Conditions::Or(Box::new(Conditions::Leaf(e)), Box::new(Conditions::Leaf(f)));

        let or12 = Conditions::Or(Box::new(and1), Box::new(or2));
        let or123 = Conditions::Or(Box::new(or12), Box::new(or3));
        // TODO: Test this??
    }
}
