use crate::server::operators::select::{Select, Predicate, Conditions, Condition};

pub fn dnf(s: Select) -> Select {
    let new_cond = dnf_helper(s.predicate.condition);
    let new_pred = Predicate { name: s.predicate.name, condition: new_cond };
    Select { name: s.name, predicate: new_pred }
}

fn dnf_helper(f: Conditions) -> Conditions {
    if is_all_and(f.clone()) {
        f
    } else {
        match f {
            Conditions::Leaf(_) => f,
            Conditions::Or(l, r) => Conditions::Or(Box::new(dnf_helper(*l)), Box::new(dnf_helper(*r))),
            Conditions::And(l, r) => {
                if is_or((*l).clone()) {
                    let (lp, rp) = pushdown_disjunction(*r, *l);
                    Conditions::Or(Box::new(dnf_helper(lp)), Box::new(dnf_helper(rp)))
                } else if is_or((*r).clone()) {
                    let (lp, rp) = pushdown_disjunction(*l, *r);
                    Conditions::Or(Box::new(dnf_helper(lp)), Box::new(dnf_helper(rp)))
                } else {
                    dnf_helper(Conditions::And(Box::new(dnf_helper(*l)), Box::new(dnf_helper(*r))))
                }
            }
        }
    }
}

fn pushdown_disjunction(x: Conditions, or: Conditions) -> (Conditions, Conditions) {
    match or {
        Conditions::Or(orl, orr) => {
            (Conditions::And(orl, Box::new(x.clone())), Conditions::And(orr, Box::new(x.clone())))
        }
        _ => panic!("Messed up!")
    }
}

fn is_or(f: Conditions) -> bool {
    match f {
        Conditions::Or(_, _) => true,
        _ => false
    }
}

fn is_all_and(f: Conditions) -> bool {
    match f {
        Conditions::Leaf(_) => true,
        Conditions::And(l, r) => is_all_and(*l) && is_all_and(*r),
        Conditions::Or(_, _) => false,
    }
}
