function generate_condition() {
    return {
        Leaf: {
            lhs: { LabelKey: "Key" },
            rhs: { LabelValue: "Value" },
            op: "Eq"
        }
    }
}

for (i = 0; i < 1; i++) {
    const condition = generate_condition()
    const ret = {
        Select: {
            name: `sample ${i}`,
            predicate: {
                name: `predicate ${i}`,
                condition
            }
        }
    }
    console.log(JSON.stringify(ret))
}
