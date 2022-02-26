pub struct Select {
    fields: Vec<Field>,
    table: Table,
    conditions: Vec<Condition>,
}

pub struct Field;
pub struct Value;

pub struct Condition {
    field: Field,
    value: Value,
}

pub struct Table {
    name: String,
}
