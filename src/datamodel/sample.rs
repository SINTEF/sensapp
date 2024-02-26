use super::SensAppDateTime;

#[derive(Debug, PartialEq)]
pub struct Sample<V> {
    pub datetime: SensAppDateTime,
    pub value: V,
}
