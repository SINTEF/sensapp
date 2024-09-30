// Note that this file is not automatically generated from the .proto file.
use crate::datamodel::matchers::{
    LabelMatcher as SensAppLabelMatcher, SensorMatcher, StringMatcher,
};

#[derive(prost::Message)]
pub struct ReadRequest {
    #[prost(message, repeated, tag = "1")]
    pub queries: Vec<Query>,
    #[prost(enumeration = "ResponseType", repeated, tag = "2")]
    pub accepted_response_types: Vec<i32>,
}

#[derive(prost::Message)]
pub struct Query {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(message, optional, tag = "4")]
    pub hints: Option<ReadHints>,
}

#[derive(prost::Message)]
pub struct LabelMatcher {
    #[prost(enumeration = "label_matcher::Type", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub value: String,
}

#[derive(prost::Message)]
pub struct ReadHints {
    #[prost(int64, tag = "1")]
    pub step_ms: i64,
    #[prost(string, tag = "2")]
    pub func: String,
    #[prost(int64, tag = "3")]
    pub start_ms: i64,
    #[prost(int64, tag = "4")]
    pub end_ms: i64,
    #[prost(string, repeated, tag = "5")]
    pub grouping: Vec<String>,
    #[prost(bool, tag = "6")]
    pub by: bool,
    #[prost(int64, tag = "7")]
    pub range_ms: i64,
}

pub mod label_matcher {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        Eq = 0,
        Neq = 1,
        Re = 2,
        Nre = 3,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ResponseType {
    Samples = 0,
    StreamedXorChunks = 1,
}

impl From<&LabelMatcher> for StringMatcher {
    fn from(label_matcher: &LabelMatcher) -> Self {
        match label_matcher::Type::try_from(label_matcher.r#type).unwrap() {
            label_matcher::Type::Eq => StringMatcher::Equal(label_matcher.value.clone()),
            label_matcher::Type::Neq => StringMatcher::NotEqual(label_matcher.value.clone()),
            label_matcher::Type::Re => StringMatcher::Match(label_matcher.value.clone()),
            label_matcher::Type::Nre => StringMatcher::NotMatch(label_matcher.value.clone()),
        }
    }
}

impl From<&LabelMatcher> for SensAppLabelMatcher {
    fn from(label_matcher: &LabelMatcher) -> Self {
        SensAppLabelMatcher::new(label_matcher.name.clone(), label_matcher.into())
    }
}

impl Query {
    pub fn to_sensor_matcher(&self) -> SensorMatcher {
        let mut name_matcher = StringMatcher::All;
        let mut label_matchers: Vec<SensAppLabelMatcher> = Vec::new();

        for matcher in &self.matchers {
            if matcher.name == "__name__" {
                name_matcher = matcher.into();
            } else {
                label_matchers.push(matcher.into());
            }
        }

        SensorMatcher::new(
            name_matcher,
            if label_matchers.is_empty() {
                None
            } else {
                Some(label_matchers)
            },
        )
    }
}
