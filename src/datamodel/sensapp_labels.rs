use std::collections::BTreeMap;

use hybridmap::HybridMap;
use smallvec::SmallVec;

pub type SensAppLabels = SmallVec<[(String, String); 8]>;

pub trait SensAppLabelsExt {
    fn build_with_iterators<'a>(
        context_iterator: Option<impl Iterator<Item = (&'a String, &'a String)>>,
        labels_iterator: Option<impl Iterator<Item = (String, String)>>,
    ) -> Option<Self>
    where
        Self: std::marker::Sized;

    fn build_with_context<'a>(
        context_reference: &Option<HybridMap<String, String>>,
        labels_iterator: Option<impl Iterator<Item = (String, String)>>,
    ) -> Option<Self>
    where
        Self: std::marker::Sized;
}

impl SensAppLabelsExt for SensAppLabels {
    fn build_with_iterators<'a>(
        context_iterator: Option<impl Iterator<Item = (&'a String, &'a String)>>,
        labels_iterator: Option<impl Iterator<Item = (String, String)>>,
    ) -> Option<Self> {
        if let Some(context_iterator) = context_iterator {
            let mut labels_builder = BTreeMap::new();

            for (key, value) in context_iterator {
                labels_builder.insert(key.clone(), value.clone());
            }
            if let Some(labels_iterator) = labels_iterator {
                for (key, value) in labels_iterator {
                    let mut key_with_prefix = key;
                    while labels_builder.contains_key(&key_with_prefix) {
                        key_with_prefix.insert(0, '_');
                    }
                    labels_builder.insert(key_with_prefix, value);
                }
            }

            Some(labels_builder.into_iter().collect())
        } else {
            labels_iterator.map(|labels_iterator| labels_iterator.collect())
        }
    }

    fn build_with_context<'a>(
        context_reference: &Option<HybridMap<String, String>>,
        labels_iterator: Option<impl Iterator<Item = (String, String)>>,
    ) -> Option<Self> {
        Self::build_with_iterators(
            context_reference.as_ref().map(|context| context.iter()),
            labels_iterator,
        )
    }
}
