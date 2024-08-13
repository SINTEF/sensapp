use smallvec::SmallVec;

pub type SensAppVec<T> = SmallVec<[T; 4]>;

pub type SensAppLabels = SmallVec<[(String, String); 8]>;
