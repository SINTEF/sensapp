use smallvec::SmallVec;

pub type SensAppVec<T> = SmallVec<[T; 1]>;

pub type SensAppLabels = SmallVec<[(String, String); 8]>;
