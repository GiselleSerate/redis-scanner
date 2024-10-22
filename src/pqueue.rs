use crate::argv::Sort;
use parking_lot::Mutex;
use std::{
  collections::{BTreeMap, BTreeSet, HashMap},
  fmt::Debug,
};

pub trait HashAndOrd: Debug {
  fn weight(&self) -> i64;
  // hack to avoid self-referential struct
  fn int_hash(&self) -> u64;
  fn merge(&mut self, other: Self);
}

struct Inner<T: HashAndOrd> {
  pub sorted: BTreeMap<i64, BTreeSet<u64>>,
  pub index:  HashMap<u64, T>,
}

impl<T: HashAndOrd> Inner<T> {
  pub fn remove_sorted(&mut self, weight: i64, int_hash: u64) {
    let should_remove = if let Some(inner) = self.sorted.get_mut(&weight) {
      inner.remove(&int_hash);
      inner.is_empty()
    } else {
      false
    };

    if should_remove {
      self.sorted.remove(&weight);
    }
  }
}

pub struct PrioQueue<T: HashAndOrd + Clone> {
  inner:    Mutex<Inner<T>>,
  sort:     Sort,
  max_size: usize,
}

impl<T: HashAndOrd + Clone> PrioQueue<T> {
  pub fn new(sort: Sort, max: usize) -> Self {
    PrioQueue {
      inner: Mutex::new(Inner {
        sorted: BTreeMap::new(),
        index:  HashMap::with_capacity(max),
      }),
      max_size: max,
      sort,
    }
  }

  pub fn deep_copy(&self) -> Self {
    let guard = self.inner.lock();

    PrioQueue {
      inner:    Mutex::new(Inner {
        index:  guard.index.iter().map(|(a, b)| (*a, b.clone())).collect(),
        sorted: guard.sorted.iter().map(|(a, b)| (*a, b.clone())).collect(),
      }),
      sort:     self.sort.clone(),
      max_size: self.max_size,
    }
  }

  pub fn push_or_update(&self, val: T) -> Option<T> {
    let mut inner = self.inner.lock();
    let old_len = inner.index.len();
    let int_hash = val.int_hash();

    if let Some(old_val) = inner.index.get_mut(&int_hash) {
      // update in place
      let old_weight = old_val.weight();
      old_val.merge(val);
      let new_weight = old_val.weight();

      // remove old weight -> hash index record
      inner.remove_sorted(old_weight, int_hash);
      // add pointer to new index record
      inner.sorted.entry(new_weight).or_default().insert(int_hash);
      None
    } else if self.max_size != 0 && old_len >= self.max_size {
      // insert and remove one element
      let to_remove = match self.sort {
        Sort::Asc => inner.sorted.last_key_value().and_then(|(_, v)| v.last().cloned()),
        Sort::Desc => inner.sorted.first_key_value().and_then(|(_, v)| v.last().cloned()),
      };
      let old = if let Some(to_remove) = to_remove {
        if let Some(old) = inner.index.remove(&to_remove) {
          let should_swap = match self.sort {
            Sort::Asc => val.weight() < old.weight(),
            Sort::Desc => val.weight() > old.weight(),
          };
          if !should_swap {
            inner.index.insert(to_remove, old);
            return None;
          }

          let old_weight = old.weight();
          let old_int_hash = old.int_hash();
          inner.remove_sorted(old_weight, old_int_hash);
          Some(old)
        } else {
          None
        }
      } else {
        None
      };

      let new_weight = val.weight();
      let new_int_hash = val.int_hash();
      inner.index.insert(new_int_hash, val);
      inner.sorted.entry(new_weight).or_default().insert(new_int_hash);
      old
    } else {
      // insert new element
      let weight = val.weight();
      let int_hash = val.int_hash();
      inner.index.insert(int_hash, val);
      inner.sorted.entry(weight).or_default().insert(int_hash);
      None
    }
  }

  pub fn into_vec(self) -> Vec<T> {
    let mut inner = self.inner.lock();
    let mut out = Vec::with_capacity(inner.index.len());

    while !inner.index.is_empty() {
      let int_hashes = match self.sort {
        Sort::Asc => inner.sorted.pop_first().map(|(_, v)| v).unwrap_or_default(),
        Sort::Desc => inner.sorted.pop_last().map(|(_, v)| v).unwrap_or_default(),
      };

      for int_hash in int_hashes.into_iter() {
        if let Some(val) = inner.index.remove(&int_hash) {
          out.push(val);
        }
      }
    }
    out
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::argv::Sort;
  use std::hash::{DefaultHasher, Hash, Hasher};

  #[derive(Debug, Clone, Eq, PartialEq)]
  struct FakeVal {
    pub _weight: i64,
    pub _key:    String,
  }

  impl FakeVal {
    pub fn new<S: Into<String>>(w: i64, k: S) -> Self {
      FakeVal {
        _key:    k.into(),
        _weight: w,
      }
    }
  }

  impl HashAndOrd for FakeVal {
    fn weight(&self) -> i64 {
      self._weight
    }

    fn int_hash(&self) -> u64 {
      let mut h = DefaultHasher::new();
      self._key.hash(&mut h);
      h.finish()
    }

    fn merge(&mut self, other: Self) {
      self._weight += other._weight;
    }
  }

  #[test]
  fn should_pqueue_asc_lt_max() {
    let pqueue = PrioQueue::new(Sort::Asc, 10);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![
      FakeVal::new(1, "a"),
      FakeVal::new(2, "b"),
      FakeVal::new(3, "c")
    ]);
  }

  #[test]
  fn should_pqueue_desc_lt_max() {
    let pqueue = PrioQueue::new(Sort::Desc, 10);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![
      FakeVal::new(3, "c"),
      FakeVal::new(2, "b"),
      FakeVal::new(1, "a")
    ]);
  }

  #[test]
  fn should_pqueue_asc_gt_max() {
    let pqueue = PrioQueue::new(Sort::Asc, 2);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![FakeVal::new(1, "a"), FakeVal::new(2, "b"),]);
  }

  #[test]
  fn should_pqueue_desc_gt_max() {
    let pqueue = PrioQueue::new(Sort::Desc, 2);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![FakeVal::new(3, "c"), FakeVal::new(2, "b"),]);
  }

  #[test]
  fn should_pqueue_asc_lt_max_update() {
    let pqueue = PrioQueue::new(Sort::Asc, 10);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));
    pqueue.push_or_update(FakeVal::new(4, "b"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![
      FakeVal::new(1, "a"),
      FakeVal::new(3, "c"),
      FakeVal::new(6, "b")
    ]);
  }

  #[test]
  fn should_pqueue_desc_lt_max_update() {
    let pqueue = PrioQueue::new(Sort::Desc, 10);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));
    pqueue.push_or_update(FakeVal::new(4, "b"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![
      FakeVal::new(6, "b"),
      FakeVal::new(3, "c"),
      FakeVal::new(1, "a")
    ]);
  }

  #[test]
  fn should_pqueue_asc_lt_max_dupe_weights() {
    let pqueue = PrioQueue::new(Sort::Asc, 10);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(2, "e"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![
      FakeVal::new(1, "a"),
      FakeVal::new(2, "e"),
      FakeVal::new(2, "b"),
      FakeVal::new(3, "c")
    ]);
  }

  #[test]
  fn should_pqueue_desc_lt_max_dupe_weights() {
    let pqueue = PrioQueue::new(Sort::Desc, 10);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(2, "e"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![
      FakeVal::new(3, "c"),
      FakeVal::new(2, "e"),
      FakeVal::new(2, "b"),
      FakeVal::new(1, "a")
    ]);
  }

  #[test]
  fn should_pqueue_asc_gt_max_dupe_weights() {
    let pqueue = PrioQueue::new(Sort::Asc, 2);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(2, "e"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![FakeVal::new(1, "a"), FakeVal::new(2, "e"),]);
  }

  #[test]
  fn should_pqueue_desc_gt_max_dupe_weights() {
    let pqueue = PrioQueue::new(Sort::Desc, 2);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(2, "e"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![FakeVal::new(3, "c"), FakeVal::new(2, "e"),]);
  }

  #[test]
  fn should_pqueue_asc_gt_max_dupe_weights_2() {
    let pqueue = PrioQueue::new(Sort::Asc, 3);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(2, "e"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![
      FakeVal::new(1, "a"),
      FakeVal::new(2, "e"),
      FakeVal::new(2, "b")
    ]);
  }

  #[test]
  fn should_pqueue_desc_gt_max_dupe_weights_2() {
    let pqueue = PrioQueue::new(Sort::Desc, 3);
    pqueue.push_or_update(FakeVal::new(2, "b"));
    pqueue.push_or_update(FakeVal::new(2, "e"));
    pqueue.push_or_update(FakeVal::new(1, "a"));
    pqueue.push_or_update(FakeVal::new(3, "c"));

    let out = pqueue.into_vec();
    assert_eq!(out, vec![
      FakeVal::new(3, "c"),
      FakeVal::new(2, "e"),
      FakeVal::new(2, "b")
    ]);
  }
}
