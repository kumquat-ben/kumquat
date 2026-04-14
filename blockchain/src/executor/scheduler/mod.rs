use std::collections::HashMap;

use crate::executor::PreparedTransaction;
use crate::storage::block_store::Hash;

#[derive(Clone, Debug)]
pub struct DependencyGraph {
    pub edges: Vec<Vec<usize>>,
    pub indegree: Vec<usize>,
}

impl DependencyGraph {
    pub(crate) fn build(transactions: &[PreparedTransaction]) -> Self {
        let mut edges = vec![Vec::new(); transactions.len()];
        let mut indegree = vec![0usize; transactions.len()];
        let mut last_seen_by_token = HashMap::<Hash, usize>::new();

        for (current_idx, tx) in transactions.iter().enumerate() {
            let mut added_dependencies = std::collections::HashSet::new();
            for input in &tx.declared_inputs {
                if let Some(previous_idx) = last_seen_by_token.insert(input.token_id, current_idx) {
                    if added_dependencies.insert(previous_idx) {
                        edges[previous_idx].push(current_idx);
                        indegree[current_idx] += 1;
                    }
                }
            }
        }

        Self { edges, indegree }
    }
}
