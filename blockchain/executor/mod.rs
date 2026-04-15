use dashmap::DashMap;
use rayon::ThreadPoolBuilder;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage::block_store::Hash;
use crate::storage::state::{AccountState, AccountType, CoinInventory, DenominationToken};
use crate::storage::tx_store::TransactionRecord;

pub mod scheduler;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredTokenInput {
    pub token_id: Hash,
    pub expected_version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionRoute {
    FastPath,
    Consensus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionRejection {
    InvalidNonce,
    UndeclaredInputs,
    BundleTooLarge,
    MissingFeeToken,
    MissingFee,
    EmptyTransferSet,
    DuplicateToken,
    MissingToken,
    InsufficientBalance,
    StaleVersion,
    Unauthorized,
    LockConflict,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionStatus {
    Applied,
    Rejected(ExecutionRejection),
}

#[derive(Clone, Debug)]
pub struct ExecutionOutcome {
    pub tx_id: Hash,
    pub route: TransactionRoute,
    pub status: ExecutionStatus,
}

#[derive(Clone, Debug)]
struct PreparedTransaction {
    original_index: usize,
    tx: TransactionRecord,
    declared_inputs: Vec<DeclaredTokenInput>,
    route: TransactionRoute,
}

#[derive(Clone, Debug)]
pub struct BatchExecutionResult {
    pub accounts: Vec<(Hash, AccountState)>,
    pub outcomes: Vec<ExecutionOutcome>,
}

struct TokenRuntime {
    token: DenominationToken,
    lock_holder: Option<Hash>,
    lock_acquired_at: u64,
}

struct ConcurrentTokenStore {
    tokens: DashMap<Hash, Arc<Mutex<TokenRuntime>>>,
    accounts: DashMap<Hash, Arc<Mutex<AccountState>>>,
    lock_timeout_secs: u64,
}

impl ConcurrentTokenStore {
    fn new(accounts: &[(Hash, AccountState)]) -> Self {
        let token_store = DashMap::new();
        let account_store = DashMap::new();

        for (address, account) in accounts {
            let mut account_copy = account.clone();
            account_copy.tokens.clear();
            account_copy.balance = account_copy.total_account_value();
            account_store.insert(*address, Arc::new(Mutex::new(account_copy)));

            for token in &account.tokens {
                if !token.denomination().is_bill() {
                    continue;
                }
                token_store.insert(
                    token.token_id,
                    Arc::new(Mutex::new(TokenRuntime {
                        token: token.clone(),
                        lock_holder: None,
                        lock_acquired_at: 0,
                    })),
                );
            }
        }

        Self {
            tokens: token_store,
            accounts: account_store,
            lock_timeout_secs: 30,
        }
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn get_or_create_account(&self, address: Hash, block_height: u64) -> Arc<Mutex<AccountState>> {
        if let Some(existing) = self.accounts.get(&address) {
            return existing.clone();
        }

        let account = Arc::new(Mutex::new(AccountState::new_user(0, block_height)));
        self.accounts.insert(address, account.clone());
        account
    }

    fn cleanup_stale_lock(&self, runtime: &mut TokenRuntime, now: u64) {
        if runtime.lock_holder.is_some()
            && now.saturating_sub(runtime.lock_acquired_at) >= self.lock_timeout_secs
        {
            runtime.lock_holder = None;
            runtime.lock_acquired_at = 0;
        }
    }

    fn execute_prepared(
        &self,
        prepared: &PreparedTransaction,
        fee_recipient: &Hash,
        block_height: u64,
    ) -> Result<(), ExecutionRejection> {
        let mut lock_ids = prepared
            .declared_inputs
            .iter()
            .map(|input| input.token_id)
            .collect::<Vec<_>>();
        lock_ids.sort();
        lock_ids.dedup();

        let mut token_entries = Vec::with_capacity(lock_ids.len());
        for token_id in &lock_ids {
            let token_entry = self
                .tokens
                .get(token_id)
                .map(|entry| entry.clone())
                .ok_or(ExecutionRejection::MissingToken)?;
            token_entries.push((*token_id, token_entry));
        }

        let now = Self::current_timestamp();
        let mut guards = Vec::with_capacity(token_entries.len());
        for (_, token_entry) in &token_entries {
            let mut guard = token_entry.lock().unwrap();
            self.cleanup_stale_lock(&mut guard, now);
            if let Some(holder) = guard.lock_holder {
                if holder != prepared.tx.tx_id {
                    return Err(ExecutionRejection::LockConflict);
                }
            }
            guard.lock_holder = Some(prepared.tx.tx_id);
            guard.lock_acquired_at = now;
            guards.push(guard);
        }

        let mut guard_index = HashMap::new();
        for (idx, guard) in guards.iter().enumerate() {
            guard_index.insert(guard.token.token_id, idx);
        }

        for input in &prepared.declared_inputs {
            let guard = &guards[*guard_index
                .get(&input.token_id)
                .ok_or(ExecutionRejection::MissingToken)?];
            if guard.token.version != input.expected_version {
                return Err(ExecutionRejection::StaleVersion);
            }
            if guard.token.owner != prepared.tx.sender {
                return Err(ExecutionRejection::Unauthorized);
            }
        }

        let sender_account = self.get_or_create_account(prepared.tx.sender, block_height);
        let recipient_account = self.get_or_create_account(prepared.tx.recipient, block_height);
        let fee_account = self.get_or_create_account(*fee_recipient, block_height);

        {
            let mut sender = sender_account.lock().unwrap();
            if sender.nonce != prepared.tx.nonce {
                return Err(ExecutionRejection::InvalidNonce);
            }
            if !sender.coin_inventory.can_cover(&prepared.tx.coin_transfer)
                || !sender.coin_inventory.can_cover(&prepared.tx.coin_fee)
            {
                return Err(ExecutionRejection::InsufficientBalance);
            }
            sender
                .coin_inventory
                .remove_inventory(&prepared.tx.coin_transfer)
                .map_err(|_| ExecutionRejection::InsufficientBalance)?;
            sender
                .coin_inventory
                .remove_inventory(&prepared.tx.coin_fee)
                .map_err(|_| ExecutionRejection::InsufficientBalance)?;
            sender.nonce += 1;
            sender.last_updated = block_height;
            sender.sync_balance_from_hybrid();
        }
        {
            let mut recipient = recipient_account.lock().unwrap();
            recipient
                .coin_inventory
                .add_inventory(&prepared.tx.coin_transfer)
                .map_err(|_| ExecutionRejection::InsufficientBalance)?;
            recipient.last_updated = block_height;
            recipient.sync_balance_from_hybrid();
        }
        {
            let mut fee = fee_account.lock().unwrap();
            fee.coin_inventory
                .add_inventory(&prepared.tx.coin_fee)
                .map_err(|_| ExecutionRejection::InsufficientBalance)?;
            fee.last_updated = block_height;
            fee.sync_balance_from_hybrid();
        }

        for token_id in &prepared.tx.transfer_token_ids {
            let guard = &mut guards[*guard_index.get(token_id).unwrap()];
            if guard.token.owner != prepared.tx.recipient {
                guard.token.owner = prepared.tx.recipient;
                guard.token.version += 1;
            }
        }

        if let Some(fee_token_id) = prepared.tx.fee_token_id {
            let guard = &mut guards[*guard_index.get(&fee_token_id).unwrap()];
            if guard.token.owner != *fee_recipient {
                guard.token.owner = *fee_recipient;
                guard.token.version += 1;
            }
        }

        for guard in &mut guards {
            guard.lock_holder = None;
            guard.lock_acquired_at = 0;
        }

        Ok(())
    }

    fn mint_block_reward_tokens(&self, miner: Hash, block_height: u64, block_hash: &Hash) {
        let miner_account = self.get_or_create_account(miner, block_height);
        {
            let mut miner_state = miner_account.lock().unwrap();
            miner_state.last_updated = block_height;
        }

        for token in crate::storage::block_store::reward_outcome(miner, block_height, block_hash) {
            self.tokens.insert(
                token.token_id,
                Arc::new(Mutex::new(TokenRuntime {
                    token,
                    lock_holder: None,
                    lock_acquired_at: 0,
                })),
            );
        }
    }

    fn materialize_accounts(&self) -> Vec<(Hash, AccountState)> {
        let mut token_buckets: HashMap<Hash, Vec<DenominationToken>> = HashMap::new();
        for entry in self.tokens.iter() {
            let runtime = entry.value().lock().unwrap();
            token_buckets
                .entry(runtime.token.owner)
                .or_default()
                .push(runtime.token.clone());
        }

        let mut addresses = HashSet::new();
        for entry in self.accounts.iter() {
            addresses.insert(*entry.key());
        }
        for owner in token_buckets.keys() {
            addresses.insert(*owner);
        }

        let mut materialized = Vec::with_capacity(addresses.len());
        for address in addresses {
            let mut account = self
                .accounts
                .get(&address)
                .map(|entry| entry.lock().unwrap().clone())
                .unwrap_or_else(|| AccountState::new_user(0, 0));
            let mut tokens = token_buckets.remove(&address).unwrap_or_default();
            tokens.sort_by_key(|token| token.token_id);
            account.tokens = tokens.clone();
            account.bills = tokens
                .into_iter()
                .filter_map(|token| crate::storage::BillToken::try_from(token).ok())
                .collect();
            account.sync_balance_from_hybrid();
            materialized.push((address, account));
        }

        materialized
    }
}

fn classify_transaction(
    tx: &TransactionRecord,
    accounts: &HashMap<Hash, AccountState>,
) -> TransactionRoute {
    let sender_type = accounts
        .get(&tx.sender)
        .map(|account| account.account_type)
        .unwrap_or(AccountType::User);
    let recipient_type = accounts
        .get(&tx.recipient)
        .map(|account| account.account_type)
        .unwrap_or(AccountType::User);

    if sender_type != AccountType::User || recipient_type != AccountType::User {
        TransactionRoute::Consensus
    } else {
        TransactionRoute::FastPath
    }
}

fn prepare_transaction(
    tx: &TransactionRecord,
    original_index: usize,
    accounts: &HashMap<Hash, AccountState>,
) -> Result<PreparedTransaction, ExecutionRejection> {
    let sender = accounts
        .get(&tx.sender)
        .ok_or(ExecutionRejection::InsufficientBalance)?;

    if tx.nonce != sender.nonce {
        return Err(ExecutionRejection::InvalidNonce);
    }
    if tx.transfer_token_ids.is_empty() && tx.coin_transfer.is_empty() {
        return Err(ExecutionRejection::EmptyTransferSet);
    }
    let fee_token_id = tx.fee_token_id;
    if fee_token_id.is_none() && tx.coin_fee.is_empty() {
        return Err(ExecutionRejection::MissingFee);
    }

    let mut touched = tx.transfer_token_ids.clone();
    if let Some(fee_token_id) = fee_token_id {
        touched.push(fee_token_id);
    }
    if touched.len() > 20 {
        return Err(ExecutionRejection::BundleTooLarge);
    }

    let mut seen = HashSet::new();
    let mut declared_inputs = Vec::with_capacity(touched.len());
    for token_id in touched {
        if !seen.insert(token_id) {
            return Err(ExecutionRejection::DuplicateToken);
        }
        let expected_version = sender
            .tokens
            .iter()
            .find(|token| token.token_id == token_id && token.denomination().is_bill())
            .map(|token| token.version)
            .ok_or(ExecutionRejection::MissingToken)?;
        declared_inputs.push(DeclaredTokenInput {
            token_id,
            expected_version,
        });
    }

    if !sender.coin_inventory.can_cover(&tx.coin_transfer) || !sender.coin_inventory.can_cover(&tx.coin_fee) {
        return Err(ExecutionRejection::InsufficientBalance);
    }

    let bill_total = tx
        .transfer_token_ids
        .iter()
        .filter_map(|token_id| sender.token_value(token_id))
        .sum::<u64>();
    if bill_total + tx.coin_transfer.total_value_cents() != tx.value {
        return Err(ExecutionRejection::UndeclaredInputs);
    }

    Ok(PreparedTransaction {
        original_index,
        tx: tx.clone(),
        declared_inputs,
        route: classify_transaction(tx, accounts),
    })
}

pub fn execute_transaction_batch(
    accounts: &[(Hash, AccountState)],
    transactions: &[TransactionRecord],
    miner: &Hash,
    block_height: u64,
    block_hash: Option<&Hash>,
) -> BatchExecutionResult {
    let account_map = accounts.iter().cloned().collect::<HashMap<_, _>>();
    let mut outcomes = transactions
        .iter()
        .map(|tx| ExecutionOutcome {
            tx_id: tx.tx_id,
            route: classify_transaction(tx, &account_map),
            status: ExecutionStatus::Rejected(ExecutionRejection::UndeclaredInputs),
        })
        .collect::<Vec<_>>();

    let mut prepared = Vec::new();
    for (index, tx) in transactions.iter().enumerate() {
        match prepare_transaction(tx, index, &account_map) {
            Ok(tx) => {
                outcomes[index] = ExecutionOutcome {
                    tx_id: tx.tx.tx_id,
                    route: tx.route,
                    status: ExecutionStatus::Applied,
                };
                prepared.push(tx);
            }
            Err(reason) => {
                outcomes[index] = ExecutionOutcome {
                    tx_id: tx.tx_id,
                    route: classify_transaction(tx, &account_map),
                    status: ExecutionStatus::Rejected(reason),
                };
            }
        }
    }

    let token_store = Arc::new(ConcurrentTokenStore::new(accounts));
    let graph = scheduler::DependencyGraph::build(&prepared);
    let pool = ThreadPoolBuilder::new()
        .num_threads(num_cpus::get().max(1))
        .build()
        .expect("parallel scheduler thread pool must initialize");
    let (completion_tx, completion_rx) = mpsc::channel();

    let mut indegree = graph.indegree.clone();
    let mut ready_queue = VecDeque::new();
    for (idx, degree) in indegree.iter().enumerate() {
        if *degree == 0 {
            ready_queue.push_back(idx);
        }
    }

    let mut in_flight = 0usize;
    let mut completed = 0usize;

    let submit_ready = |queue: &mut VecDeque<usize>, in_flight: &mut usize| {
        while let Some(node_idx) = queue.pop_front() {
            let prepared_tx = prepared[node_idx].clone();
            let store = token_store.clone();
            let tx = completion_tx.clone();
            let fee_recipient = *miner;
            pool.spawn(move || {
                let result = store.execute_prepared(&prepared_tx, &fee_recipient, block_height);
                let _ = tx.send((node_idx, result));
            });
            *in_flight += 1;
        }
    };

    submit_ready(&mut ready_queue, &mut in_flight);
    while completed < prepared.len() {
        let Ok((node_idx, result)) = completion_rx.recv() else {
            break;
        };
        in_flight = in_flight.saturating_sub(1);
        completed += 1;

        let original_index = prepared[node_idx].original_index;
        outcomes[original_index].status = match result {
            Ok(()) => ExecutionStatus::Applied,
            Err(reason) => ExecutionStatus::Rejected(reason),
        };

        for dependent in &graph.edges[node_idx] {
            indegree[*dependent] -= 1;
            if indegree[*dependent] == 0 {
                ready_queue.push_back(*dependent);
            }
        }
        submit_ready(&mut ready_queue, &mut in_flight);
    }

    if let Some(block_hash) = block_hash {
        token_store.mint_block_reward_tokens(*miner, block_height, block_hash);
    }

    BatchExecutionResult {
        accounts: token_store.materialize_accounts(),
        outcomes,
    }
}
