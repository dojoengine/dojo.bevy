//! Dojo v2 plugin using native Bevy tasks instead of external Tokio runtime.
//!
//! This plugin provides the same functionality as the original Dojo plugin but uses
//! Bevy's native task system for better integration and performance.

use bevy::prelude::*;
use bevy::tasks::{IoTaskPool, Task};
use crossbeam_channel::{Receiver, Sender, unbounded};
use dojo_types::schema::Struct;
use futures::StreamExt;
use futures::lock::Mutex;
use starknet::accounts::single_owner::SignError;
use starknet::accounts::{Account, AccountError, ExecutionEncoding, SingleOwnerAccount};
use starknet::core::types::{BlockId, BlockTag, Call, InvokeTransactionResult};
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider};
use starknet::signers::local_wallet::SignError as LocalWalletSignError;
use starknet::signers::{LocalWallet, SigningKey};
use starknet::{core::types::Felt, providers::AnyProvider};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use torii_grpc_client::WorldClient;
use torii_grpc_client::types::proto::world::RetrieveEntitiesResponse;
use torii_grpc_client::types::{Clause, Query as ToriiQuery};
use url::Url;

/// Represents the state of a subscription task
pub struct SubscriptionTaskState {
    pub task: Task<()>,
    pub is_active: bool,
}

/// The Dojo v2 plugin using native Bevy tasks.
pub struct DojoPluginV2;

impl Plugin for DojoPluginV2 {
    fn build(&self, app: &mut App) {
        app.add_event::<DojoInitializedEventV2>();
        app.add_event::<DojoEntityUpdatedV2>();
        app.add_systems(Update, (check_torii_task_v2, check_sn_task_v2));
    }
}

/// Event emitted when Dojo v2 is initialized.
#[derive(Event)]
pub struct DojoInitializedEventV2;

/// Event emitted when an entity is updated from Torii.
#[derive(Event, Debug)]
pub struct DojoEntityUpdatedV2 {
    pub entity_id: Felt,
    pub models: Vec<Struct>,
}

/// Starknet connection state using Bevy tasks.
#[derive(Default)]
pub struct StarknetConnectionV2 {
    pub connecting_task: Option<Task<Arc<SingleOwnerAccount<AnyProvider, LocalWallet>>>>,
    pub account: Option<Arc<SingleOwnerAccount<AnyProvider, LocalWallet>>>,
    pub pending_txs: VecDeque<
        Task<Result<InvokeTransactionResult, AccountError<SignError<LocalWalletSignError>>>>,
    >,
}

/// Torii connection state using Bevy tasks.
#[derive(Default)]
pub struct ToriiConnectionV2 {
    pub init_task: Option<Task<Result<WorldClient, torii_grpc_client::Error>>>,
    pub client: Option<Arc<Mutex<WorldClient>>>,
    pub pending_retrieve_entities:
        VecDeque<Task<Result<RetrieveEntitiesResponse, torii_grpc_client::Error>>>,
    pub subscriptions: Arc<Mutex<HashMap<String, SubscriptionTaskState>>>,
    pub subscription_sender: Option<Sender<(Felt, Vec<Struct>)>>,
    pub subscription_receiver: Option<Receiver<(Felt, Vec<Struct>)>>,
    pub pending_subscription_stores: VecDeque<Task<Result<(), String>>>,
}

/// Main Dojo resource using Bevy tasks.
#[derive(Resource, Default)]
pub struct DojoResourceV2 {
    pub sn: StarknetConnectionV2,
    pub torii: ToriiConnectionV2,
}

impl DojoResourceV2 {
    /// Connects to Torii using Bevy tasks.
    pub fn connect_torii(&mut self, torii_url: String, world_address: Felt) {
        info!("Connecting to Torii (v2).");
        let task_pool = IoTaskPool::get();
        let task = task_pool.spawn(async move { WorldClient::new(torii_url, world_address).await });
        self.torii.init_task = Some(task);

        let (sender, receiver) = unbounded();
        self.torii.subscription_sender = Some(sender);
        self.torii.subscription_receiver = Some(receiver);
    }

    /// Connects to a Starknet account using Bevy tasks.
    pub fn connect_account(&mut self, rpc_url: String, account_addr: Felt, private_key: Felt) {
        info!("Connecting to Starknet (v2).");
        let task_pool = IoTaskPool::get();
        let task = task_pool
            .spawn(async move { connect_to_starknet_v2(rpc_url, account_addr, private_key).await });
        self.sn.connecting_task = Some(task);
    }

    /// Connects to a predeployed account using Bevy tasks.
    pub fn connect_predeployed_account(&mut self, rpc_url: String, account_idx: usize) {
        info!("Connecting to Starknet (predeployed, v2).");
        let task_pool = IoTaskPool::get();
        let task = task_pool
            .spawn(async move { connect_predeployed_account_v2(rpc_url, account_idx).await });
        self.sn.connecting_task = Some(task);
    }

    /// Queues a transaction using Bevy tasks.
    pub fn queue_tx(&mut self, calls: Vec<Call>) {
        if let Some(account) = self.sn.account.clone() {
            let task_pool = IoTaskPool::get();
            let task = task_pool.spawn(async move {
                let tx = account.execute_v3(calls);
                tx.send().await
            });
            self.sn.pending_txs.push_back(task);
        } else {
            warn!("No Starknet account initialized, skipping transaction.");
        }
    }

    /// Queues an entity retrieval query using Bevy tasks.
    pub fn queue_retrieve_entities(&mut self, query: ToriiQuery) {
        if let Some(client) = self.torii.client.clone() {
            let task_pool = IoTaskPool::get();
            let task = task_pool.spawn(async move {
                let mut client = client.lock().await;
                client.retrieve_entities(query).await
            });
            self.torii.pending_retrieve_entities.push_back(task);
        } else {
            warn!("No Torii client initialized, skipping query.");
        }
    }

    /// Subscribes to entity updates using Bevy tasks.
    pub fn subscribe_entities(&mut self, id: String, clause: Option<Clause>) {
        if let Some(client) = self.torii.client.clone() {
            let sender = self.torii.subscription_sender.clone();
            let task_pool = IoTaskPool::get();
            let task = task_pool.spawn(async move {
                let subscription_result = {
                    let mut client = client.lock().await;
                    client.subscribe_entities(clause).await
                };

                match subscription_result {
                    Ok(mut subscription) => {
                        while let Some(Ok((n, e))) = subscription.next().await {
                            debug!("Torii subscribe entities update: {} {:?}", n, e);
                            if let Some(ref sender) = sender {
                                let _ = sender.send((e.hashed_keys, e.models));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to subscribe to entities: {:?}", e);
                    }
                }
            });

            // Store the subscription task with proper cleanup of old subscriptions
            let subscriptions = self.torii.subscriptions.clone();
            let task_id = id.clone();
            let store_task: Task<Result<(), String>> = IoTaskPool::get().spawn(async move {
                let mut subs = subscriptions.lock().await;

                // Clean up old subscription if it exists
                if let Some(_old_state) = subs.remove(&task_id) {
                    // Mark old task as inactive (it will naturally terminate)
                    debug!("Replacing existing subscription: {}", task_id);
                }

                subs.insert(
                    task_id,
                    SubscriptionTaskState {
                        task,
                        is_active: true,
                    },
                );

                Ok(())
            });

            // Store the subscription storage task to track completion
            self.torii.pending_subscription_stores.push_back(store_task);
        } else {
            warn!("No Torii client initialized, skipping subscription.");
        }
    }
}

/// System to check Torii tasks and handle responses.
fn check_torii_task_v2(
    mut dojo: ResMut<DojoResourceV2>,
    mut ev_retrieve_entities: EventWriter<DojoEntityUpdatedV2>,
    mut ev_initialized: EventWriter<DojoInitializedEventV2>,
) {
    // Check if Torii client initialization is complete
    if let Some(mut task) = dojo.torii.init_task.take() {
        if let Some(result) = bevy::tasks::block_on(bevy::tasks::poll_once(&mut task)) {
            match result {
                Ok(client) => {
                    info!("Torii client initialized (v2).");
                    dojo.torii.client = Some(Arc::new(Mutex::new(client)));
                    ev_initialized.write(DojoInitializedEventV2);
                }
                Err(e) => {
                    error!("Failed to initialize Torii client: {:?}", e);
                    // Put the task back if it failed
                    dojo.torii.init_task = Some(task);
                }
            }
        } else {
            // Task not ready yet, put it back
            dojo.torii.init_task = Some(task);
        }
    }

    // Check pending subscription storage tasks
    let mut completed_stores = Vec::new();
    for (index, task) in dojo
        .torii
        .pending_subscription_stores
        .iter_mut()
        .enumerate()
    {
        if let Some(result) = bevy::tasks::block_on(bevy::tasks::poll_once(task)) {
            completed_stores.push((index, result));
        }
    }

    // Process completed subscription storage tasks
    for (index, result) in completed_stores.into_iter().rev() {
        dojo.torii.pending_subscription_stores.remove(index);
        match result {
            Ok(_) => {
                debug!("Subscription successfully stored");
            }
            Err(e) => {
                error!("Failed to store subscription: {}", e);
            }
        }
    }

    // Check pending entity retrieval tasks
    let mut completed_tasks = Vec::new();
    for (index, task) in dojo.torii.pending_retrieve_entities.iter_mut().enumerate() {
        if let Some(result) = bevy::tasks::block_on(bevy::tasks::poll_once(task)) {
            completed_tasks.push((index, result));
        }
    }

    // Process completed tasks in reverse order to maintain indices
    for (index, result) in completed_tasks.into_iter().rev() {
        dojo.torii.pending_retrieve_entities.remove(index);

        match result {
            Ok(response) => {
                debug!("Retrieve entities response: {:?}", response);
                for e in response.entities {
                    ev_retrieve_entities.write(DojoEntityUpdatedV2 {
                        entity_id: Felt::from_bytes_be_slice(&e.hashed_keys),
                        models: e
                            .models
                            .into_iter()
                            .map(|m| m.try_into().unwrap())
                            .collect(),
                    });
                }
            }
            Err(e) => {
                error!("Failed to retrieve entities: {:?}", e);
            }
        }
    }

    // Check for subscription updates
    if let Some(receiver) = &dojo.torii.subscription_receiver {
        while let Ok((entity_id, models)) = receiver.try_recv() {
            debug!("Torii subscription update: {:?}", (entity_id, &models));
            ev_retrieve_entities.write(DojoEntityUpdatedV2 { entity_id, models });
        }
    }
}

/// System to check Starknet tasks and handle responses.
fn check_sn_task_v2(mut dojo: ResMut<DojoResourceV2>) {
    // Check if Starknet account connection is complete
    if let Some(mut task) = dojo.sn.connecting_task.take() {
        if let Some(result) = bevy::tasks::block_on(bevy::tasks::poll_once(&mut task)) {
            info!("Connected to Starknet (v2).");
            dojo.sn.account = Some(result);
        } else {
            // Task not ready yet, put it back
            dojo.sn.connecting_task = Some(task);
        }
    }

    // Check pending transactions - only if we have an account and pending transactions
    if !dojo.sn.pending_txs.is_empty() {
        if dojo.sn.account.is_some() {
            let mut completed_tasks = Vec::new();
            for (index, task) in dojo.sn.pending_txs.iter_mut().enumerate() {
                if let Some(result) = bevy::tasks::block_on(bevy::tasks::poll_once(task)) {
                    completed_tasks.push((index, result));
                }
            }

            // Process completed tasks in reverse order to maintain indices
            for (index, result) in completed_tasks.into_iter().rev() {
                dojo.sn.pending_txs.remove(index);

                match result {
                    Ok(tx_result) => {
                        info!("Transaction completed: {:#x}", tx_result.transaction_hash);
                    }
                    Err(e) => {
                        error!("Transaction failed: {:?}", e);
                    }
                }
            }
        } else {
            // Clear pending transactions if no account is available
            warn!(
                "Clearing {} pending transactions - no account available",
                dojo.sn.pending_txs.len()
            );
            dojo.sn.pending_txs.clear();
        }
    }
}

/// Connects to a Starknet account (v2).
async fn connect_to_starknet_v2(
    rpc_url: String,
    account_addr: Felt,
    private_key: Felt,
) -> Arc<SingleOwnerAccount<AnyProvider, LocalWallet>> {
    let provider = AnyProvider::JsonRpcHttp(JsonRpcClient::new(HttpTransport::new(
        Url::parse(&rpc_url).expect("Expecting valid Starknet RPC URL"),
    )));

    let chain_id = provider.chain_id().await.unwrap();
    let signer = LocalWallet::from(SigningKey::from_secret_scalar(private_key));

    Arc::new(SingleOwnerAccount::new(
        provider,
        signer,
        account_addr,
        chain_id,
        ExecutionEncoding::New,
    ))
}

/// Connects to a predeployed account (v2).
pub async fn connect_predeployed_account_v2(
    rpc_url: String,
    account_idx: usize,
) -> Arc<SingleOwnerAccount<AnyProvider, LocalWallet>> {
    let provider = AnyProvider::JsonRpcHttp(JsonRpcClient::new(HttpTransport::new(
        Url::parse(&rpc_url).unwrap(),
    )));

    let client = reqwest::Client::new();
    let response = client
        .post(&rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "dev_predeployedAccounts",
            "params": [],
            "id": 1
        }))
        .send()
        .await
        .expect("Failed to fetch predeployed accounts.");

    let result: serde_json::Value = response
        .json()
        .await
        .expect("Failed to parse predeployed accounts.");

    if let Some(vals) = result.get("result").and_then(|v| v.as_array()) {
        let chain_id = provider.chain_id().await.expect("Failed to get chain id.");

        for (i, a) in vals.iter().enumerate() {
            let address = a["address"].as_str().unwrap();

            let private_key = if let Some(pk) = a["privateKey"].as_str() {
                pk
            } else {
                continue;
            };

            let provider = AnyProvider::JsonRpcHttp(JsonRpcClient::new(HttpTransport::new(
                Url::parse(&rpc_url).unwrap(),
            )));

            let signer = LocalWallet::from(SigningKey::from_secret_scalar(
                Felt::from_hex(private_key).unwrap(),
            ));

            let mut account = SingleOwnerAccount::new(
                provider,
                signer,
                Felt::from_hex(address).unwrap(),
                chain_id,
                ExecutionEncoding::New,
            );

            account.set_block_id(BlockId::Tag(BlockTag::Pending));

            if i == account_idx {
                return Arc::new(account);
            }
        }
    }

    panic!("Account index out of bounds.");
}
