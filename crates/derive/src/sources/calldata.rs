//! CallData Source

use crate::{
    errors::{PipelineError, PipelineResult},
    traits::{AsyncIterator, CelestiaProvider},
};
use alloc::{boxed::Box, collections::VecDeque, format, string::String, sync::Arc, vec};
use alloy_consensus::{Transaction, TxEnvelope};
use alloy_primitives::{Address, Bytes, TxKind};
use async_trait::async_trait;
use celestia_types::{nmt::Namespace, Blob, Commitment};
use kona_providers::ChainProvider;
use op_alloy_protocol::BlockInfo;
use tracing::info;

/// A data iterator that reads from calldata.
#[derive(Debug, Clone)]
pub struct CalldataSource<CP, CE>
where
    CP: ChainProvider + Send,
    CE: CelestiaProvider + Send,
{
    /// The chain provider to use for the calldata source.
    pub chain_provider: CP,
    /// The batch inbox address.
    pub batch_inbox_address: Address,
    /// Block Ref
    pub block_ref: BlockInfo,
    /// The L1 Signer.
    pub signer: Address,
    /// Current calldata.
    pub calldata: VecDeque<Bytes>,
    /// Whether the calldata source is open.
    pub open: bool,
    /// Celestia client used to perform celestia-node rpc requests
    pub celestia: CE,
    /// Namespace used to write and read data from
    pub namespace: Namespace,
}

impl<CP: ChainProvider + Send, CE: CelestiaProvider + Send> CalldataSource<CP, CE> {
    /// Creates a new calldata source.
    pub const fn new(
        chain_provider: CP,
        batch_inbox_address: Address,
        block_ref: BlockInfo,
        signer: Address,
        celestia: CE,
        namespace: Namespace,
    ) -> Self {
        Self {
            chain_provider,
            batch_inbox_address,
            block_ref,
            signer,
            calldata: VecDeque::new(),
            open: false,
            celestia,
            namespace,
        }
    }

    /// Loads the calldata into the source if it is not open.
    async fn load_calldata(&mut self) -> Result<(), CP::Error> {
        if self.open {
            return Ok(());
        }

        let (_, txs) =
            self.chain_provider.block_info_and_transactions_by_hash(self.block_ref.hash).await?;

        let mut results = VecDeque::new();
        for tx in txs.iter() {
            let (tx_kind, data) = match tx {
                TxEnvelope::Legacy(tx) => (tx.tx().to(), tx.tx().input()),
                TxEnvelope::Eip2930(tx) => (tx.tx().to(), tx.tx().input()),
                TxEnvelope::Eip1559(tx) => (tx.tx().to(), tx.tx().input()),
                _ => continue,
            };
            let TxKind::Call(to) = tx_kind else { continue };

            if to != self.batch_inbox_address {
                continue;
            }
            if tx.recover_signer().ok().is_none() || tx.recover_signer().unwrap() != self.signer {
                continue;
            }

            let result = match data[0] {
                0xce => {
                    let height_bytes = &data[1..9];
                    let height = u64::from_le_bytes(height_bytes.try_into().unwrap());
                    let commitment = Commitment(data[9..42].try_into().unwrap());

                    match self.celestia.blob_get(height, self.namespace, commitment).await {
                        Ok(blob) => {
                            info!("Got blob from Celestia: {:?}", blob);
                            blob
                        }
                        Err(_) => continue,
                    }
                }
                _ => Bytes::from(data.to_vec()),
            };
            info!("Pusing result for calldata batch");
            results.push_back(result);
        }

        info!("Assigning results to calldata");
        self.calldata = results;
        self.open = true;

        Ok(())
    }
}

#[async_trait]
impl<CP: ChainProvider + Send, CE: CelestiaProvider + Send> AsyncIterator
    for CalldataSource<CP, CE>
{
    type Item = Bytes;

    async fn next(&mut self) -> PipelineResult<Self::Item> {
        if self.load_calldata().await.is_err() {
            return Err(PipelineError::Provider(format!(
                "Failed to load calldata for block {}",
                self.block_ref.hash
            ))
            .temp());
        }
        self.calldata.pop_front().ok_or(PipelineError::Eof.temp())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::PipelineErrorKind;
    use alloy_consensus::{Signed, TxEip2930, TxEip4844, TxEip4844Variant, TxLegacy};
    use alloy_primitives::{address, Address, Signature};
    use kona_providers::test_utils::TestChainProvider;

    pub(crate) fn test_legacy_tx(to: Address) -> TxEnvelope {
        let sig = Signature::test_signature();
        TxEnvelope::Legacy(Signed::new_unchecked(
            TxLegacy { to: TxKind::Call(to), ..Default::default() },
            sig,
            Default::default(),
        ))
    }

    pub(crate) fn test_eip2930_tx(to: Address) -> TxEnvelope {
        let sig = Signature::test_signature();
        TxEnvelope::Eip2930(Signed::new_unchecked(
            TxEip2930 { to: TxKind::Call(to), ..Default::default() },
            sig,
            Default::default(),
        ))
    }

    pub(crate) fn test_blob_tx(to: Address) -> TxEnvelope {
        let sig = Signature::test_signature();
        TxEnvelope::Eip4844(Signed::new_unchecked(
            TxEip4844Variant::TxEip4844(TxEip4844 { to, ..Default::default() }),
            sig,
            Default::default(),
        ))
    }

    pub(crate) fn default_test_calldata_source() -> CalldataSource<TestChainProvider> {
        CalldataSource::new(
            TestChainProvider::default(),
            Default::default(),
            BlockInfo::default(),
            Default::default(),
        )
    }

    #[tokio::test]
    async fn test_load_calldata_open() {
        let mut source = default_test_calldata_source();
        source.open = true;
        assert!(source.load_calldata().await.is_ok());
    }

    #[tokio::test]
    async fn test_load_calldata_provider_err() {
        let mut source = default_test_calldata_source();
        assert!(source.load_calldata().await.is_err());
    }

    #[tokio::test]
    async fn test_load_calldata_chain_provider_empty_txs() {
        let mut source = default_test_calldata_source();
        let block_info = BlockInfo::default();
        source.chain_provider.insert_block_with_transactions(0, block_info, Vec::new());
        assert!(!source.open); // Source is not open by default.
        assert!(source.load_calldata().await.is_ok());
        assert!(source.calldata.is_empty());
        assert!(source.open);
    }

    #[tokio::test]
    async fn test_load_calldata_wrong_batch_inbox_address() {
        let batch_inbox_address = address!("0123456789012345678901234567890123456789");
        let mut source = default_test_calldata_source();
        let block_info = BlockInfo::default();
        let tx = test_legacy_tx(batch_inbox_address);
        source.chain_provider.insert_block_with_transactions(0, block_info, vec![tx]);
        assert!(!source.open); // Source is not open by default.
        assert!(source.load_calldata().await.is_ok());
        assert!(source.calldata.is_empty());
        assert!(source.open);
    }

    #[tokio::test]
    async fn test_load_calldata_wrong_signer() {
        let batch_inbox_address = address!("0123456789012345678901234567890123456789");
        let mut source = default_test_calldata_source();
        source.batch_inbox_address = batch_inbox_address;
        let block_info = BlockInfo::default();
        let tx = test_legacy_tx(batch_inbox_address);
        source.chain_provider.insert_block_with_transactions(0, block_info, vec![tx]);
        assert!(!source.open); // Source is not open by default.
        assert!(source.load_calldata().await.is_ok());
        assert!(source.calldata.is_empty());
        assert!(source.open);
    }

    #[tokio::test]
    async fn test_load_calldata_valid_legacy_tx() {
        let batch_inbox_address = address!("0123456789012345678901234567890123456789");
        let mut source = default_test_calldata_source();
        source.batch_inbox_address = batch_inbox_address;
        let tx = test_legacy_tx(batch_inbox_address);
        source.signer = tx.recover_signer().unwrap();
        let block_info = BlockInfo::default();
        source.chain_provider.insert_block_with_transactions(0, block_info, vec![tx]);
        assert!(!source.open); // Source is not open by default.
        assert!(source.load_calldata().await.is_ok());
        assert!(!source.calldata.is_empty()); // Calldata is NOT empty.
        assert!(source.open);
    }

    #[tokio::test]
    async fn test_load_calldata_valid_eip2930_tx() {
        let batch_inbox_address = address!("0123456789012345678901234567890123456789");
        let mut source = default_test_calldata_source();
        source.batch_inbox_address = batch_inbox_address;
        let tx = test_eip2930_tx(batch_inbox_address);
        source.signer = tx.recover_signer().unwrap();
        let block_info = BlockInfo::default();
        source.chain_provider.insert_block_with_transactions(0, block_info, vec![tx]);
        assert!(!source.open); // Source is not open by default.
        assert!(source.load_calldata().await.is_ok());
        assert!(!source.calldata.is_empty()); // Calldata is NOT empty.
        assert!(source.open);
    }

    #[tokio::test]
    async fn test_load_calldata_blob_tx_ignored() {
        let batch_inbox_address = address!("0123456789012345678901234567890123456789");
        let mut source = default_test_calldata_source();
        source.batch_inbox_address = batch_inbox_address;
        let tx = test_blob_tx(batch_inbox_address);
        source.signer = tx.recover_signer().unwrap();
        let block_info = BlockInfo::default();
        source.chain_provider.insert_block_with_transactions(0, block_info, vec![tx]);
        assert!(!source.open); // Source is not open by default.
        assert!(source.load_calldata().await.is_ok());
        assert!(source.calldata.is_empty());
        assert!(source.open);
    }

    #[tokio::test]
    async fn test_next_err_loading_calldata() {
        let mut source = default_test_calldata_source();
        assert!(matches!(source.next().await, Err(PipelineErrorKind::Temporary(_))));
    }
}
