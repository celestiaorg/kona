//! Data source

use alloc::boxed::Box;
use alloy_primitives::Bytes;
use async_trait::async_trait;
use kona_providers::ChainProvider;

use crate::{
    errors::PipelineResult,
    sources::{BlobSource, CalldataSource},
    traits::{AsyncIterator, BlobProvider, CelestiaProvider},
};

/// An enum over the various data sources.
#[derive(Debug, Clone)]
pub enum EthereumDataSourceVariant<CP, B, CE>
where
    CP: ChainProvider + Send + Sync,
    B: BlobProvider + Send + Sync,
    CE: CelestiaProvider + Send + Sync,
{
    /// A calldata source.
    Calldata(CalldataSource<CP, CE>),
    /// A blob source.
    Blob(BlobSource<CP, B, CE>),
}

#[async_trait]
impl<CP, B, CE> AsyncIterator for EthereumDataSourceVariant<CP, B, CE>
where
    CP: ChainProvider + Send + Sync,
    B: BlobProvider + Send + Sync,
    CE: CelestiaProvider + Send + Sync,
{
    type Item = Bytes;

    async fn next(&mut self) -> PipelineResult<Self::Item> {
        match self {
            Self::Calldata(c) => c.next().await,
            Self::Blob(b) => b.next().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use kona_primitives::BlobData;
    use kona_providers::test_utils::TestChainProvider;
    use op_alloy_protocol::BlockInfo;

    use crate::{sources::EthereumDataSourceVariant, traits::test_utils::TestBlobProvider};

    #[tokio::test]
    async fn test_variant_next_calldata() {
        let chain = TestChainProvider::default();
        let block_ref = BlockInfo::default();
        let mut source =
            CalldataSource::new(chain, Default::default(), block_ref, Default::default());
        source.open = true;
        source.calldata.push_back(Default::default());
        let mut variant: EthereumDataSourceVariant<TestChainProvider, TestBlobProvider> =
            EthereumDataSourceVariant::Calldata(source);
        assert!(variant.next().await.is_ok());
    }

    #[tokio::test]
    async fn test_variant_next_blob() {
        let chain = TestChainProvider::default();
        let blob = TestBlobProvider::default();
        let block_ref = BlockInfo::default();
        let mut source =
            BlobSource::new(chain, blob, Default::default(), block_ref, Default::default());
        source.open = true;
        source.data.push(BlobData { calldata: Some(Default::default()), ..Default::default() });
        let mut variant: EthereumDataSourceVariant<TestChainProvider, TestBlobProvider> =
            EthereumDataSourceVariant::Blob(source);
        assert!(variant.next().await.is_ok());
    }
}
