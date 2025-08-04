use async_trait::async_trait;

#[async_trait(?Send)]
pub trait PeerProvider {
    async fn get_peers(&self) -> Vec<super::PeerHandle>;
}
