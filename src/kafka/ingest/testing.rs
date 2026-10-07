use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use tokio::task::JoinSet;

use crate::config::IngestTuning;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::ClusterStore;
use crate::testing::FakeCluster;

use super::runner::{self, Upstream};
use super::{
    AclLane, ConfigLane, LaneSource, LogDirLane, OffsetLane, QuotaLane, SchemaIdLane,
    ScramUserLane, SubjectLane, TopologyLane, WatermarkLane, Wave,
};

pub const IDLE: Duration = Duration::from_secs(600);

pub struct Rig {
    pub cluster: FakeCluster,
    pub store: Arc<ClusterStore>,
    lanes: JoinSet<()>,
}

impl Rig {
    pub fn new(cluster: FakeCluster) -> Self {
        let store = Arc::new(ClusterStore::named(&cluster.identity().name));
        Self::over(cluster, store)
    }

    pub fn over(cluster: FakeCluster, store: Arc<ClusterStore>) -> Self {
        Self {
            cluster,
            store,
            lanes: JoinSet::new(),
        }
    }

    pub fn local() -> Self {
        Self::new(FakeCluster::local())
    }

    pub fn port(&self) -> Arc<dyn ClusterSession> {
        Arc::new(self.cluster.clone())
    }

    pub fn topology(&self) -> TopologyLane {
        TopologyLane::with_interval(self.port(), IDLE)
    }

    pub fn watermarks(&self) -> WatermarkLane {
        WatermarkLane::with_interval(self.port(), IDLE, &IngestTuning::default())
    }

    pub fn configs(&self) -> ConfigLane {
        ConfigLane::with_interval(self.port(), IDLE)
    }

    pub fn subjects(&self) -> SubjectLane {
        SubjectLane::with_interval(self.port(), IDLE)
    }

    pub fn schema_ids(&self) -> SchemaIdLane {
        SchemaIdLane::with_interval(self.port(), IDLE)
    }

    pub fn log_dirs(&self) -> LogDirLane {
        LogDirLane::with_interval(self.port(), IDLE)
    }

    pub fn acls(&self) -> AclLane {
        AclLane::with_interval(self.port(), IDLE)
    }

    pub fn quotas(&self) -> QuotaLane {
        QuotaLane::with_interval(self.port(), IDLE)
    }

    pub fn scram_users(&self) -> ScramUserLane {
        ScramUserLane::with_interval(self.port(), IDLE)
    }

    pub fn offsets(&self) -> OffsetLane {
        OffsetLane::new(self.port())
    }

    pub async fn poll<S: LaneSource>(&self, lane: &S) {
        let Some(upstream) = S::Upstream::feed(&self.store).table().now_or_never() else {
            panic!(
                "the {} lane polled before its upstream committed",
                lane.name()
            );
        };
        runner::poll(&self.store, lane, &upstream).await;
    }

    pub async fn poll_catalog(&self) {
        self.poll(&self.topology()).await;
        self.poll(&self.watermarks()).await;
    }

    pub async fn ingest(&self) {
        self.poll_catalog().await;
        self.poll(&self.configs()).await;
        self.poll(&self.log_dirs()).await;
        self.poll(&self.subjects()).await;
        if self.store.subjects.load().is_some() {
            self.poll(&self.schema_ids()).await;
        }
        self.poll(&self.acls()).await;
        self.poll(&self.quotas()).await;
        self.poll(&self.scram_users()).await;
        self.sweep(&self.offsets()).await;
    }

    pub async fn sweep(&self, lane: &OffsetLane) -> Wave {
        let topology = self
            .store
            .topology
            .load()
            .expect("the offset lane sweeps after a topology commit");
        lane.sweep(&self.store, &topology).await
    }

    pub fn spawn<S: LaneSource>(&mut self, lane: S) {
        self.lanes.spawn(runner::run(Arc::clone(&self.store), lane));
    }

    pub fn spawn_offsets(&mut self, lane: OffsetLane) {
        self.lanes.spawn(lane.run(Arc::clone(&self.store)));
    }
}
