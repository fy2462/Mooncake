use crate::ha::types::{HaError, LeaderRole, LeadershipLossEvent, LeadershipSession};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, watch};

#[derive(Default)]
pub(super) struct LeadershipSessionState {
    pub(super) active_owner_token: Option<String>,
    pub(super) loss_suppressed_owner_token: Option<String>,
}

pub(super) type SharedLeadershipSessionState = Arc<Mutex<LeadershipSessionState>>;

pub(super) fn resolve_cluster_namespace(cluster_namespace: &str) -> String {
    if cluster_namespace.trim().is_empty() {
        std::env::var("MC_STORE_CLUSTER_ID")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "mooncake".to_string())
    } else {
        cluster_namespace.to_string()
    }
}

pub(super) fn validate_session(session: &LeadershipSession) -> Result<(), HaError> {
    if session.owner_token.trim().is_empty() || session.lease_ttl == Duration::ZERO {
        return Err(HaError::InvalidParams(
            "leadership session owner token and lease ttl must be set".into(),
        ));
    }
    Ok(())
}

pub(super) fn report_leadership_loss_if_active(
    session_state: &SharedLeadershipSessionState,
    session: &LeadershipSession,
    role_tx: &watch::Sender<LeaderRole>,
    loss_tx: &broadcast::Sender<LeadershipLossEvent>,
) -> bool {
    report_leadership_loss_if_active_with(session_state, session, role_tx, loss_tx, || {})
}

pub(super) fn report_leadership_loss_if_active_with<F>(
    session_state: &SharedLeadershipSessionState,
    session: &LeadershipSession,
    role_tx: &watch::Sender<LeaderRole>,
    loss_tx: &broadcast::Sender<LeadershipLossEvent>,
    on_confirmed_loss: F,
) -> bool
where
    F: FnOnce(),
{
    let mut state = session_state
        .lock()
        .expect("leadership session state mutex poisoned");
    if state.active_owner_token.as_deref() != Some(session.owner_token.as_str())
        || state.loss_suppressed_owner_token.as_deref() == Some(session.owner_token.as_str())
    {
        return false;
    }
    let _ = role_tx.send(LeaderRole::Standby);
    on_confirmed_loss();
    state.active_owner_token = None;
    let _ = loss_tx.send(LeadershipLossEvent {
        owner_token: session.owner_token.clone(),
        view_version: session.view.view_version,
    });
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::types::MasterView;

    fn session(owner_token: &str, view_version: u64) -> LeadershipSession {
        LeadershipSession {
            view: MasterView {
                leader_address: "127.0.0.1:50051".into(),
                view_version,
            },
            owner_token: owner_token.into(),
            lease_ttl: Duration::from_secs(3),
        }
    }

    #[test]
    fn active_keepalive_failure_demotes_and_reports_exact_session() {
        let state = SharedLeadershipSessionState::default();
        state.lock().unwrap().active_owner_token = Some("owner-1".into());
        let (role_tx, role_rx) = watch::channel(LeaderRole::Leader);
        let (loss_tx, mut loss_rx) = broadcast::channel(1);
        let session = session("owner-1", 7);

        assert!(report_leadership_loss_if_active(
            &state, &session, &role_tx, &loss_tx
        ));

        assert_eq!(*role_rx.borrow(), LeaderRole::Standby);
        assert_eq!(
            loss_rx.try_recv().unwrap(),
            LeadershipLossEvent {
                owner_token: "owner-1".into(),
                view_version: 7,
            }
        );
        assert!(state.lock().unwrap().active_owner_token.is_none());
    }

    #[test]
    fn explicitly_released_session_is_left_for_release_to_demote() {
        let state = SharedLeadershipSessionState::default();
        {
            let mut state = state.lock().unwrap();
            state.active_owner_token = Some("owner-1".into());
            state.loss_suppressed_owner_token = Some("owner-1".into());
        }
        let (role_tx, role_rx) = watch::channel(LeaderRole::Leader);
        let (loss_tx, mut loss_rx) = broadcast::channel(1);

        assert!(!report_leadership_loss_if_active(
            &state,
            &session("owner-1", 7),
            &role_tx,
            &loss_tx,
        ));

        assert_eq!(*role_rx.borrow(), LeaderRole::Leader);
        assert!(matches!(
            loss_rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        assert_eq!(
            state.lock().unwrap().active_owner_token.as_deref(),
            Some("owner-1")
        );
    }

    #[test]
    fn stale_keepalive_failure_cannot_demote_new_session() {
        let state = SharedLeadershipSessionState::default();
        state.lock().unwrap().active_owner_token = Some("owner-2".into());
        let (role_tx, role_rx) = watch::channel(LeaderRole::Leader);
        let (loss_tx, mut loss_rx) = broadcast::channel(1);

        assert!(!report_leadership_loss_if_active(
            &state,
            &session("owner-1", 7),
            &role_tx,
            &loss_tx,
        ));

        assert_eq!(*role_rx.borrow(), LeaderRole::Leader);
        assert!(matches!(
            loss_rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        assert_eq!(
            state.lock().unwrap().active_owner_token.as_deref(),
            Some("owner-2")
        );
    }

    #[tokio::test]
    async fn session_loss_receiver_filters_later_leadership_terms() {
        let (loss_tx, loss_rx) = broadcast::channel(4);
        let old_session = session("owner-1", 7);
        let mut receiver = crate::ha::types::LeadershipLossReceiver::new(loss_rx, &old_session);
        loss_tx
            .send(LeadershipLossEvent {
                owner_token: "owner-2".into(),
                view_version: 8,
            })
            .unwrap();
        loss_tx
            .send(LeadershipLossEvent {
                owner_token: "owner-1".into(),
                view_version: 7,
            })
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            LeadershipLossEvent {
                owner_token: "owner-1".into(),
                view_version: 7,
            }
        );
    }

    #[tokio::test]
    async fn session_loss_receiver_reports_lag_instead_of_waiting_forever() {
        let (loss_tx, loss_rx) = broadcast::channel(1);
        let old_session = session("owner-1", 7);
        let mut receiver = crate::ha::types::LeadershipLossReceiver::new(loss_rx, &old_session);
        loss_tx
            .send(LeadershipLossEvent {
                owner_token: "owner-1".into(),
                view_version: 7,
            })
            .unwrap();
        loss_tx
            .send(LeadershipLossEvent {
                owner_token: "owner-2".into(),
                view_version: 8,
            })
            .unwrap();

        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(1))
        ));
    }

    #[test]
    fn confirmed_loss_hook_runs_only_for_the_active_session() {
        let state = SharedLeadershipSessionState::default();
        state.lock().unwrap().active_owner_token = Some("owner-2".into());
        let (role_tx, _) = watch::channel(LeaderRole::Leader);
        let (loss_tx, _) = broadcast::channel(1);
        let calls = std::sync::atomic::AtomicUsize::new(0);

        assert!(!report_leadership_loss_if_active_with(
            &state,
            &session("owner-1", 7),
            &role_tx,
            &loss_tx,
            || {
                calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
        ));
        assert!(report_leadership_loss_if_active_with(
            &state,
            &session("owner-2", 8),
            &role_tx,
            &loss_tx,
            || {
                calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
