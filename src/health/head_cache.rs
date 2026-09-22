use crate::database::{
    accept::db_batch,
    error::DbError,
    types::{Batch, GenericBytes, RequestBus},
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use tokio_stream::{StreamExt, wrappers::WatchStream};

/// Check if we need to do a reorg or if a new block has finalized.
pub async fn manage_cache<K, V>(
    head_cache: &Arc<RwLock<BTreeMap<u64, BTreeSet<K>>>>,
    blocknum_rx: tokio::sync::watch::Receiver<u64>,
    finalized_rx: Arc<tokio::sync::watch::Receiver<u64>>,
    cache: RequestBus<K, V>,
) -> Result<(), DbError>
where
    K: GenericBytes,
    V: GenericBytes,
{
    let mut block_number = 0;
    let mut blocknum_stream = WatchStream::new(blocknum_rx);
    let mut finalized_stream = WatchStream::new(finalized_rx.as_ref().clone());

    loop {
        tokio::select! {
            Some(new_block) = blocknum_stream.next() => {
                if new_block <= block_number {
                    tracing::warn!("Reorg detected! Removing stale entries from the cache.");
                    handle_reorg(head_cache, block_number, new_block, cache.clone()).await?;
                }
                block_number = new_block;
            }
            Some(finalized) = finalized_stream.next() => {
                remove_stale(head_cache, finalized)?;
            }
            else => break,
        }
    }
    Ok(())
}

/// We use the head_cache to store keys of querries we made near the tip
/// If a reorg happens, we need to remove all queries in the reorg range
/// from the sled database.
async fn handle_reorg<K, V>(
    head_cache: &Arc<RwLock<BTreeMap<u64, BTreeSet<K>>>>,
    block_number: u64,
    new_block: u64,
    cache: RequestBus<K, V>,
) -> Result<(), DbError>
where
    K: GenericBytes,
    V: GenericBytes,
{
    let mut batch = Batch::with_capacity(0);
    {
        let mut entries = head_cache.write().unwrap();
        let blocks: Vec<_> = entries
            .range(new_block..=block_number)
            .map(|(&number, _)| number)
            .collect();
        for number in blocks {
            for key in entries.remove(&number).unwrap() {
                batch.delete(key);
            }
        }
    }

    // Send the batch to the cache
    drop(db_batch(&cache, batch).await);

    Ok(())
}

/// Removes stale entries from `head_cache`
///
/// Once a new block finalizes, we can be sure that certain TXs wont
/// reorg, so theyre safe to be permanantly in the cache.
fn remove_stale<K: GenericBytes>(
    head_cache: &Arc<RwLock<BTreeMap<u64, BTreeSet<K>>>>,
    block_number: u64,
) -> Result<(), DbError> {
    head_cache
        .write()
        .unwrap()
        .retain(|number, _| *number > block_number);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::types::DbRequest;
    use crate::database_processing;
    use crate::db_get;
    use sled::{Config, Db};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn finalization_prunes_tracking_without_head_notifications() {
        let head_cache = Arc::new(RwLock::new(BTreeMap::from([
            (4, BTreeSet::from([b"finalized".to_vec()])),
            (5, BTreeSet::from([b"pending".to_vec()])),
        ])));
        let (head_tx, head_rx) = tokio::sync::watch::channel(0);
        let (finalized_tx, finalized_rx) = tokio::sync::watch::channel(0);
        let (db_tx, mut db_rx) = mpsc::unbounded_channel::<DbRequest<Vec<u8>, Vec<u8>>>();
        let task_cache = head_cache.clone();
        let task = tokio::spawn(async move {
            manage_cache(&task_cache, head_rx, Arc::new(finalized_rx), db_tx)
                .await
                .unwrap();
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), db_rx.recv())
            .await
            .unwrap()
            .unwrap();
        drop(head_tx);
        finalized_tx.send(4).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while head_cache.read().unwrap().contains_key(&4) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("finalized tracking retained without a new head");
        assert!(head_cache.read().unwrap().contains_key(&5));
        drop(finalized_tx);
        task.await.unwrap();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_handle_reorg() {
        // Create test data and resources
        let head_cache = Arc::new(RwLock::new(BTreeMap::new()));
        let cache = Config::tmp().unwrap();
        let cache = Db::open_with_config(&cache).unwrap();

        let _ = cache.insert("key1", "value1");
        let _ = cache.insert("key2", "value2");
        let _ = cache.insert("key3", "value3");

        // Add some data to the head_cache
        {
            let mut head_cache_guard = head_cache.write().unwrap();
            head_cache_guard.insert(1, BTreeSet::from(["key1".as_bytes()]));
            head_cache_guard.insert(2, BTreeSet::from(["key2".as_bytes()]));
            head_cache_guard.insert(3, BTreeSet::from(["key3".as_bytes()]));
        }

        let (db_tx, db_rx) = mpsc::unbounded_channel::<DbRequest<&[u8], &[u8]>>();
        tokio::task::spawn(database_processing(db_rx, cache));

        // Call handle_reorg
        let result = handle_reorg(&head_cache, 3, 2, db_tx.clone()).await;

        // Verify the result and check if the data is removed from the cache
        assert!(result.is_ok(), "handle_reorg failed");
        {
            let head_cache_guard = head_cache.read().expect("failed to read head cache");
            assert!(
                head_cache_guard.contains_key(&1),
                "head cache does not contain key1"
            );
            assert!(
                !head_cache_guard.contains_key(&2),
                "head cache should not contain key2"
            );
            assert!(
                !head_cache_guard.contains_key(&3),
                "head cache should not contain key3"
            );
        }

        // Check if the data is removed from the cache
        let key1 = db_get!(db_tx.clone(), "key1".as_bytes()).unwrap();
        assert!(key1.is_some(), "failed to get key1 from db");
        let key2 = db_get!(db_tx.clone(), "key2".as_bytes()).unwrap();
        assert!(
            key2.is_none(),
            "successfully got key2 from db which should have failed"
        );
        let key3 = db_get!(db_tx.clone(), "key3".as_bytes()).unwrap();
        assert!(
            key3.is_none(),
            "successfully got key3 from db which should have failed"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn same_height_reorg_removes_all_responses_after_parent_finalizes() {
        for finalize_parent in [false, true] {
            let head_cache = Arc::new(RwLock::new(BTreeMap::from([
                (1, BTreeSet::from([b"parent".as_slice()])),
                (2, BTreeSet::from([b"head_a".as_slice(), b"head_b"])),
            ])));
            let config = Config::tmp().unwrap();
            let cache = Db::open_with_config(&config).unwrap();
            for key in [b"parent".as_slice(), b"head_a", b"head_b"] {
                cache.insert(key, b"cached response").unwrap();
            }
            let (db_tx, db_rx) = mpsc::unbounded_channel::<DbRequest<&[u8], &[u8]>>();
            let worker = tokio::spawn(database_processing(db_rx, cache));

            if finalize_parent {
                remove_stale(&head_cache, 1).unwrap();
            }
            handle_reorg(&head_cache, 2, 2, db_tx.clone())
                .await
                .unwrap();

            assert!(
                db_get!(db_tx.clone(), b"parent".as_slice())
                    .unwrap()
                    .is_some()
            );
            for key in [b"head_a".as_slice(), b"head_b"] {
                assert!(db_get!(db_tx.clone(), key).unwrap().is_none());
            }
            assert!(!head_cache.read().unwrap().contains_key(&2));
            assert_eq!(
                head_cache.read().unwrap().contains_key(&1),
                !finalize_parent
            );
            drop(db_tx);
            worker.await.unwrap();
        }
    }

    #[test]
    fn test_remove_stale() {
        // Create test data and resources
        let head_cache = Arc::new(RwLock::new(BTreeMap::new()));

        // Add some data to the head_cache
        {
            let mut head_cache_guard = head_cache.write().unwrap();
            head_cache_guard.insert(1, BTreeSet::from(["key1".as_bytes()]));
            head_cache_guard.insert(2, BTreeSet::from(["key2".as_bytes()]));
        }

        // Call remove_stale
        let result = remove_stale(&head_cache, 1);

        // Verify the result and check if the data is removed from the cache
        assert!(result.is_ok());
        let head_cache_guard = head_cache.read().unwrap();
        assert!(!head_cache_guard.contains_key(&1));
        assert!(head_cache_guard.contains_key(&2));
    }

    #[test]
    fn finalizing_maximum_block_number_prunes_all_tracking() {
        let head_cache = Arc::new(RwLock::new(BTreeMap::from([
            (u64::MAX - 1, BTreeSet::from([b"parent".as_slice()])),
            (u64::MAX, BTreeSet::from([b"head".as_slice()])),
        ])));

        remove_stale(&head_cache, u64::MAX).unwrap();

        assert!(head_cache.read().unwrap().is_empty());
    }
}
