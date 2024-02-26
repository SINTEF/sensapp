use std::sync::Arc;

use anyhow::Result;
use async_broadcast::{InactiveReceiver, Receiver, Sender};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct WaitForAll {
    nb_started: Arc<Mutex<usize>>,
    nb_finished: Arc<Mutex<usize>>,
    finished_sender: Sender<()>,
    finished_receiver: InactiveReceiver<()>,
}

impl WaitForAll {
    pub fn new() -> Self {
        let (s, r) = async_broadcast::broadcast(1);
        Self {
            nb_started: Arc::new(Mutex::new(0)),
            nb_finished: Arc::new(Mutex::new(0)),
            finished_sender: s,
            finished_receiver: r.deactivate(),
        }
    }

    pub async fn add(&mut self, mut receiver: Receiver<()>) {
        {
            let mut nb_started = self.nb_started.lock().await;
            *nb_started += 1;
        }
        let nb_started_clone = self.nb_started.clone();
        let nb_finished_clone = self.nb_finished.clone();
        let finished_sender_clone = self.finished_sender.clone();

        tokio::spawn(async move {
            let _ = receiver.recv().await;
            {
                let mut nb_finished = nb_finished_clone.lock().await;
                *nb_finished += 1;
            }
            if !finished_sender_clone.is_closed() && finished_sender_clone.receiver_count() > 0 {
                {
                    let nb_started = nb_started_clone.lock().await;
                    let nb_finished = nb_finished_clone.lock().await;
                    if *nb_started != *nb_finished {
                        return;
                    }
                }
                let _ = finished_sender_clone.broadcast(()).await;
            }
        });
    }

    pub async fn wait(&mut self) -> Result<()> {
        // If already finished, return immediately
        if *self.nb_started.lock().await == *self.nb_finished.lock().await {
            return Ok(());
        }

        let mut receiver = self.finished_receiver.activate_cloned();
        receiver.recv().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_for_all() {
        let mut wfa = WaitForAll::new();

        let (s1, r1) = async_broadcast::broadcast(1);
        let (s2, r2) = async_broadcast::broadcast(1);

        wfa.add(r1).await;
        wfa.add(r2).await;

        let s2_clone = s2.clone();

        tokio::spawn(async move {
            println!("Waiting for s1");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            println!("Broadcasting s1");
            s1.broadcast(()).await.unwrap();
        });

        tokio::spawn(async move {
            println!("Waiting for s2");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            println!("Broadcasting s2");
            s2.broadcast(()).await.unwrap();
        });

        println!("Waiting for all");
        wfa.wait().await.unwrap();
        println!("done");

        // Should return fast since it's already finished
        wfa.wait().await.unwrap();

        // What happens now ?
        assert!(s2_clone.broadcast(()).await.is_err());
    }

    #[tokio::test]
    async fn test_without_waiting() {
        let mut wfa = WaitForAll::new();

        let (s1, r1) = async_broadcast::broadcast(1);
        wfa.add(r1).await;
        s1.broadcast(()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
