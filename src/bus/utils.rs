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
        let nb_finished_clone = self.nb_finished.clone();
        let finished_sender_clone = self.finished_sender.clone();

        tokio::spawn(async move {
            let _ = receiver.recv().await;
            {
                let mut nb_finished = nb_finished_clone.lock().await;
                *nb_finished += 1;
            }
            if !finished_sender_clone.is_closed() && finished_sender_clone.receiver_count() > 0 {
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
