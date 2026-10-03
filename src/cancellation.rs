use tokio::sync::oneshot;

pub(crate) struct CancellationGuard {
    sender: Option<oneshot::Sender<()>>,
}

impl CancellationGuard {
    pub(crate) fn new() -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();

        (
            Self {
                sender: Some(sender),
            },
            receiver,
        )
    }

    pub(crate) fn disarm(&mut self) {
        self.sender = None;
    }
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}
