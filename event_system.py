import threading

class EventSystem:
    def __init__(self, state: dict):
        self.state = state
        self.event = threading.Event()  # Used to pause/resume execution
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()  # Start the thread immediately

    def _run(self):
        """Background loop that waits for an event before continuing."""
        while self.running:
            self.event.wait()  # Pause until triggered
            if not self.running:
                break

            self.on_trigger()

            self.event.clear()  # Reset the event and wait again

    def trigger(self):
        """Trigger the background process to run."""
        self.event.set()

    def stop(self):
        """Stop the background process completely."""
        self.running = False
        self.event.set()  # Unblock the thread
        self.thread.join()  # Wait for the thread to exit

    def on_trigger(self):
        """Override this method in a subclass."""
        print("Executing event")
