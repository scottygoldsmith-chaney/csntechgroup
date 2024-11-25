from flask import Flask
import threading
from utilities import process_all_clients

# Initialize Flask app
app = Flask(__name__)

def run_main_logic():
    """Run the core logic for processing Planning Center API."""
    try:
        print("Starting Planning Center API data processing...")
        process_all_clients()
        print("Planning Center API data processing completed.")
    except Exception as e:
        print(f"Error in Planning Center API processing: {e}")

@app.route("/")
def trigger():
    """HTTP endpoint to start the main logic."""
    thread = threading.Thread(target=run_main_logic)
    thread.start()
    return "Planning Center API processing started!"

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
    
