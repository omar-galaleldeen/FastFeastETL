import subprocess
import sys
import logging
from utils.logger import get_logger
from ingestion import ingestion_runner

# Initialize root logger
logger = get_logger(__name__)

def main():
    logger.info("Starting FastFeast ETL Pipeline...")
    print("=====================================================")
    print("🚀 FastFeast Pipeline is running! Waiting for files...")
    print("Press Ctrl+C to stop the pipeline gracefully.")
    print("=====================================================\n")

    try:
        # This will block the main thread and run the continuous ingestion loop
        ingestion_runner.start()
        
    except KeyboardInterrupt:
        # Graceful shutdown when you press Ctrl+C in the terminal
        print("\n🛑 Stop signal received (Ctrl+C).")
        logger.info("KeyboardInterrupt received. Initiating graceful shutdown...")
        
    except Exception as e:
        # Catch any catastrophic failure that escaped the runner
        print(f"\n❌ Critical Error: {e}")
        logger.critical(f"Critical pipeline failure: {e}")
        
    finally:
        # Ensure watcher threads and tracker DB are closed cleanly
        print("Shutting down background threads... please wait.")
        ingestion_runner.stop()
        logger.info("Pipeline stopped successfully.")
        print("👋 Pipeline stopped. Goodbye!")

    # 1. Start the SLA Updater as a background detached process
    try:
        subprocess.Popen([sys.executable, "utils/sla_updater_job.py"])
    except Exception as e:
        print(f"⚠️ Warning: Could not start SLA Updater: {e}")

        
if __name__ == "__main__":
    main()
