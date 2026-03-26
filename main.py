from ingestion.ingestion_runner import start, stop

try:
    start()   
except KeyboardInterrupt:
    print("Stopping...")
    stop() 