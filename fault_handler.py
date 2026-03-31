import os
import pandas as pd
from datetime import datetime
from utils.logger import get_logger
from utils.alerter import send_alert

logger = get_logger(__name__)

class fault_handler:
    def __init__(self):
        # Setup quarantine directories for bad data
        self.base_dir = "data/quarantine"
        os.makedirs(f"{self.base_dir}/batch", exist_ok=True)
        os.makedirs(f"{self.base_dir}/stream", exist_ok=True)
