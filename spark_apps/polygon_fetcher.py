import time
import json
import logging
from datetime import datetime, timedelta
from functools import wraps
from polygon import RESTClient
from kafka import KafkaProducer
from ignores.api_keys import POLYGON_API_KEY

