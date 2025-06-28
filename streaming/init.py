__version__ = "1.0.0"
__author__ = "BigData Team"

from .kafka_producer import RealTimeEventProducer
from .kafka_consumer import RealTimeEventConsumer
from .demo_streaming import StreamingDemo

__all__ = [
    'RealTimeEventProducer',
    'RealTimeEventConsumer', 
    'StreamingDemo'
]