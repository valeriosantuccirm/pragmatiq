# TODO: adjust once ready for release!

# PragmatiQ

**Asynchronous Task Queue & Event Management with Monitoring**

PragmatiQ is a Python framework for building distributed systems with Redis-backed task queues, event-driven architecture, and built-in monitoring using Prometheus and Jaeger. It supports both CPU-bound and I/O-bound workloads with priority scheduling and fault tolerance.

[![Python 3.12.0](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Redis Required](https://img.shields.io/badge/redis-%3E=5.0-red.svg)](https://redis.io/)
[![Architecture: Event-Driven](https://img.shields.io/badge/architecture-event--driven-brightgreen)](https://microservices.io/patterns/data/event-driven-architecture.html)

---

## 🚀 Features

- **Redis-backed Priority Queues**: Multiple priority levels with dead-letter queue support
- **Task Processing**: Async CPU-bound (Ray) and I/O-bound (asyncio) task execution
- **Event-Driven Architecture**: Publish/subscribe pattern with event-task binding
- **Observability**: Built-in Prometheus metrics and Jaeger distributed tracing
- **Priority Scheduling**: 10 priority levels (0 highest - 9 lowest)
- **Fault Tolerance**: Automatic retries and dead-letter queue handling
- **Singleton Management**: Thread-safe singleton instance for cross-application consistency
- **Flexible Configuration**: Customizable Redis, Prometheus, and tracing settings

---

## 📦 Installation

```sh
pip install pragmatiq # TODO: once released
```

---

## ⚡ Quick Start

### **Basic Task Processing**

```python
import asyncio
from pragmatiq import PragmatiQ

async def main():
    pragmatiq = PragmatiQ()
    
    @pragmatiq.collector("io")
    async def process_data(data: str) -> str:
        return f"Processed {data}"
    
    async with pragmatiq.lifespan():
        result = await process_data("sample")
        print(f"Task ID: {result['task_id']}")
        
asyncio.run(main())
```

### **Event-Driven Example**

```python
import asyncio
from pragmatiq import PragmatiQ, Event

class DataProcessedEvent(Event):
    pass

async def main():
    pragmatiq = PragmatiQ()
    
    @pragmatiq.event_handler(DataProcessedEvent)
    async def handle_processed_data(event: DataProcessedEvent):
        print(f"Processed data: {event.data}")
    
    @pragmatiq.collector("io", priority=2)
    async def background_processing(data: dict):
        await asyncio.sleep(0.5)
        return {"status": "success", **data}
    
    pragmatiq.bind_event_to_task("io", DataProcessedEvent, background_processing, priority=2)
    
    async with pragmatiq.lifespan():
        pragmatiq.dispatch_event(DataProcessedEvent({"id": 123, "payload": "test"}))
        await asyncio.sleep(1)  # Allow event processing
        
asyncio.run(main())
```

---

## ⚙️ Configuration

Customize settings using `PragmatiQConfig`:

```python
from pragmatiq import PragmatiQ, PragmatiQConfig

config = PragmatiQConfig(
    redis_host="redis-cluster",
    redis_port=6379,
    queue_name="myapp_tasks",
    process_pool_size=8,
    prometheus_port=9100,
    service_name="data-processor"
)

pragmatiq = PragmatiQ(config=config)
```

---

## 📊 Monitoring

### **Built-in Metrics**

Default Prometheus endpoint: `http://localhost:9090/metrics`

Key metrics:
- `tasks_enqueued_total`: Counter of all enqueued tasks
- `task_duration_seconds`: Histogram of task execution times
- `task_state_{completed,failed,running}`: Task state gauges
- `cpu_bound_tasks_total`: CPU-specific task counter

### **Distributed Tracing with Jaeger**

Jaeger tracing captures:
- Task enqueueing spans
- Task execution timing
- Event propagation tracking
- Redis operation tracing

Example usage:

```python
from pragmatiq import Monitor

async def check_system():
    async with Monitor() as client:
        queue_length = await client.query_prometheus('pragmatiq_queue_length')
        traces = await client.get_jaeger_traces(service="data-processor", tags={"error": "true"}, limit=20)
```

---

## 🔥 Advanced Usage

### **Priority Task Handling**

```python
@pragmatiq.collector("cpu", priority=0, timeout=120)
def process_video(video_path: str):
    return perform_video_processing(video_path)
```

### **Custom Event Payloads**

```python
import time
from pragmatiq import Event

class DatabaseUpdatedEvent(Event):
    def __init__(self, table: str, action: str):
        super().__init__({"table": table, "action": action})
        self.timestamp = time.time()

@pragmatiq.event_handler(DatabaseUpdatedEvent)
async def handle_db_update(event: DatabaseUpdatedEvent):
    logger.info(f"Update on {event.data['table']} at {event.timestamp}")
```

---

## 📌 Requirements

- Redis Server **5.0+**
- Python **3.12.0**
- Prometheus (**optional**)
- Jaeger (**optional**)

---

## 🤝 Contributing

Contributions are welcome! Please see our **Contribution Guidelines** for details.

---

## 📜 License

**MIT License** - See `LICENSE` for full details.
