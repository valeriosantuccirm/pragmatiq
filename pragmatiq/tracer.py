from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

# Set global tracer
trace_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider=trace_provider)
