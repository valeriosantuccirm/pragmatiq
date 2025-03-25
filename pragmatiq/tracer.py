from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

# Global TracerProvider and Tracer
trace_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider=trace_provider)
