from fastapi import FastAPI, Request
from api.routes import router
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import logging
import json
import time
from api import models  # Ensure models are imported
from api.database import engine
from api.telemetry import (
    setup_telemetry, get_tracer,
    # Import metrics with descriptive names (best practice)
    http_requests_total,
    http_request_duration_seconds, 
    http_errors_total,
    application_errors_total,
    active_connections,
    custom_registry,
    # Import helper functions for consistent labeling
    normalize_route,
    get_error_class,
)
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

# Initialize Logging First - Use JSON formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        if isinstance(record.msg, dict):
            return json.dumps(record.msg)
        return json.dumps({"message": record.getMessage(), "level": record.levelname})

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# Remove existing handlers
for handler in root_logger.handlers:
    root_logger.removeHandler(handler)
# Add JSON console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(JsonFormatter())
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# Create structured logger
class StructuredLogger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
    
    def info(self, msg, **kwargs):
        # Add trace context
        span = trace.get_current_span()
        span_context = span.get_span_context()
        
        # Format as JSON directly
        log_data = {
            "message": msg,
            "level": "INFO",
            "trace_id": format(span_context.trace_id, "032x") if span_context.is_valid else None,
            "span_id": format(span_context.span_id, "016x") if span_context.is_valid else None,
            **kwargs
        }
        # Send as dict rather than string
        self.logger.info(log_data)
    
    def error(self, msg, **kwargs):
        # Add trace context
        span = trace.get_current_span()
        span_context = span.get_span_context()
        
        # Format as JSON directly
        log_data = {
            "message": msg,
            "level": "ERROR",
            "trace_id": format(span_context.trace_id, "032x") if span_context.is_valid else None,
            "span_id": format(span_context.span_id, "016x") if span_context.is_valid else None,
            **kwargs
        }
        # Send as dict rather than string
        self.logger.error(log_data)

# Replace logger with structured logger
structured_logger = StructuredLogger(__name__)

# Create FastAPI app
app = FastAPI()

# Auto-create tables on startup
structured_logger.info("database_init", status="starting", action="check_tables")
models.Base.metadata.create_all(bind=engine)
structured_logger.info("database_init", status="complete", action="tables_created")

# Register API routes
app.include_router(router)

# Initialize OpenTelemetry (will use OTEL_SERVICE_NAME environment variable)
setup_telemetry()

# Instrument FastAPI and SQLAlchemy AFTER routes are registered
# Disable FastAPI's automatic metrics to avoid conflicts
FastAPIInstrumentor.instrument_app(app, excluded_urls="/metrics")
SQLAlchemyInstrumentor().instrument(engine=engine)

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """
    Middleware to collect Prometheus metrics and OpenTelemetry traces.
    
    This middleware demonstrates best practices for observability:
    1. Normalize routes to avoid high cardinality
    2. Use consistent error classification
    3. Correlate metrics with traces
    4. Proper exception handling
    """
    start_time = time.time()
    method = request.method
    # Use normalized route to avoid high cardinality in metrics
    route = normalize_route(request.url.path)
    
    # Increment active connections gauge
    active_connections.inc()
    
    # Create explicit span for the request
    tracer = get_tracer(__name__)
    with tracer.start_as_current_span(
        f"{method} {route}",
        attributes={
            "http.method": method,
            "http.url": str(request.url),
            "http.route": route,
            "http.scheme": request.url.scheme,
            "http.host": request.url.hostname,
        },
    ) as span:
        
        try:
            response = await call_next(request)
            status_code = response.status_code
            
            # Add response attributes to span
            span.set_attribute("http.status_code", status_code)
            span.set_attribute("http.response.size", response.headers.get("content-length", 0))
            
            if status_code >= 400:
                span.set_status(trace.Status(trace.StatusCode.ERROR))
            
            # Calculate request duration
            duration = time.time() - start_time
            
            # Record metrics using best practices
            # 1. Traffic metric
            http_requests_total.labels(
                method=method,
                route=route,  # Normalized route, not full path
                status_code=status_code
            ).inc()
            
            # 2. Latency metric
            http_request_duration_seconds.labels(
                method=method,
                route=route
            ).observe(duration)
            
            # Get trace context for logging correlation
            span_context = span.get_span_context()
            trace_id = format(span_context.trace_id, "032x") if span_context.is_valid else None
            span_id = format(span_context.span_id, "016x") if span_context.is_valid else None
            
            # Structured logging with trace correlation
            structured_logger.info(
                "request_processed",
                method=method,
                route=route,
                status_code=status_code,
                duration_seconds=duration,
                duration_ms=round(duration * 1000, 2),
                trace_id=trace_id,
                span_id=span_id
            )
            
            # 3. Error metrics (if applicable)
            if status_code >= 400:
                error_class = get_error_class(status_code)
                http_errors_total.labels(
                    method=method,
                    route=route,
                    error_code=error_class  # Use error class, not specific status
                ).inc()
                
                structured_logger.error(
                    "http_error",
                    method=method,
                    route=route,
                    status_code=status_code,
                    error_class=error_class,
                    duration_ms=round(duration * 1000, 2),
                    trace_id=trace_id,
                    span_id=span_id
                )
            
            return response
            
        except Exception as e:
            # Set span to error state
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            span.record_exception(e)
            
            # Record application errors with proper classification
            application_errors_total.labels(
                error_type=type(e).__name__,
                component="middleware"
            ).inc()
            
            # Get trace context for logging
            span_context = span.get_span_context()
            trace_id = format(span_context.trace_id, "032x") if span_context.is_valid else None
            span_id = format(span_context.span_id, "016x") if span_context.is_valid else None
            
            structured_logger.error(
                "request_failed",
                method=method,
                route=route,
                error=str(e),
                error_type=type(e).__name__,
                trace_id=trace_id,
                span_id=span_id
            )
            
            # Re-raise the exception
            raise e
        
        finally:
            # Decrement active connections in finally block to ensure it always happens
            active_connections.dec()

@app.get("/")
async def root():
    return {"message": "KodeKloud Record Store API is running1111!"}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(custom_registry), media_type=CONTENT_TYPE_LATEST)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "1.0.0"}

@app.get("/trace-test")
async def trace_test():
    tracer = get_tracer(__name__)
    with tracer.start_as_current_span("test-span") as span:
        span.set_attribute("test.attribute", "test-value")
        span.set_attribute("custom.operation", "trace-test")
        
        # Get the current trace and span ID for logging
        span_context = span.get_span_context()
        trace_id = format(span_context.trace_id, "032x") if span_context.is_valid else None
        span_id = format(span_context.span_id, "016x") if span_context.is_valid else None
        
        # Log with explicit trace context
        structured_logger.info(
            "trace_test_executed", 
            span_name="test-span", 
            service="api",
            trace_id=trace_id,
            span_id=span_id,
            test_attribute="test-value"
        )
        
        # Create a child span
        with tracer.start_as_current_span("child-span") as child_span:
            child_span.set_attribute("relationship", "child")
            time.sleep(0.1)  # Add a small delay
            
            # Log from child span
            child_context = child_span.get_span_context()
            child_trace_id = format(child_context.trace_id, "032x") if child_context.is_valid else None
            child_span_id = format(child_context.span_id, "016x") if child_context.is_valid else None
            
            structured_logger.info(
                "child_span_executed",
                span_name="child-span",
                service="api",
                trace_id=child_trace_id,
                span_id=child_span_id,
                parent_span_id=span_id
            )
        
        return {
            "message": "Test spans created",
            "trace_id": trace_id,
            "span_id": span_id
        }

@app.get("/error-test")
async def error_test():
    tracer = get_tracer(__name__)
    with tracer.start_as_current_span("error-span") as span:
        span.set_attribute("error", True)
        span.set_attribute("custom.operation", "error-simulation")
        
        # Set span status to error
        span.set_status(trace.Status(trace.StatusCode.ERROR, "Simulated error for testing"))
        
        # Get the current trace and span ID for logging
        span_context = span.get_span_context()
        trace_id = format(span_context.trace_id, "032x") if span_context.is_valid else None
        span_id = format(span_context.span_id, "016x") if span_context.is_valid else None
        
        structured_logger.error(
            "error_test_executed", 
            span_name="error-span", 
            service="api", 
            error_type="SimulatedError",
            error_reason="Testing error logging and tracing",
            trace_id=trace_id,
            span_id=span_id
        )
        
        # Simulate an HTTP 500 error
        return Response(
            content=json.dumps({
                "error": "Simulated error",
                "trace_id": trace_id,
                "span_id": span_id
            }), 
            status_code=500, 
            media_type="application/json"
        )

@app.on_event("startup")
async def generate_test_logs():
    # Generate some logs with trace contexts
    tracer = get_tracer(__name__)
    
    # Generate log with trace context
    with tracer.start_as_current_span("startup-span") as span:
        span.set_attribute("test.attribute", "test-value")
        span.set_attribute("custom.operation", "startup-test")
        structured_logger.info("Application started", 
                             operation="app_startup",
                             trace_id=format(span.get_span_context().trace_id, "032x"),
                             span_id=format(span.get_span_context().span_id, "016x"))
    
    # Generate error log with trace context
    with tracer.start_as_current_span("error-test-span") as span:
        span.set_attribute("error", True)
        span.set_attribute("custom.operation", "error-simulation")
        structured_logger.error("Test error log", 
                             error_type="SimulatedError",
                             operation="error_test",
                             trace_id=format(span.get_span_context().trace_id, "032x"),
                             span_id=format(span.get_span_context().span_id, "016x"))

structured_logger.info("api_startup", status="complete", version="1.0.0")
