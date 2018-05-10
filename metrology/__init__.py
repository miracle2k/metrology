from metrology.registry import registry
from metrology.instruments import *

# Backwards-compatibility
Metrology = registry


def get(name):
    return registry.get(name)

def register_counter(name):
    return registry.counter(name)

def register_derive(name):
    return registry.derive(name)

def register_meter(name):
    return registry.meter(name)

def register_gauge(name, gauge=None):
    return registry.gauge(name, gauge)

def register_timer(name):
    return registry.timer(name)

def register_utilization_timer(name):
    return registry.utilization_timer(name)

def register_histogram(name, histogram=None):
    return registry.histogram(name, histogram)

def register_health_check(name, health_check):
    return registry.health_check(name, health_check)

def register_profiler(name):
    return registry.profiler(name)

def stop(cls):
    return registry.stop()
