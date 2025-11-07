import os
import json




VALID_ALLOCATOR_REBALANCE_COMBINATIONS = {
    "SIMPLE2Q": set(["marginal-hits-old", "marginal-hits-new", "free-mem", "disabled", "hits", "tail-age", "lama", 'eviction-rate']),
    "LRU2Q": set(["marginal-hits-old", "marginal-hits-new", "free-mem", "disabled", "hits", "tail-age", "lama", 'eviction-rate']),
    "TINYLFU": set(["free-mem", "disabled", "hits", "tail-age", 'eviction-rate', "lama"]),
    "TINYLFUTail": set(["marginal-hits-old", "marginal-hits-new"]),
}