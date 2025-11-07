#!/usr/bin/env python3

import numpy as np
import struct
import csv
from argparse import ArgumentParser
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

force_overwrite = False

class ZipfGenerator:
    def __init__(self, m, alpha, base_id=0):
        # Calculate Zeta values from 1 to n using NumPy:
        print(f"Generating Zipf distribution with m={m}, alpha={alpha}")
        tmp = np.power(np.arange(1, m + 1), -alpha)
        zeta = np.cumsum(tmp)

        # Store the translation map:
        self.distMap = zeta / zeta[-1]
        self.base_id = base_id

    def next(self):
        # Generate a uniform 0-1 pseudo-random value:
        u = np.random.uniform(0, 1)

        # Translate the Zipf variable:
        return (np.searchsorted(self.distMap, u) + self.base_id).item()


class NonConvexScanGenerator:
    def __init__(self, m, base_id):
        self.m = m
        self.base_id = base_id
        self.current_index = -1
    
    def next(self):
        self.current_index = (self.current_index + 1) % self.m
        return self.current_index + self.base_id

class NonConvexTraceGenerator:
    """
    Generates a stream of object IDs designed to produce a non-convex 
    miss ratio curve under LRU caching.

    The trace alternates between accessing two working sets (WS1, WS2) of 
    different sizes with potentially different repetition counts, mimicking 
    access patterns that cause plateaus and cliffs in LRU performance.

    Acts as an iterator, yielding the next object ID on each call to next().
    """

    def __init__(self, m, n, s1_ratio=0.6, s2_ratio=0.2, rep1=5, rep2=1):
        """
        Initializes the trace generator.

        Args:
            m (int): The total number of distinct objects available. 
                     The generated trace will use s1_size + s2_size distinct 
                     objects, where s1_size + s2_size <= m.
            n (int): The total number of requests (length of the trace).
            s1_ratio (float, optional): Ratio of 'm' to use for the size of 
                                       the primary working set (WS1). 
                                       Defaults to 0.6. The cliff in the 
                                       miss curve typically occurs around this size.
            s2_ratio (float, optional): Ratio of 'm' to use for the size of 
                                       the secondary working set (WS2). 
                                       Defaults to 0.2. Influences the start 
                                       of the first plateau.
            rep1 (int, optional): Number of times to iterate through WS1 
                                  sequentially before switching to WS2. 
                                  Defaults to 5. Controls WS1 access frequency.
            rep2 (int, optional): Number of times to iterate through WS2 
                                  sequentially before switching back to WS1. 
                                  Defaults to 1. Controls WS2 access frequency.

        Raises:
            ValueError: If parameters are invalid (e.g., m, n <= 0, ratios 
                        invalid, resulting working set sizes <= 0, or 
                        s1_size + s2_size > m, rep1/rep2 < 1).
        """
        if not isinstance(m, int) or m <= 0:
            raise ValueError("m (total distinct objects) must be a positive integer.")
        if not isinstance(n, int) or n < 0: # n=0 is technically valid (empty trace)
            raise ValueError("n (total requests) must be a non-negative integer.")
        if not (0 < s1_ratio <= 1):
             raise ValueError("s1_ratio must be between 0 (exclusive) and 1 (inclusive).")
        if not (0 < s2_ratio <= 1):
             raise ValueError("s2_ratio must be between 0 (exclusive) and 1 (inclusive).")
        if not isinstance(rep1, int) or rep1 < 1:
             raise ValueError("rep1 must be an integer >= 1.")
        if not isinstance(rep2, int) or rep2 < 1:
             raise ValueError("rep2 must be an integer >= 1.")

        self.m = m
        self.n = n
        self.rep1 = rep1
        self.rep2 = rep2

        # Calculate working set sizes based on ratios
        # Use floor to ensure integer sizes and guarantee <= m
        self.s1_size = math.floor(m * s1_ratio)
        self.s2_size = math.floor(m * s2_ratio)

        # Validation of calculated sizes
        if self.s1_size <= 0:
            raise ValueError(f"Resulting s1_size ({self.s1_size}) is not positive. Adjust m or s1_ratio.")
        if self.s2_size <= 0:
             raise ValueError(f"Resulting s2_size ({self.s2_size}) is not positive. Adjust m or s2_ratio.")
        if self.s1_size + self.s2_size > m:
            # This shouldn't happen with floor if ratios are <= 1, but check just in case
            raise ValueError(f"Calculated s1_size ({self.s1_size}) + s2_size ({self.s2_size}) exceeds m ({m}). Adjust ratios.")
        
        print(f"Generator Initialized: m={m}, n={n}")
        print(f"  WS1 Size (s1_size): {self.s1_size} (Objects 0 to {self.s1_size - 1})")
        print(f"  WS2 Size (s2_size): {self.s2_size} (Objects {self.s1_size} to {self.s1_size + self.s2_size - 1})")
        print(f"  Repetitions per cycle: REP1={self.rep1}, REP2={self.rep2}")
        print(f"  Total distinct objects used: {self.s1_size + self.s2_size}")

        # Internal state for iteration
        self._requests_generated = 0
        self._current_set = 1  # Start with WS1
        self._set_repetitions_left = self.rep1
        self._position_in_set = 0 # Next object index within the current set

    def __iter__(self):
        """Return the iterator object itself."""
        # Reset state if iteration is restarted
        self._requests_generated = 0
        self._current_set = 1 
        self._set_repetitions_left = self.rep1
        self._position_in_set = 0
        return self

    def __next__(self):
        """Returns the next object ID in the trace stream."""
        if self._requests_generated >= self.n:
            raise StopIteration

        # 1. Determine the object_id for the current state
        if self._current_set == 1:
            object_id = self._position_in_set
        else: # _current_set == 2
            object_id = self.s1_size + self._position_in_set

        # 2. Increment count and advance state for the *next* call
        self._requests_generated += 1
        self._position_in_set += 1

        current_set_size = self.s1_size if self._current_set == 1 else self.s2_size

        if self._position_in_set >= current_set_size:
            # Finished one full pass/repetition of the current set
            self._position_in_set = 0 # Reset position for next pass (if any)
            self._set_repetitions_left -= 1
            
            if self._set_repetitions_left <= 0:
                # Finished all repetitions for this set, switch to the other set
                if self._current_set == 1:
                    self._current_set = 2
                    self._set_repetitions_left = self.rep2
                    # Check if WS2 has size > 0 before proceeding
                    if self.s2_size <= 0:
                        # Should have been caught by init, but safety check
                        # If s2 is empty, switch back immediately (or stop?)
                        # Let's assume sizes > 0 based on init checks. 
                        pass 
                else: # _current_set == 2
                    self._current_set = 1
                    self._set_repetitions_left = self.rep1
                    # Check if WS1 has size > 0
                    if self.s1_size <= 0:
                         pass # Assume sizes > 0

        return object_id

    # Optional: Alias next() for convenience if desired, though __next__ is standard
    def next(self):
        return self.__next__()

class UniformGenerator:
    def __init__(self, m, base_id=0):
        self.m = m
        self.base_id = base_id
        print(f"Generating uniform distribution with m={m}")
    
    def next(self):
        return np.random.randint(0, self.m) + self.base_id


def gen_uniform(m: int, n: int, start: int = 0) -> np.ndarray:
    """generate uniform distributed workload

    Args:
        m (int): the number of objects
        n (int): the number of requests
        start (int, optional): start obj_id. Defaults to 0.

    Returns:
        requests that are uniform distributed
    """

    return np.random.uniform(0, m, n).astype(int) + start

class MergedStaticGenerator:
    def __init__(self, generators_config, base_id=0):
        self.generators = []
        for config in generators_config:
            print(config)
            if 'type' in config and config['type'] == 'uniform':
                generator = UniformGenerator(config['m'], base_id)
            elif 'type' in config and config['type'] == 'non_convex':
                generator = NonConvexScanGenerator(config['m'], base_id)
            else:
                generator = ZipfGenerator(config['m'], config['alpha'], base_id)
            self.generators.append(generator)
            # make sure that obj ids don't overlap
            base_id += config['m']
        # round robin weights
        self.shares = [config['share'] for config in generators_config]
        self.obj_sizes = [config['size'] for config in generators_config]
        self.i = 0
        
        self.generator_index = 0
        self.generator_cnt = 0

    def get_total_requests(self):
        return self.total_requests
    
    def _move_to_next_generator(self):
        self.generator_index = (self.generator_index + 1) % len(self.generators)
        self.generator_cnt = 0
    
    def next(self):
        generator = self.generators[self.generator_index]
        obj_id, obj_size = generator.next(), self.obj_sizes[self.generator_index]
        self.generator_cnt += 1
        if self.generator_cnt >= self.shares[self.generator_index]:
            self._move_to_next_generator()
        return obj_id, obj_size


class PeriodicGenerator:
    def __init__(self, static_generator_configs, weight_array, request_per_cycle, base_id = 0):
        self.requests_per_generator_per_cycle = [weight/(sum(weight_array)) * request_per_cycle for weight in weight_array]
        
        self.generators = []
        for static_generator_config in static_generator_configs:
            generator = MergedStaticGenerator(static_generator_config, base_id)
            # make sure that obj ids don't overlap
            base_id += sum([config['m'] for config in static_generator_config])
            self.generators.append(generator)
            
        self.requests_per_cycle = request_per_cycle
        
        self.generator_index = 0
        self.generator_cnt = 0
        self.cycle_index = 0
        self.cycle_cnt = 0
    
    def _move_to_next_generator(self):
        self.generator_index = (self.generator_index + 1) % len(self.generators)
        self.generator_cnt = 0
    
    def _move_to_next_cycle(self):
        self.cycle_index += 1
        self.cycle_cnt = 0
        self.generator_index = 0
        self.cycle_cnt = 0
    
    def next(self):
        obj_id, obj_size = self.generators[self.generator_index].next()
        self.generator_cnt += 1
        self.cycle_cnt += 1
        if self.generator_cnt >= self.requests_per_generator_per_cycle[self.generator_index]:
            self._move_to_next_generator()
        if self.cycle_cnt >= self.requests_per_cycle:
            self._move_to_next_cycle()
        return obj_id, obj_size
        

def generate(generator, total_requests, time_span=86400 * 7, output_file=None):
    s = struct.Struct("<IQIq")
    i = 0
    if output_file:
        if output_file.endswith("bin"):
            with open(output_file, "wb") as f:
                while i < total_requests:
                    obj_id, obj_size = generator.next()
                    i += 1
                    ts = i * time_span // total_requests
                    f.write(s.pack(ts, obj_id, obj_size, -2))
        else:
            with open(output_file, "w", newline='') as f:
                writer = csv.writer(f, lineterminator='\n')
                writer.writerow(["clock_time", "object_id", "object_size", "next_access_vtime"])
                while i < total_requests:
                    obj_id, obj_size = generator.next()
                    i += 1
                    ts = i * time_span // total_requests
                    writer.writerow([ts, obj_id, obj_size, -2])
            
    else:
        while i < total_requests:
            obj_id, obj_size = generator.next()
            i += 1
            ts = i * time_span // total_requests
            print(f"{ts} {obj_id} {obj_size}")


def process_config(config):
    if config['type'] == 'static':
        generator = MergedStaticGenerator(config['generators_config'], config['total_requests'])
    elif config['type'] == 'periodic':
        generator = PeriodicGenerator(config['generators_config'], config['weight_array'], config['request_per_cycle'])
    else:
        raise ValueError(f"Unknown generator type: {config['type']}")
    if not force_overwrite and os.path.exists(config['output_file']):
        print(f"File {config['output_file']} already exists. Skipping...")
        return
    generate(generator, config['total_requests'], config['time_span'], config['output_file'])
    print(f"Generated data for {config['output_file']}")

def generate_based_on_config_file(config_file_path):
    with open(config_file_path) as f:
        configs = json.load(f)
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_config, config) for config in configs]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")
        

# Example usage
if __name__ == "__main__":    
    # put the config file here
    generate_based_on_config_file("demo_config.json")
    
    