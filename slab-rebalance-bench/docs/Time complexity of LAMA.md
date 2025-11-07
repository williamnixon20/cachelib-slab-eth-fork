Suppose only 1 class, m unique objects and n total requests

1. First pass: scan all requests in the buffered window, keeping track of 
   - the first access of each unique object: firstAccessTime
   - the last access of each unique object: lastAccessTime
   - reuse time of each request: reuseTimeHistogram

time complexity: O(n)

2. Second pass: calculate footprints
   precomputation: 
   - sort the first access time and last access time of each unique object: <s>O(m * logm)</s> (optimized to O(m))
   - prefix sum for the reuseTimeHistogram: worst case O(n), could be cheapter though, depends on how many different reuse times there are.
   - footprint for each window size:
   O(m + n)

3. from footprint values to MRC:
   O(n)

Total: O(m) + O(n)

Another tricky thing for LAMA is that the request serving thread writes to the circular buffer while the rebalancing thread needs to read from it, there can be race conditions. Blocking request serving for rebalancing would be way too expensive, so to minimize the critical section, in rebalancing I make a copy of the cirular buffer, which introduces extra memory overheads.