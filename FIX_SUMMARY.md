# Producer Proliferation Fix - Summary

## Problem
Multiple Kafka producer instances were being created when only ONE should exist per UserRestProxy, causing memory leaks and connection churn.

**Symptoms:**
- 37+ producers created in 3 minutes locally
- 845 TIME_WAIT sockets in production
- ~12 connections/second churn rate
- Memory growth from SSL buffers (not Python heap)

## Root Cause
When `UserRestProxy.__init__()` failed (e.g., due to missing SASL credentials), the exception was not caught, so the proxy was never added to `self.proxies`. On the next request, the code would try to create a NEW UserRestProxy, which would fail again, creating another producer attempt.

**Code path:**
1. Request comes in → `get_user_proxy()` called
2. Check `if self.proxies.get(key) is None` → TRUE (key not in dict)
3. Try to create `UserRestProxy(...)` → Exception raised in `__init__`
4. Exception not caught → proxy not added to `self.proxies`
5. Next request → repeat from step 1

## Solution
**Cache failed UserRestProxy creation attempts** to prevent retry on every request.

### Changes Made

**File:** `src/karapace/kafka_rest_apis/__init__.py`

1. **Catch all exceptions** when creating UserRestProxy (not just specific ones)
2. **Mark failed attempts** by setting `self.proxies[key] = None`
3. **Check for cached failures** using `key not in self.proxies` instead of `self.proxies.get(key) is None`
4. **Return immediately** for cached failures without retrying

### Key Code Changes

**Before:**
```python
if self.proxies.get(key) is None:
    self.proxies[key] = UserRestProxy(...)  # Exception here = not added to dict
```

**After:**
```python
if key not in self.proxies:
    try:
        self.proxies[key] = UserRestProxy(...)
    except Exception as e:
        self.proxies[key] = None  # Cache the failure
        self.r(body={"message": "Forbidden"}, ...)
elif self.proxies[key] is None:
    # Previously failed - return immediately
    self.r(body={"message": "Forbidden"}, ...)
```

## Test Results

**Before fix:**
- 30 requests = 30 UserRestProxy creation attempts
- 30 "Failed to create UserRestProxy" messages
- Memory grows continuously

**After fix:**
- 10 requests = 1 UserRestProxy creation attempt + 9 cached failures
- 1 "Failed to create UserRestProxy" message
- 9 "Proxy creation previously failed" messages
- No additional producer creation

## Verification

```bash
# Send 10 requests
for i in {1..10}; do
  curl -X POST -H "Content-Type: application/vnd.kafka.binary.v2+json" \
    -d '{"records": [{"value": "dGVzdA=="}]}' \
    http://${IPADDRESS}:30002/topics/testtopic1
done

# Check logs
kubectl logs deployment/karapace-rest-deployment | grep -E "(EXCEPTION CAUGHT|Previously failed)"
```

**Expected output:**
- 1x "EXCEPTION CAUGHT! Failed to create UserRestProxy"
- 9x "Proxy creation previously failed for unauthenticated requests"

## Impact

### Memory Leak Fixed
- **Before:** 37 producers in 3 minutes = ~12/minute = continuous memory growth
- **After:** 1 producer attempt, then cached = no additional memory growth

### Connection Churn Fixed
- **Before:** 845 TIME_WAIT sockets in production
- **After:** Minimal connections (only successful ones)

### Performance Improved
- **Before:** Every request attempts UserRestProxy creation (expensive)
- **After:** Failed attempts cached, subsequent requests return immediately

## Deployment

**Branch:** `fix/producer-proliferation-memory-leak`
**Commit:** `ad2fb207`

**Files changed:**
- `src/karapace/kafka_rest_apis/__init__.py` (main fix)
- `src/karapace/kafka_rest_apis/__main__.py` (debug logging)
- `src/karapace/__main__.py` (debug logging)

## Next Steps

1. ✅ Fix implemented and tested locally
2. ✅ Committed and pushed to GitHub
3. 🔄 Create pull request
4. 🔄 Review and merge
5. 🔄 Deploy to production
6. 🔄 Monitor memory and TIME_WAIT sockets
7. 🔄 Verify fix resolves production issue

## Notes

- The fix handles ALL exceptions, not just specific Kafka exceptions
- Failed proxies are cached indefinitely (could add TTL if needed)
- The fix works for both authenticated and unauthenticated requests
- Debug logging added to track proxy creation vs reuse
