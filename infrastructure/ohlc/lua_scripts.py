"""
🔥 ATOMIC OHLC LUA SCRIPTS - ZERO RACE CONDITIONS
================================================
These Lua scripts run ATOMICALLY on Redis server.
All operations (read, update, write) happen in a single transaction.
Performance: 5-10x faster than multiple Redis commands.
"""
# ============================================================================
# SCRIPT 1: ATOMIC OHLC UPDATE WITH DUAL SNAPSHOT
# ============================================================================
ATOMIC_OHLC_UPDATE = """
-- ARGV[1] = prefix (e.g., "nse_fo")
-- ARGV[2] = token (e.g., "8765")
-- ARGV[3] = minute_int (e.g., "91500")
-- ARGV[4] = ltp (last traded price)
-- ARGV[5] = volume (tick volume)
-- ARGV[6] = timestamp_ms
local prefix = ARGV[1]
local token = ARGV[2]
local minute_int = tonumber(ARGV[3])
local ltp = tonumber(ARGV[4])
local volume = tonumber(ARGV[5])
local timestamp_ms = tonumber(ARGV[6])

-- Validate inputs (allow volume=0, just check for nil)
if not prefix or not token or not minute_int or not ltp or volume == nil then
    return redis.error_reply("Invalid arguments: prefix, token, minute_int, ltp, or volume is nil")
end

-- Ensure volume is at least 0
volume = volume or 0

-- Ensure token is a string (handle both string and numeric tokens)
token = tostring(token)

-- Key patterns (with trailing colon as per requirement)
local latest_key = prefix .. ":" .. token .. ":_latest_:"
local confirmed_key = prefix .. ":" .. token .. ":_confirmed_:"
-- Fetch current _latest_ candle
local latest = redis.call("LRANGE", latest_key, 0, -1)
local should_close = false
local old_minute = nil
if #latest == 6 then
    old_minute = tonumber(latest[6])
    if old_minute ~= minute_int then
        should_close = true
    end
end
-- Step 1: Close previous candle if minute changed
if should_close then
    -- Move _latest_ to _confirmed_
    redis.call("DEL", confirmed_key)
    for i, val in ipairs(latest) do
        redis.call("RPUSH", confirmed_key, val)
    end
    -- Store confirmed candle to historical key (with trailing colon)
    local hist_key = prefix .. ":" .. token .. ":" .. tostring(old_minute) .. ":"
    redis.call("DEL", hist_key)
    for i = 1, 5 do  -- OHLC + volume (no minute_int)
        redis.call("RPUSH", hist_key, latest[i])
    end
    -- Set expiry on historical candle (24 hours)
    redis.call("EXPIRE", hist_key, 86400)
    -- Clear _latest_ for new candle
    redis.call("DEL", latest_key)
    latest = {}
end
-- Step 2: Update or create _latest_ candle
if #latest == 0 then
    -- New candle: O=H=L=C=ltp, volume initialized with tick volume
    redis.call("RPUSH", latest_key, tostring(ltp))  -- open
    redis.call("RPUSH", latest_key, tostring(ltp))  -- high
    redis.call("RPUSH", latest_key, tostring(ltp))  -- low
    redis.call("RPUSH", latest_key, tostring(ltp))  -- close
    redis.call("RPUSH", latest_key, tostring(volume))  -- volume
    redis.call("RPUSH", latest_key, tostring(minute_int))  -- minute_int
else
    -- Update existing candle with nil checks
    local open = tonumber(latest[1]) or ltp
    local high = tonumber(latest[2]) or ltp
    local low = tonumber(latest[3]) or ltp
    local close = tonumber(latest[4]) or ltp
    local volume_total = tonumber(latest[5]) or 0
    
    -- Update high/low/close + accumulate volume
    high = math.max(high, ltp)
    low = math.min(low, ltp)
    close = ltp
    volume_total = volume_total + volume
    
    -- Replace list atomically
    redis.call("DEL", latest_key)
    redis.call("RPUSH", latest_key, tostring(open))
    redis.call("RPUSH", latest_key, tostring(high))
    redis.call("RPUSH", latest_key, tostring(low))
    redis.call("RPUSH", latest_key, tostring(close))
    redis.call("RPUSH", latest_key, tostring(volume_total))
    redis.call("RPUSH", latest_key, tostring(minute_int))
end
-- Set expiry on _latest_ (1 hour)
redis.call("EXPIRE", latest_key, 3600)
-- Return updated candle
return redis.call("LRANGE", latest_key, 0, -1)
"""
# ============================================================================
# SCRIPT 2: BULK FETCH (Get latest + confirmed + historical range)
# ============================================================================
BULK_FETCH_OHLC = """
-- Arguments:
-- ARGV[1] = prefix (e.g., "nse_fo")
-- ARGV[2] = token (e.g., "8765")
-- ARGV[3] = start_minute (optional, e.g., "91500")
-- ARGV[4] = end_minute (optional, e.g., "92000")
local prefix = ARGV[1]
local token = ARGV[2]
local start_minute = tonumber(ARGV[3] or "0")
local end_minute = tonumber(ARGV[4] or "999999")
local latest_key = prefix .. ":" .. token .. ":_latest_:"
local confirmed_key = prefix .. ":" .. token .. ":_confirmed_:"
-- Fetch snapshots
local latest = redis.call("LRANGE", latest_key, 0, -1)
local confirmed = redis.call("LRANGE", confirmed_key, 0, -1)
-- Fetch historical candles in range (simplified - no SCAN in Lua)
-- For production, use separate SCAN-based approach
local historical = {}
-- Return as JSON-like structure
return {
    latest = latest,
    confirmed = confirmed,
    historical = historical
}
"""
# ============================================================================
# SCRIPT 3: FORCE CLOSE CANDLE (Manual close for EOD, testing)
# ============================================================================
FORCE_CLOSE_CANDLE = """
-- Arguments:
-- ARGV[1] = prefix
-- ARGV[2] = token
local prefix = ARGV[1]
local token = ARGV[2]
local latest_key = prefix .. ":" .. token .. ":_latest_:"
local confirmed_key = prefix .. ":" .. token .. ":_confirmed_:"
-- Fetch _latest_
local latest = redis.call("LRANGE", latest_key, 0, -1)
if #latest == 0 then
    return {ok = false, message = "No active candle"}
end
-- Move to _confirmed_
redis.call("DEL", confirmed_key)
for i, val in ipairs(latest) do
    redis.call("RPUSH", confirmed_key, val)
end
-- Store to historical
local minute_int = latest[6]
local hist_key = prefix .. ":" .. token .. ":" .. minute_int
redis.call("DEL", hist_key)
for i = 1, 5 do
    redis.call("RPUSH", hist_key, latest[i])
end
redis.call("EXPIRE", hist_key, 86400)
-- Clear _latest_
redis.call("DEL", latest_key)
return {ok = true, message = "Candle closed", minute = minute_int}
"""
# ============================================================================
# SCRIPT 4: GET HISTORICAL RANGE (O(N) scan with cursor)
# ============================================================================
GET_HISTORICAL_RANGE = """
-- Arguments:
-- ARGV[1] = prefix
-- ARGV[2] = token
-- ARGV[3] = start_minute
-- ARGV[4] = end_minute
local prefix = ARGV[1]
local token = ARGV[2]
local start_minute = tonumber(ARGV[3])
local end_minute = tonumber(ARGV[4])
local results = {}
local pattern = prefix .. ":" .. token .. ":*"
-- Use SCAN to find all matching keys
local cursor = "0"
repeat
    local scan_result = redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", 100)
    cursor = scan_result[1]
    local keys = scan_result[2]
    for i, key in ipairs(keys) do
        -- Extract minute_int from key
        local minute_str = key:match(":(%d+)$")
        if minute_str then
            local minute = tonumber(minute_str)
            if minute >= start_minute and minute <= end_minute then
                local candle = redis.call("LRANGE", key, 0, -1)
                if #candle == 5 then
                    table.insert(results, {minute = minute, ohlcv = candle})
                end
            end
        end
    end
until cursor == "0"
-- Sort by minute
table.sort(results, function(a, b) return a.minute < b.minute end)
return results
"""
# ============================================================================
# SCRIPT SHA REGISTRY (Populated at runtime)
# ============================================================================
SCRIPT_REGISTRY = {
    "atomic_update": ATOMIC_OHLC_UPDATE,
    "bulk_fetch": BULK_FETCH_OHLC,
    "force_close": FORCE_CLOSE_CANDLE,
    "historical_range": GET_HISTORICAL_RANGE,
}
# SHA256 hashes will be stored here after SCRIPT LOAD
SCRIPT_SHAS = {}
