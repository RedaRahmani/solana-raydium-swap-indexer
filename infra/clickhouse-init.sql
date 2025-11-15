CREATE TABLE IF NOT EXISTS swaps.raydium_swaps_raw (
    id UUID,
    slot UInt64,
    idx UInt64,
    is_full_entry UInt8,
    raw String,
    ts DateTime
) ENGINE = MergeTree()
ORDER BY (ts, slot, idx);
