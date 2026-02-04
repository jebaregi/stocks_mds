SELECT
    COALESCE(v:c, v:quote.c, v:data.c)::FLOAT AS current_price,
    COALESCE(v:d, v:quote.d, v:data.d)::FLOAT AS change_amount,
    COALESCE(v:dp,v:quote.dp, v:data.dp)::FLOAT AS change_percent,
    COALESCE(v:h, v:quote.h, v:data.h)::FLOAT AS day_high,
    COALESCE(v:l, v:quote.l, v:data.l)::FLOAT AS day_low,
    COALESCE(v:o,v:quote.o, v:data.o)::FLOAT AS day_open,
    COALESCE(v:pc,v:quote.pc, v:data.pc)::FLOAT AS prev_close,
    v:"symbol"::STRING  AS symbol,
    v:"timestamp"::timestamp AS market_timestamp
FROM {{ source('raw', 'BRONZE_STOCKS_QUOTES_RAW') }}