--Bridge contract (ETH) - 0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC
--Bridge contract (ERC20) - 0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab

WITH time_seq AS (
    SELECT 
        sequence(
        CAST('2024-05-21' as timestamp),
        date_trunc('day', cast(now() as timestamp)),
        interval '1' day
        ) AS time 
),

days AS (
    SELECT 
        time.time AS block_day 
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),

prices AS (
    SELECT
        DATE_TRUNC('day', minute) AS day,
        contract_address,
        symbol,
        MAX_BY(price, minute) AS price
    FROM    
        prices.usd
    WHERE blockchain = 'ethereum'
    GROUP BY 1,2,3
),

dex_prices as (
    SELECT 
        DATE_TRUNC('day', hour) as day,
        contract_address,
        MAX_BY(median_price, hour) as price 
    FROM 
    dex.prices
    WHERE blockchain = 'ethereum'
    GROUP BY 1, 2 
),

all_balances AS (
    SELECT
        day,
        contract_address,
        symbol,
        SUM(value) AS value
    FROM (
        SELECT
             DATE_TRUNC('day', e.evt_block_time) AS day,
             e.contract_address,
             symbol,
             SUM(value / POW(10, t.decimals)) AS value
         FROM
            erc20_ethereum.evt_Transfer e
         LEFT JOIN 
            tokens.erc20 t ON e.contract_address = t.contract_address
                           AND t.blockchain = 'ethereum'
        WHERE
            to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC, 0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab)
        GROUP BY 1,2,3
    
        UNION ALL
    
        SELECT
            DATE_TRUNC('day', e.evt_block_time) AS day,
            e.contract_address,
            symbol,
            -SUM(value / POW(10, t.decimals)) AS value
        FROM 
             erc20_ethereum.evt_Transfer e
        LEFT JOIN 
            tokens.erc20 t ON e.contract_address = t.contract_address
                           AND t.blockchain = 'ethereum'
        WHERE
            "from" IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC, 0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab)
        GROUP BY 1,2,3
        
        UNION ALL
        
        SELECT 
            DATE_TRUNC('day',block_time) AS day,
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
            'ETH' AS symbol,
            -SUM(CAST(value AS DOUBLE) / POWER(10, 18)) AS value
        FROM 
            ethereum.traces tr
        WHERE 
            "from" IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC)
            AND success
            AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
           -- AND block_time > timestamp '2023-06-15'
        GROUP BY 1,2
        
        UNION ALL
        
        SELECT 
            DATE_TRUNC('day',block_time) AS day,
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
            'ETH' AS symbol,
            SUM(CAST(value AS DOUBLE) / POWER(10, 18)) AS value
        FROM 
            ethereum.traces tr
        WHERE 
            to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC)
            AND success
            AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
           -- AND block_time > timestamp '2023-06-15'
        GROUP BY 1,2
)
    GROUP BY 1, 2, 3
),

daily_balances AS (
    SELECT 
        day, 
        contract_address,
        symbol,
        SUM(COALESCE(value, 0)) OVER (PARTITION BY contract_address, symbol ORDER BY day) AS balance,
        LEAD(day, 1, current_timestamp) OVER (PARTITION BY contract_address, symbol ORDER BY day ASC) as next_day
    FROM
        all_balances
 ) 
 
     SELECT
        block_day AS day,
        db.contract_address,
        db.symbol,
        COALESCE(b.price, c.price) as price,
        balance,
        balance * COALESCE(b.price, c.price) AS balance_usd
    FROM    
        daily_balances db
    INNER JOIN 
        days d ON db.day <= d.block_day AND d.block_day < db.next_day
    LEFT JOIN
      prices b ON db.day = b.day AND db.contract_address = b.contract_address
    LEFT JOIN
      dex_prices c ON db.day = c.day AND db.contract_address = c.contract_address
       /*
)

    SELECT
        *,
        SUM(balance_usd) OVER(PARTITION BY day) AS cummulative_tvl
    FROM    
        balances_usd
    WHERE 
        balance > 0
    --WHERE day = date_trunc('day', CURRENT_DATE)
    ORDER BY day DESC, symbol DESC  
*/
