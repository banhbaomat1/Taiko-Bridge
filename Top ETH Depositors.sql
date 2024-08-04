WITH prices AS (
    SELECT
        contract_address,
        symbol,
        MAX_BY(price, minute) AS price
    FROM    
        prices.usd
    WHERE
        blockchain = 'ethereum'
        AND DATE_TRUNC('day', minute) = CURRENT_DATE
        AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    GROUP BY 1,2
),
 
raw_balance AS (   
    SELECT
        "from" AS user,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
        COUNT(tx_hash) AS txn,
        SUM(value / POW(10, 18)) AS value
    FROM
        ethereum.traces tr
    WHERE
        to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC)
        AND success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND value > 0
    GROUP BY 1, 2
 ),
 
 top_users AS (
    SELECT
        get_href(get_chain_explorer_address('ethereum', CAST(user AS VARCHAR)), CAST(user AS VARCHAR)) AS address,
        value,
        txn,
        value * price AS eth_bridged
    FROM    
        raw_balance rb
    LEFT JOIN
        prices p ON rb.contract_address = p.contract_address
),

add_rank as (
    SELECT 
        CASE 
            WHEN rank_ = 1 THEN '🥇'
            WHEN rank_ = 2 THEN '🥈'
            WHEN rank_ = 3 THEN '🥉'
            ELSE CONCAT('#', CAST(rank_ as VARCHAR))
        END as rank_with_emojis,
        * 
        FROM (
        SELECT 
            *, 
            ROW_NUMBER() OVER (ORDER BY eth_bridged DESC) as rank_
        FROM 
           top_users
    ) x )

    SELECT
        *
    FROM 
        add_rank
    ORDER BY eth_bridged DESC
    LIMIT 25
