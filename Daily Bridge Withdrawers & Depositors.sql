WITH time_seq AS (
    SELECT 
        sequence(
        CAST('2024-05-26' as timestamp),
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

all_deposits AS (
SELECT
    day,
    SUM(no_of_deposits) AS deposits,
    SUM(no_of_depositors) AS depositors
FROM (
   SELECT
        DATE_TRUNC('day', evt_block_time) AS day,
        COUNT(evt_tx_hash) AS no_of_deposits,
        COUNT(DISTINCT "from") AS no_of_depositors
   FROM
        erc20_ethereum.evt_Transfer 
    WHERE
        to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC, 0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab)
    GROUP BY 1 
    
    UNION ALL
    
    SELECT
        DATE_TRUNC('day', block_time) AS day,
        COUNT(tx_hash) AS no_of_deposits,
        COUNT(DISTINCT "from") AS no_of_depositors
    FROM
        ethereum.traces tr
    WHERE
        to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC)
        AND success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND value > 0
    GROUP BY 1 
)
GROUP BY 1
),

all_withdraw AS (
SELECT
    day,
    SUM(no_of_withdraws) AS withdraws,
    SUM(no_of_withdrawers) AS withdrawers
FROM (
   SELECT
        DATE_TRUNC('day', evt_block_time) AS day,
        COUNT(DISTINCT evt_tx_hash) AS no_of_withdraws,
        COUNT(DISTINCT to) AS no_of_withdrawers
   FROM
        erc20_ethereum.evt_Transfer 
    WHERE
        "from" IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC, 0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab)
    GROUP BY 1 

    UNION ALL
  
    SELECT
        DATE_TRUNC('day', block_time) AS day,
        COUNT(DISTINCT tx_hash) AS no_of_withdraws,
        COUNT(DISTINCT to) AS no_of_withdrawers
    FROM
        ethereum.traces tr
    WHERE
        "from" IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC)
        AND success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND value > 0
    GROUP BY 1 
)
GROUP BY 1
)

    SELECT
        block_day AS day,
        COALESCE(deposits, 0) AS total_deposits,
        COALESCE(withdraws, 0) AS total_withdraws,
        COALESCE(depositors, 0) AS total_depositors,
        COALESCE(withdrawers, 0) AS total_withdrawers,
        SUM(deposits) OVER(ORDER BY block_day) AS cummulative_deposits,
        SUM(withdraws) OVER(ORDER BY block_day) AS cummulative_withdraws
    FROM
        days a
    LEFT JOIN 
        all_deposits b ON a.block_day = b.day
    LEFT JOIN 
        all_withdraw c ON a.block_day = c.day    

