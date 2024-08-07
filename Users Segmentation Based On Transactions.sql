WITH user_transactions AS (
    SELECT
        "from" AS user,
        COUNT(DISTINCT evt_tx_hash) AS total_transactions
    FROM
        erc20_ethereum.evt_Transfer
    WHERE
        to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC, 0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab)
    GROUP BY 1
    
    UNION ALL
    
    SELECT
        to AS user,
        COUNT(DISTINCT evt_tx_hash) AS total_transactions
    FROM
        erc20_ethereum.evt_Transfer
    WHERE
        "from" IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC, 0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab)
    GROUP BY 1
    
    UNION ALL
    
    SELECT
       DISTINCT "from" AS user,
       COUNT(DISTINCT tx_hash) AS total_transactions
    FROM
        ethereum.traces tr
    WHERE
        to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC)
        AND success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND value > 0
    GROUP BY 1
    
    UNION ALL
    
    SELECT
       DISTINCT to AS user,
       COUNT(DISTINCT tx_hash) AS total_transactions
    FROM
        ethereum.traces tr
    WHERE
        "from" IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC)
        AND success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND value > 0
    GROUP BY 1
),

aggregated_user_transactions AS (
    SELECT
        user,
        SUM(total_transactions) AS total_transactions
    FROM
        user_transactions
    GROUP BY 1
),

segmentation AS (
    SELECT
        user,
        total_transactions,
        CASE
            WHEN total_transactions > 20 THEN 'Power User [ > 20 ]'
            WHEN total_transactions BETWEEN 5 AND 20 THEN 'Regular User [ 5 - 20 ]'
            WHEN total_transactions BETWEEN 2 AND 5 THEN 'Occasional User [ 2 - 5 ]'
            WHEN total_transactions = 1 THEN 'Single-Use User [ 1 ]'
            ELSE 'Unknown'
        END AS user_segment
    FROM
        aggregated_user_transactions
)

    SELECT
        user_segment,
        COUNT(DISTINCT user) AS total_users
    FROM
        segmentation
    GROUP BY 1
    ORDER BY 2 DESC
