WITH all_users AS (
    SELECT
        COUNT(DISTINCT user) AS total_unique
    FROM (
        SELECT
            DISTINCT "from" AS user,
            COUNT(*) AS txn
        FROM
            erc20_ethereum.evt_Transfer
        WHERE
            to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC, 0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab)
        GROUP BY 1
        
        UNION ALL
        
        SELECT
           DISTINCT "from" AS user,
           COUNT(*) AS txn
        FROM
            ethereum.traces tr
        WHERE
            to IN (0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC)
            AND success
            AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
            AND value > 0
        GROUP BY 1
    )
)

    SELECT
        total_unique,
        cummulative_deposits + cummulative_withdraws AS total_transactions
    FROM
        query_3793311 a
    INNER JOIN
        all_users b ON 1=1
    WHERE 
        a.day = CURRENT_DATE
    
