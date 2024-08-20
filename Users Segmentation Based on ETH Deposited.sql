WITH categorized_deposits AS (
    SELECT
        "from" AS user,
        DATE_TRUNC('day', block_time) AS day,
        SUM(value / 1e18) AS eth_deposited
    FROM
        ethereum.traces
    WHERE
        to = 0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC
        AND success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    GROUP BY 1, 2
),

categorized_users AS (
    SELECT
        *,
        CASE
            WHEN eth_deposited < 0.1 THEN 'Small Depositor [ < 0.1 ETH ]'
            WHEN eth_deposited BETWEEN 0.1 AND 1 THEN 'Medium Depositor [ 0.1 ETH - 1 ETH ]'
            ELSE 'Large Depositor [ > 1 ETH ]'
        END AS user_category
    FROM
        categorized_deposits
)

    SELECT
        user_category,
        COUNT(DISTINCT user) AS user_count,
        SUM(eth_deposited) AS total_eth_deposited
    FROM
        categorized_users
    GROUP BY 1
