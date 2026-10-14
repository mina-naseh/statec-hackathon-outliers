SELECT
    period,
    SUM(CASE WHEN flow = 1 THEN value_in_euros ELSE 0 END) AS imports_eur,
    SUM(CASE WHEN flow = 2 THEN value_in_euros ELSE 0 END) AS exports_eur
FROM comext_monthly
WHERE declarant = 'LU'
GROUP BY period
ORDER BY period;
