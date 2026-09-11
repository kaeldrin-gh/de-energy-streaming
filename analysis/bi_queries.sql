-- BI queries behind analysis/findings.md
--
-- Run against the serving database:
--   make bi
-- or manually:
--   docker compose exec -T postgres psql -U energy -d serving < analysis/bi_queries.sql
--
-- Every number in the findings document comes from these queries, at the
-- data window noted in the document. Times are UTC in storage; local hours
-- are Europe/Berlin (DST-aware, computed in the silver layer).

\echo '== A) average price by local hour (the duck curve) =='
SELECT local_hour,
       round(avg(price_eur_mwh)::numeric, 2) AS avg_price,
       count(*) AS n
FROM serving.price_hourly
GROUP BY local_hour
ORDER BY local_hour;

\echo '== B) evening peak vs midday trough =='
SELECT round(avg(price_eur_mwh) FILTER (WHERE local_hour BETWEEN 18 AND 20)::numeric, 2) AS evening_peak,
       round(avg(price_eur_mwh) FILTER (WHERE local_hour BETWEEN 11 AND 14)::numeric, 2) AS midday_trough
FROM serving.price_hourly;

\echo '== C) negative-price totals =='
SELECT count(*) FILTER (WHERE is_negative)                                                        AS neg_hours,
       count(*)                                                                                   AS total_hours,
       round(100.0 * count(*) FILTER (WHERE is_negative) / count(*), 1)                           AS pct_negative,
       round(min(price_eur_mwh)::numeric, 2)                                                      AS min_price,
       round(max(price_eur_mwh)::numeric, 2)                                                      AS max_price
FROM serving.price_hourly;

\echo '== D) negative-price share: weekend vs weekday =='
SELECT is_weekend,
       count(*) FILTER (WHERE is_negative) AS neg_hours,
       count(*)                            AS total,
       round(100.0 * count(*) FILTER (WHERE is_negative) / count(*), 1) AS pct
FROM serving.price_hourly
GROUP BY is_weekend
ORDER BY is_weekend;

\echo '== E) monthly trend =='
SELECT to_char(delivery_ts AT TIME ZONE 'Europe/Berlin', 'YYYY-MM')     AS month,
       count(*) FILTER (WHERE is_negative)                              AS neg_hours,
       count(*)                                                         AS hours,
       round(avg(price_eur_mwh)::numeric, 2)                            AS avg_price,
       round(min(price_eur_mwh)::numeric, 2)                            AS min_price
FROM serving.price_hourly
GROUP BY 1
ORDER BY 1;

\echo '== F) weekend vs weekday average price =='
SELECT is_weekend,
       round(avg(price_eur_mwh)::numeric, 2) AS avg_price,
       count(*)                              AS n
FROM serving.price_hourly
GROUP BY is_weekend
ORDER BY is_weekend;

\echo '== G) weekend midday vs weekday evening (cheapest window of the week) =='
SELECT is_weekend,
       (local_hour BETWEEN 11 AND 14)        AS midday,
       round(avg(price_eur_mwh)::numeric, 2) AS avg_price,
       count(*)                              AS n
FROM serving.price_hourly
WHERE (local_hour BETWEEN 11 AND 14) OR (local_hour BETWEEN 18 AND 20)
GROUP BY 1, 2
ORDER BY 1, 2;

\echo '== H) average intraday spread (max - min per local day) =='
SELECT round(avg(spread)::numeric, 2) AS avg_daily_spread
FROM (
    SELECT to_char(delivery_ts AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') AS d,
           max(price_eur_mwh) - min(price_eur_mwh)                         AS spread
    FROM serving.price_hourly
    GROUP BY 1
) x;

\echo '== I) days with negative hours, and the worst days =='
SELECT count(*) FILTER (WHERE negative_hours > 0) AS days_with_neg,
       max(negative_hours)                        AS max_neg_hours_in_day
FROM serving.daily_stats;

SELECT local_day, avg_price, min_price, max_price, negative_hours
FROM serving.daily_stats
ORDER BY negative_hours DESC
LIMIT 3;
