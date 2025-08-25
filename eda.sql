/* Exploratory Data Analysis (EDA) Questions

Fuel Efficiency:
- Average fuel consumption per vehicle type or route segment?
- Variation by time of day or traffic level?
- Correlation between fuel use and congestion or weather severity?

Warehouse & Inventory Insights:
- Peak inventory levels vs. order fulfillment rates?
- Does limited equipment/workforce create delays that propagate to shipments?

Delivery Performance & Bottlenecks:
- Actual vs. scheduled delivery times (ETA variation)?
- Distribution of deviations by route/time period?
- Outlier shipments with extreme loading/unloading times?
- Impact of port/customs congestion on downstream delays?

Supplier & Route Risk:
- Suppliers/routes with longest lead times or lowest reliability?
- Percentage of routes classified as high-risk (accident-prone, congested)?
- Co-occurrence of traffic/weather factors with high-risk events?

Cargo Condition:
- Sensor (e.g. temperature) readings: distributions and anomalies?
- Association between temperature excursions or high-risk tags and cargo damage?
*/


/* 1. Average Fuel Consumption per Risk Classification
What it does:
	- Groups by the categorical risk_classification (e.g. “High Risk”, “Moderate Risk”).
	- Computes the mean fuel_consumption_rate per class.
*/

WITH fuel_by_risk AS (
  SELECT
    risk_classification,
    AVG(fuel_consumption_rate) AS avg_fuel_rate
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
  GROUP BY risk_classification
)
SELECT
  risk_classification,
  ROUND(avg_fuel_rate, 4) AS avg_fuel_rate
FROM fuel_by_risk
ORDER BY avg_fuel_rate DESC;


/* 2. Fuel Variation by Hour of Day & by Traffic Level
What it does:
	- hourly: average fuel by each hour (0–23).
	- by_traffic: average fuel for each numeric congestion level.
	- The final cross-join lets you inspect both dimensions side by side.
*/

WITH
  hourly AS (
    SELECT
      HOUR(timestamp) AS hour_of_day,
      AVG(fuel_consumption_rate) AS avg_fuel_rate
    FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
    GROUP BY hour_of_day
  ),
  by_traffic AS (
    SELECT
      traffic_congestion_level,
      AVG(fuel_consumption_rate) AS avg_fuel_rate
    FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
    GROUP BY traffic_congestion_level
  )
SELECT
  h.hour_of_day,
  ROUND(h.avg_fuel_rate,4) AS fuel_by_hour,
  t.traffic_congestion_level,
  ROUND(t.avg_fuel_rate,4) AS fuel_by_traffic
FROM hourly h
CROSS JOIN by_traffic t
ORDER BY h.hour_of_day, t.traffic_congestion_level;


/* 3. Pearson Correlation of Fuel vs. Traffic & Weather
What it does: Uses window functions to get the sample means, computes covariances, then applies
                ρ= cov(X,Y) / (σX . σY)
                cov(X,Y)=E[XY]−E[X]E[Y]
*/

WITH stats AS (
  SELECT
    -- Means
    AVG(fuel_consumption_rate)          AS mean_fuel,
    AVG(traffic_congestion_level)       AS mean_traffic,
    AVG(weather_condition_severity)     AS mean_weather,

    -- Population standard deviations
    STDDEV_POP(fuel_consumption_rate)       AS std_fuel,
    STDDEV_POP(traffic_congestion_level)    AS std_traffic,
    STDDEV_POP(weather_condition_severity)  AS std_weather,

    -- Means of the cross-products
    AVG(fuel_consumption_rate * traffic_congestion_level)    AS mean_prod_fuel_traffic,
    AVG(fuel_consumption_rate * weather_condition_severity)  AS mean_prod_fuel_weather
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT
  ROUND(
    (mean_prod_fuel_traffic - mean_fuel * mean_traffic)
      / (std_fuel * std_traffic),
    4
  ) AS corr_fuel_traffic,
  ROUND(
    (mean_prod_fuel_weather - mean_fuel * mean_weather)
      / (std_fuel * std_weather),
    4
  ) AS corr_fuel_weather
FROM stats;


/* 4. Peak Daily Inventory vs. Fulfillment Rate
What it does: 
	- Finds each day’s maximum inventory (MAX(…)) and success rate (AVG(…)) of the binary order_fulfillment_status.
*/

WITH daily_stats AS (
  SELECT
    DATE(timestamp)                   AS day,
    MAX(warehouse_inventory_level)    AS peak_inventory,
    AVG(order_fulfillment_status)     AS avg_fulfillment_rate
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
  GROUP BY day
)
SELECT
  day,
  peak_inventory,
  ROUND(avg_fulfillment_rate,4) AS fulfillment_rate
FROM daily_stats
ORDER BY day;


/* 5. Equipment Availability vs. Delivery Delays
What it does:
	- Splits records by the binary handling_equipment_availability.
	- Compares average downstream delays and loading times to assess bottleneck impact.
*/

WITH equip_effect AS (
  SELECT
    handling_equipment_availability,        -- 0 / 1 flag
    AVG(delivery_time_deviation) AS avg_delay,
    AVG(loading_unloading_time)  AS avg_load_time,
    COUNT(*)                     AS cnt
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
  GROUP BY handling_equipment_availability
)
SELECT
  handling_equipment_availability AS equipment_available,
  ROUND(avg_delay,2)       AS avg_delivery_delay_hrs,
  ROUND(avg_load_time,2)   AS avg_loading_time_hrs,
  cnt                       AS record_count
FROM equip_effect
ORDER BY equipment_available DESC;


/* 6. Actual vs. Scheduled Delivery Times (ETA Variation)
Compute daily and hourly summaries of eta_variation_hours (actual − scheduled):
What this does:
	- CTE stamped pulls out dt and hr.
	- daily computes avg/min/max eta_variation_hours per day (AQ2.1).
	- hourly does the same by hour of day (AQ2.2).
	- Final UNION ALL gives you one result set with both daily and hourly patterns.
*/

WITH 
  -- Extract date and hour
  stamped AS (
    SELECT
      timestamp,
      DATE(timestamp)          AS dt,
      HOUR(timestamp)          AS hr,
      eta_variation_hours
    FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
  ),
  -- Daily stats
  daily AS (
    SELECT
      dt,
      ROUND(AVG(eta_variation_hours), 2) AS avg_eta_var,
      ROUND(MIN(eta_variation_hours), 2) AS min_eta_var,
      ROUND(MAX(eta_variation_hours), 2) AS max_eta_var
    FROM stamped
    GROUP BY dt
  ),
  -- Hourly stats (across all days)
  hourly AS (
    SELECT
      hr,
      ROUND(AVG(eta_variation_hours), 2) AS avg_eta_var,
      ROUND(MIN(eta_variation_hours), 2) AS min_eta_var,
      ROUND(MAX(eta_variation_hours), 2) AS max_eta_var
    FROM stamped
    GROUP BY hr
  )
SELECT
  'daily' AS period_type, dt AS period_value,
  avg_eta_var, min_eta_var, max_eta_var
FROM daily
UNION ALL
SELECT
  'hourly', CAST(hr AS CHAR),  -- cast for UNION type consistency
  avg_eta_var, min_eta_var, max_eta_var
FROM hourly
ORDER BY period_type, period_value;


/* 7. Distribution of Delivery Deviations by Time Period
Show percentile breakdown of delivery_time_deviation by day of week and by hour:
*/

WITH dev AS (
  SELECT
    delivery_time_deviation,
    DAYOFWEEK(`timestamp`) - 1    AS dow,   -- 0=Sunday…6=Saturday
    HOUR(`timestamp`)              AS hr
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
),
numbered AS (
  SELECT
    dow,
    hr,
    delivery_time_deviation,
    ROW_NUMBER() OVER (
      PARTITION BY dow, hr
      ORDER BY delivery_time_deviation
    ) AS rn,
    COUNT(*) OVER (
      PARTITION BY dow, hr
    ) AS cnt
  FROM dev
),
percentiles AS (
  SELECT
    dow,
    hr,
    -- compute the row positions for each percentile
    CEIL(cnt * 0.25) AS pos25,
    CEIL(cnt * 0.50) AS pos50,
    CEIL(cnt * 0.75) AS pos75
  FROM numbered
  GROUP BY dow, hr, cnt
),
extracted AS (
  SELECT
    n.dow,
    n.hr,
    MAX(CASE WHEN n.rn = p.pos25 THEN n.delivery_time_deviation END) AS p25,
    MAX(CASE WHEN n.rn = p.pos50 THEN n.delivery_time_deviation END) AS p50,
    MAX(CASE WHEN n.rn = p.pos75 THEN n.delivery_time_deviation END) AS p75
  FROM numbered n
  JOIN percentiles p
    ON n.dow = p.dow
   AND n.hr  = p.hr
  GROUP BY n.dow, n.hr
)
SELECT
  dow,
  hr,
  ROUND(p25, 2)       AS deviation_25th,
  ROUND(p50, 2)       AS deviation_median,
  ROUND(p75, 2)       AS deviation_75th
FROM extracted
ORDER BY dow, hr;


/* 8. Outlier Shipments with Extreme Loading/Unloading Times
Flag shipments whose loading_unloading_time exceeds Q3 + 1.5 IQR:
	- stats CTE finds the first (Q1) and third (Q3) quartile.
	- outliers flags rows beyond Q3 + 1.5 IQR.
	- The final SELECT lists the top 50 extreme cases by unload time (AQ2.3).
*/

WITH numbered_all AS (
  SELECT
    loading_unloading_time,
    delivery_time_deviation,
    `timestamp`,
    ROW_NUMBER() OVER (ORDER BY loading_unloading_time) AS rn,
    COUNT(*)     OVER ()                        AS cnt
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
),
quartiles AS (
  SELECT
    -- Compute the row positions for 25th and 75th percentiles
    CEIL(cnt * 0.25) AS pos25,
    CEIL(cnt * 0.75) AS pos75
  FROM numbered_all
  LIMIT 1
),
q_values AS (
  SELECT
    MAX(CASE WHEN rn = pos25 THEN loading_unloading_time END) AS q1,
    MAX(CASE WHEN rn = pos75 THEN loading_unloading_time END) AS q3
  FROM numbered_all
  CROSS JOIN quartiles
),
outliers AS (
  SELECT
    n.`timestamp`,
    n.loading_unloading_time,
    n.delivery_time_deviation,
    q.q1,
    q.q3,
    (q.q3 - q.q1) * 1.5 AS bound,
    n.loading_unloading_time > (q.q3 + (q.q3 - q.q1) * 1.5) AS is_outlier
  FROM numbered_all AS n
  CROSS JOIN q_values AS q
)
SELECT
  `timestamp`,
  loading_unloading_time,
  delivery_time_deviation,
  bound
FROM outliers
WHERE is_outlier = 1
ORDER BY loading_unloading_time DESC
LIMIT 50;


/* 9. Impact of Port & Customs Congestion on Downstream Delays
Compare average delivery_time_deviation and delay_probability across low/med/high bins of port and customs congestion:
What this does:
	- Bins port_congestion_level into low/medium/high.
	- Bins customs_clearance_time into fast/normal/slow.
	- Aggregates average downstream delays (AQ2.4).
*/

WITH binned AS (
  SELECT
    DATE(timestamp)                AS dt,
    delivery_time_deviation,
    delay_probability,
    -- bin port congestion
    CASE
      WHEN port_congestion_level < 0.33 THEN 'low'
      WHEN port_congestion_level < 0.66 THEN 'medium'
      ELSE 'high'
    END                                AS port_bin,
    -- bin customs clearance time
    CASE
      WHEN customs_clearance_time < 1 THEN 'fast'
      WHEN customs_clearance_time < 3 THEN 'normal'
      ELSE 'slow'
    END                                AS customs_bin
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT
  port_bin,
  customs_bin,
  ROUND(AVG(delivery_time_deviation),2) AS avg_deviation_hrs,
  ROUND(AVG(delay_probability),4)       AS avg_delay_prob
FROM binned
GROUP BY port_bin, customs_bin
ORDER BY FIELD(port_bin,'low','medium','high'),
         FIELD(customs_bin,'fast','normal','slow');


/* 10. Suppliers (by Reliability Decile) with Longest Lead Time / Lowest Reliability
Since there is no explicit supplier_id, we bucket shipments into deciles of supplier_reliability_score, 
then compute lead-time and reliability per bucket:
	- NTILE(10): creates 10 equally‐sized buckets of reliability.
	- Aggregates show which deciles have the worst lead times (AQ4.1) and lowest reliability.
*/

WITH supplier_deciles AS (
  SELECT
    *,
    NTILE(10) OVER (ORDER BY supplier_reliability_score) AS supplier_decile
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
),
supplier_stats AS (
  SELECT
    supplier_decile,
    ROUND(AVG(lead_time_days), 2)             AS avg_lead_time_days,
    ROUND(AVG(supplier_reliability_score), 4) AS avg_reliability_score,
    COUNT(*)                                  AS shipment_count
  FROM supplier_deciles
  GROUP BY supplier_decile
)
SELECT
  supplier_decile,
  avg_lead_time_days,
  avg_reliability_score,
  shipment_count
FROM supplier_stats
ORDER BY avg_lead_time_days DESC
LIMIT 10;


/* 11. Percentage of Routes Classified as High-Risk
We mark a route high-risk when route_risk_level ≥ 7 (you can adjust this threshold):
	- CASE WHEN … THEN 1 counts high-risk rows.
	- Dividing by COUNT(*) yields the percentage (AQ4.2).
*/

SELECT
  ROUND(
    SUM(CASE WHEN route_risk_level >= 7 THEN 1 ELSE 0 END)
    / COUNT(*) * 100,
    2
  ) AS pct_high_risk_routes
FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset;


/* 12. Co-occurrence of Traffic/Weather with High-Risk Events
Compare average congestion & weather severity for high- vs low-risk routes:
	- Splits into High/Low risk groups.
	- Averages show which external factors co-occur with high‐risk (AQ4.3).
*/

WITH risk_flag AS (
  SELECT
    CASE WHEN route_risk_level >= 7 THEN 'High' ELSE 'Low' END AS risk_group,
    traffic_congestion_level,
    weather_condition_severity
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT
  risk_group,
  ROUND(AVG(traffic_congestion_level), 2)    AS avg_traffic_congestion,
  ROUND(AVG(weather_condition_severity), 2)  AS avg_weather_severity,
  COUNT(*)                                   AS record_count
FROM risk_flag
GROUP BY risk_group;


/* 13. Sensor (IoT Temperature) Distribution & Summary Statistics
   Goal:
     • Compute the mean and population standard deviation of iot_temperature.
     • Compute the discrete 25th, 50th (median), and 75th percentiles by assigning each row a ROW_NUMBER() and picking the values
       at ranks FLOOR((cnt-1)*0.25)+1, etc.
   AQ: Supports “Sensor distributions and anomalies” analysis.
*/

WITH ordered AS (
  SELECT
    iot_temperature,
    ROW_NUMBER() OVER (ORDER BY iot_temperature) AS rn,
    COUNT(*)       OVER ()                         AS cnt
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT
  stats.temp_mean,
  stats.temp_std,
  MAX(CASE WHEN rn = FLOOR((cnt - 1) * 0.25) + 1 THEN iot_temperature END) AS temp_q1,
  MAX(CASE WHEN rn = FLOOR((cnt - 1) * 0.50) + 1 THEN iot_temperature END) AS temp_median,
  MAX(CASE WHEN rn = FLOOR((cnt - 1) * 0.75) + 1 THEN iot_temperature END) AS temp_q3
FROM ordered
CROSS JOIN (
  SELECT
    ROUND(AVG(iot_temperature), 2)       AS temp_mean,
    ROUND(STDDEV_POP(iot_temperature), 2) AS temp_std
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
) AS stats
GROUP BY
  stats.temp_mean,
  stats.temp_std;


/* 14. Anomaly Detection: Temperature Excursions (> μ + 3σ)
List timestamps where iot_temperature exceeds mean + 3 × std:
Captures extreme temperature anomalies—key for detecting sensor or environmental outliers (AQ — Sensor anomalies).
*/

WITH stats AS (
  SELECT
    AVG(iot_temperature)     AS mean_temp,
    STDDEV_POP(iot_temperature) AS std_temp
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT
  s.timestamp,
  s.iot_temperature
FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset s
CROSS JOIN stats
WHERE s.iot_temperature > stats.mean_temp + 3 * stats.std_temp;


/* 15. Temperature Excursions & Cargo Condition Association
Compare average cargo_condition_status across records with vs. without a temperature excursion and high‐risk tagging:
	- Flags records where temperature is unusually high (> μ + 2σ).
	- Splits by temp_excursion and high_route_risk.
	- Averages cargo_condition_status show whether excursions and risk‐tags correlate with poorer cargo condition 
      (AQ — Temperature & cargo damage).
*/

WITH flagged AS (
  SELECT
    *,
    CASE 
      WHEN iot_temperature > (
        SELECT AVG(iot_temperature) + 2 * STDDEV_POP(iot_temperature)
        FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
      ) THEN 1 ELSE 0 
    END AS temp_excursion,
    CASE WHEN route_risk_level >= 7 THEN 1 ELSE 0 END AS high_route_risk
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT
  temp_excursion,
  high_route_risk,
  ROUND(AVG(cargo_condition_status), 4) AS avg_cargo_condition
FROM flagged
GROUP BY temp_excursion, high_route_risk
ORDER BY temp_excursion DESC, high_route_risk DESC;













