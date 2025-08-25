/* ADVANCED ANALYSIS & PREDICTIVE QUESTIONS
1. Disruption & Delay Prediction:
	- Predict likelihood of shipment delays using historical + live data (traffic, weather).
	- Identify top predictors (e.g. congestion spikes, previous delays).

2. Route & Schedule Optimization:
	- Recommend reroutes or reschedules to minimize fuel and delays under current forecasts.
	- Allocate loading/unloading windows based on warehouse capacity.

3. Demand Forecasting:
	- Forecast future transport capacity needs using seasonality and external indicators.

4. Driver Safety & Performance:
	- Predict safety incidents or inefficiency from driver behavior data.
	- Link behavior scores to fuel use or ETA variation for proactive training.

5. Proactive Equipment Management: Forecast equipment (forklift, dock) downtime from usage logs to schedule maintenance.
*/

/* 16. Disruption & Delay Prediction
16.1. Build a feature matrix with lagged traffic & weather:
	- CTE daily_agg aggregates raw data by day (dt) and route_risk_level, computing mean traffic, weather, delay, and a binary 
	  delay_flag (AQ6).
	- Window/LAG brings in yesterday’s conditions as predictors for modern ML workflows.
*/

WITH daily_agg AS (
  SELECT
    DATE(timestamp)                             AS dt,
    route_risk_level,
    -- aggregate predictors
    AVG(traffic_congestion_level)               AS avg_traffic,
    AVG(weather_condition_severity)             AS avg_weather,
    AVG(delivery_time_deviation)                AS avg_delay,
    -- binary delay flag (1 if deviation > 1 hr)
    AVG(CASE WHEN delivery_time_deviation > 1 THEN 1 ELSE 0 END)  
                                               AS delay_flag
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
  GROUP BY dt, route_risk_level
),
lagged_feats AS (
  SELECT
    *,
    -- 1-day lag of traffic & weather
    LAG(avg_traffic, 1) OVER (PARTITION BY route_risk_level ORDER BY dt) AS traffic_lag1,
    LAG(avg_weather, 1) OVER (PARTITION BY route_risk_level ORDER BY dt) AS weather_lag1,
    LAG(avg_delay,   1) OVER (PARTITION BY route_risk_level ORDER BY dt) AS delay_lag1
  FROM daily_agg
)
SELECT
  dt,
  route_risk_level,
  avg_traffic, traffic_lag1,
  avg_weather, weather_lag1,
  avg_delay,   delay_lag1,
  delay_flag
FROM lagged_feats
WHERE traffic_lag1 IS NOT NULL    -- drop the first day per route
  AND weather_lag1 IS NOT NULL
  AND delay_lag1   IS NOT NULL;
  

/* 16.2 Rank top predictors by correlation with delay_flag;
CORR(...) ranks features by absolute correlation with delivery_time_deviation → guides feature selection for your predictive model.
MySQL doesn’t support the direct covariance and correlation aggregates, so we must compute Pearson’s 𝑟 manually via the definition:
							rX,Y = E[XY] − E[X]E[Y] / {(E[X**2] − E[X]**2) . (E[Y**2] − E[Y]**2)
*/

WITH corr_stats AS (
  SELECT
    -- traffic vs delay
    (AVG(traffic_congestion_level * delivery_time_deviation)
      - AVG(traffic_congestion_level) * AVG(delivery_time_deviation)
    )
    /
    (SQRT(AVG(traffic_congestion_level * traffic_congestion_level)
          - POWER(AVG(traffic_congestion_level), 2)
     )
     *
     SQRT(AVG(delivery_time_deviation * delivery_time_deviation)
          - POWER(AVG(delivery_time_deviation), 2)
     )
    ) AS corr_traffic_delay,

    -- weather vs delay
    (AVG(weather_condition_severity * delivery_time_deviation)
      - AVG(weather_condition_severity) * AVG(delivery_time_deviation)
    )
    /
    (SQRT(AVG(weather_condition_severity * weather_condition_severity)
          - POWER(AVG(weather_condition_severity), 2)
     )
     *
     SQRT(AVG(delivery_time_deviation * delivery_time_deviation)
          - POWER(AVG(delivery_time_deviation), 2)
     )
    ) AS corr_weather_delay,

    -- ETA variation vs delay
    (AVG(eta_variation_hours * delivery_time_deviation)
      - AVG(eta_variation_hours) * AVG(delivery_time_deviation)
    )
    /
    (SQRT(AVG(eta_variation_hours * eta_variation_hours)
          - POWER(AVG(eta_variation_hours), 2)
     )
     *
     SQRT(AVG(delivery_time_deviation * delivery_time_deviation)
          - POWER(AVG(delivery_time_deviation), 2)
     )
    ) AS corr_eta_delay,

    -- route risk vs delay
    (AVG(route_risk_level * delivery_time_deviation)
      - AVG(route_risk_level) * AVG(delivery_time_deviation)
    )
    /
    (SQRT(AVG(route_risk_level * route_risk_level)
          - POWER(AVG(route_risk_level), 2)
     )
     *
     SQRT(AVG(delivery_time_deviation * delivery_time_deviation)
          - POWER(AVG(delivery_time_deviation), 2)
     )
    ) AS corr_risk_delay

  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)

SELECT feature, corr_value
FROM (
  SELECT 'traffic_congestion_level'    AS feature, corr_traffic_delay   AS corr_value FROM corr_stats
  UNION ALL
  SELECT 'weather_condition_severity', corr_weather_delay            FROM corr_stats
  UNION ALL
  SELECT 'eta_variation_hours',         corr_eta_delay               FROM corr_stats
  UNION ALL
  SELECT 'route_risk_level',            corr_risk_delay              FROM corr_stats
) AS t
ORDER BY ABS(corr_value) DESC;


/* 17. Route & Schedule Optimization
17.1. Recommend top-K low-delay+low-fuel routes
	- Aggregates every route_risk_level into its mean fuel+delay.
	- RANK() window sorts them by lowest delay then lowest fuel, yielding your top 5 route recommendations (AQ7).
*/

WITH route_stats AS (
  SELECT
    route_risk_level,  
    AVG(fuel_consumption_rate)   AS avg_fuel,
    AVG(delivery_time_deviation) AS avg_delay
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
  GROUP BY route_risk_level
),
ranked_routes AS (
  SELECT
    route_risk_level,
    avg_fuel,
    avg_delay,
    RANK() OVER (ORDER BY avg_delay ASC, avg_fuel ASC) AS route_rank
  FROM route_stats
)
SELECT
  route_risk_level AS recommended_route,
  avg_fuel,
  avg_delay
FROM ranked_routes
WHERE route_rank <= 5
ORDER BY route_rank;


/* 17.2 Allocate unloading windows by daily warehouse capacity
	- bucketed CTE: Calculates dt and only warehouse_bucket with NTILE(5) (no nested window calls).
	- sequenced CTE: Takes the results of bucketed and applies ROW_NUMBER() partitioned by the already-computed dt, warehouse_bucket.
	- Final SELECT: Groups by dt, warehouse_bucket, unload_window. 
                    It uses CEIL(seq_in_bucket/10) to assign each shipment to one of 10-shipment windows.
	- Final result tells you, per day and warehouse, which time window each ship belongs to and its load-time profile, 
	  supporting optimized scheduling (AQ7).
*/

WITH bucketed AS (
  SELECT
    DATE(`timestamp`)                                AS dt,
    NTILE(5) OVER (ORDER BY warehouse_inventory_level) AS warehouse_bucket,
    order_fulfillment_status,
    loading_unloading_time
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
),
sequenced AS (
  SELECT
    dt,
    warehouse_bucket,
    order_fulfillment_status,
    loading_unloading_time,
    ROW_NUMBER() OVER (
      PARTITION BY dt, warehouse_bucket
      ORDER BY loading_unloading_time DESC
    ) AS seq_in_bucket
  FROM bucketed
)
SELECT
  dt,
  warehouse_bucket,
  CEIL(seq_in_bucket / 10)                         AS unload_window,
  COUNT(*)                                         AS shipments_in_window,
  ROUND(AVG(loading_unloading_time), 2)            AS avg_load_time
FROM sequenced
GROUP BY dt, warehouse_bucket, unload_window
ORDER BY dt, warehouse_bucket, unload_window;


/* 18. Demand Forecasting
Goal: Build a simple “seasonal + lagged” forecast by weekday, last-week and last-year demand.
	- daily_demand: sums your historical_demand by date.
	- Window funcs:
		- AVG(...) OVER (PARTITION BY DAYOFWEEK(dt)) captures seasonality by weekday.
		- LAG(..., 7) and LAG(..., 365) bring in last-week/last-year values.
	- Final SELECT blends these into a simple forecast metric.
*/

WITH
-- 1. Aggregate to daily totals
daily_demand AS (
  SELECT
    DATE(`timestamp`)                AS dt,
    SUM(historical_demand)           AS total_demand
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
  GROUP BY dt
),

-- 2. Compute per‐weekday average, overall average, and lag features
demand_stats AS (
  SELECT
    dt,
    total_demand,
    DAYOFWEEK(dt)                                AS weekday,  
    AVG(total_demand) OVER (PARTITION BY DAYOFWEEK(dt)) AS avg_weekday_demand,
    AVG(total_demand) OVER ()                    AS avg_daily_demand,
    LAG(total_demand, 7)  OVER (ORDER BY dt)      AS demand_last_week,
    LAG(total_demand, 365) OVER (ORDER BY dt)     AS demand_last_year
  FROM daily_demand
)

-- 3. Produce a naïve ensemble forecast for the next day
SELECT
  dt,
  total_demand,
  ROUND(avg_weekday_demand, 2)     AS avg_weekday_demand,
  ROUND(avg_daily_demand, 2)       AS avg_daily_demand,
  demand_last_week,
  demand_last_year,
  -- simple average of the three predictors
  ROUND(
    (avg_weekday_demand + demand_last_week + demand_last_year) / 3,
    2
  )                                  AS forecast_for_next_day
FROM demand_stats
ORDER BY dt DESC
LIMIT 30;


/* 19. Driver Safety & Performance
Goal: Bin drivers by behavior score and quantify how average fuel use, ETA variation, and safety‐incident rates vary 
      across bins (proxies for “inefficiency” or risk).
	- incidents: creates a binary flag whenever behavior/fatigue is below 0.5 or predicted delay risk > 0.7.
	- NTILE(4): splits drivers into four equal-sized bins by driver_behavior_score.
	- Aggregations: compare fuel_consumption_rate, eta_variation_hours, and the safety_incident rate across bins—guiding targeted training.
*/

WITH
-- 1. Flag “safety incidents” when behavior or fatigue is low OR delay‐probability is high
incidents AS (
  SELECT
    *,
    CASE
      WHEN driver_behavior_score   < 0.5
        OR fatigue_monitoring_score < 0.5
        OR delay_probability        > 0.7
      THEN 1
      ELSE 0
    END AS safety_incident
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
),

-- 2. Assign drivers into quartiles by behavior score
behavior_quartiles AS (
  SELECT
    *,
    NTILE(4) OVER (ORDER BY driver_behavior_score) AS behavior_quartile
  FROM incidents
),

-- 3. Aggregate metrics per behavior quartile
quartile_summary AS (
  SELECT
    behavior_quartile,
    ROUND(AVG(fuel_consumption_rate),3)   AS avg_fuel_rate,
    ROUND(AVG(eta_variation_hours),3)      AS avg_eta_variation,
    ROUND(AVG(safety_incident),3)          AS incident_rate
  FROM behavior_quartiles
  GROUP BY behavior_quartile
)

SELECT
  behavior_quartile        AS driver_bin,
  avg_fuel_rate,
  avg_eta_variation,
  incident_rate
FROM quartile_summary
ORDER BY behavior_quartile;


/* 20. Proactive Equipment Management
Goal: Detect and characterize equipment “downtime events” (when handling_equipment_availability = 0), compute their durations, 
      then roll up monthly averages and a 6-month rolling mean to forecast future maintenance windows.
	- Event detection: use LAG(...) to catch 1→0 and 0→1 transitions.
	- TIMESTAMPDIFF(HOUR, …) computes each downtime span.
	- Monthly aggregation: average downtime length and count of events.
	- Windowed rolling avg: averages the current month plus the previous five to smooth seasonality and anticipate future downtime.
*/

WITH
-- 1. Identify transitions into downtime (1→0) and out of downtime (0→1)
equip_events AS (
  SELECT
    `timestamp`,
    handling_equipment_availability,
    LAG(handling_equipment_availability) OVER (ORDER BY `timestamp`) AS prev_avail
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
),

downtime_starts AS (
  SELECT `timestamp` AS start_time
  FROM equip_events
  WHERE handling_equipment_availability = 0
    AND prev_avail = 1
),

downtime_ends AS (
  SELECT `timestamp` AS end_time
  FROM equip_events
  WHERE handling_equipment_availability = 1
    AND prev_avail = 0
),

-- 2. Pair each start with the next end to compute duration in hours
durations AS (
  SELECT
    s.start_time,
    MIN(e.end_time) AS end_time,
    TIMESTAMPDIFF(
      HOUR,
      s.start_time,
      MIN(e.end_time)
    )                 AS downtime_hours
  FROM downtime_starts s
  JOIN downtime_ends e
    ON e.end_time > s.start_time
  GROUP BY s.start_time
),

-- 3. Roll up to monthly stats
monthly_downtime AS (
  SELECT
    YEAR(start_time)  AS yr,
    MONTH(start_time) AS mth,
    ROUND(AVG(downtime_hours),1) AS avg_downtime_hours,
    COUNT(*)                    AS event_count
  FROM durations
  GROUP BY yr, mth
)

-- 4. Add 6-month rolling average of downtime to project future needs
SELECT
  yr,
  mth,
  avg_downtime_hours,
  event_count,
  ROUND(
    AVG(avg_downtime_hours)
      OVER (
        ORDER BY CONCAT(yr, LPAD(mth,2,'0'))
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
      ),
    1
  ) AS rolling_6mo_avg_downtime
FROM monthly_downtime
ORDER BY yr, mth;


/* 21. Grouped Analyses per Warehouse & per Supplier
Since the dataset don’t have a real warehouse_id, we’ll derive one via quartiles of inventory. We do have 
supplier_reliability_score to bucket suppliers.
*/

-- Overall performance per warehouse_quartile
WITH enriched AS (
  SELECT
    *,
    NTILE(4) OVER (ORDER BY warehouse_inventory_level)     AS warehouse_quartile,
    NTILE(10) OVER (ORDER BY supplier_reliability_score)   AS supplier_decile,
    DATE(timestamp)                                        AS dt
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
),
daily_wh AS (
  SELECT
    dt,
    warehouse_quartile,
    AVG(warehouse_inventory_level)   AS avg_inventory,
    AVG(order_fulfillment_status)    AS avg_fulfillment_rate
  FROM enriched
  GROUP BY 1,2
)
SELECT
  warehouse_quartile,
  ROUND(AVG(avg_inventory),2)        AS overall_avg_inventory,
  ROUND(AVG(avg_fulfillment_rate),4)  AS overall_fulfillment_rate
FROM daily_wh
GROUP BY 1
ORDER BY overall_fulfillment_rate DESC;

-- 2. Top 5 supplier deciles by lead time
WITH enriched AS (
  SELECT
    *,
    NTILE(4) OVER (ORDER BY warehouse_inventory_level)     AS warehouse_quartile,
    NTILE(10) OVER (ORDER BY supplier_reliability_score)   AS supplier_decile,
    DATE(timestamp)                                        AS dt
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
),
daily_sp AS (
  SELECT
    dt,
    supplier_decile,
    AVG(lead_time_days)              AS avg_lead_time,
    AVG(supplier_reliability_score)  AS avg_reliability
  FROM enriched
  GROUP BY 1,2
)
SELECT
  supplier_decile,
  ROUND(AVG(avg_lead_time),2)       AS overall_avg_lead_time,
  ROUND(AVG(avg_reliability),4)     AS overall_avg_reliability
FROM daily_sp
GROUP BY 1
ORDER BY overall_avg_lead_time DESC
LIMIT 5;


/* 22. Time-Lagged Correlation of Equipment Shortages → Delay
MySQL doesn’t ship with a built-in CORR() function, so one need to compute Pearson’s r by hand
Below is a single SQL script that:
	- builds your daily and lagged CTEs just as before, 
    - computes the means, covariance and variances in a second CTE, and
    - returns both same-day and next-day correlations:
daily and lagged: exactly as before—aggregate daily rates and introduce the next-day equipment metric.
In stats_lag1, changed m.mean_y to MAX(m.mean_y), so it’s now a valid aggregated expression under ONLY_FULL_GROUP_BY.
*/

WITH
  /* 1) Build daily aggregates */
  daily AS (
    SELECT
      DATE(`timestamp`) AS dt,
      AVG(CASE WHEN handling_equipment_availability = 0 THEN 1 ELSE 0 END) AS pct_no_equipment,
      AVG(delivery_time_deviation)                           AS avg_delay
    FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
    GROUP BY 1
  ),

  /* 2) Add the next‐day equipment metric */
  lagged AS (
    SELECT
      dt,
      pct_no_equipment,
      LEAD(pct_no_equipment) OVER (ORDER BY dt) AS pct_no_equipment_next,
      avg_delay
    FROM daily
  ),

  /* 3) Compute the global means */
  means AS (
    SELECT
      AVG(pct_no_equipment)       AS mean_x,
      AVG(avg_delay)              AS mean_y,
      AVG(pct_no_equipment_next)  AS mean_x_lag1
    FROM lagged
  ),

  /* 4) Compute covariance & variances using those constants */
  stats AS (
    SELECT
      SUM((l.pct_no_equipment - m.mean_x) * (l.avg_delay - m.mean_y)) AS cov_xy,
      SUM(POW(l.pct_no_equipment - m.mean_x, 2))                      AS var_x,
      SUM(POW(l.avg_delay           - m.mean_y, 2))                  AS var_y
    FROM lagged l
    CROSS JOIN means m
  ),

  stats_lag1 AS (
    SELECT
      SUM((l.pct_no_equipment_next - m.mean_x_lag1) * (l.avg_delay - m.mean_y)) AS cov_xlag1_y,
      SUM(POW(l.pct_no_equipment_next - m.mean_x_lag1, 2))                     AS var_xlag1,
      MAX(m.mean_y)                                                            AS mean_y,   /* aggregate wrapper */
      SUM(POW(l.avg_delay - m.mean_y, 2))                                      AS var_y
    FROM lagged l
    CROSS JOIN means m
  )

/* 5) Final select: Pearson’s r for same-day and lag‐1 day */
SELECT
  ROUND(s.cov_xy   / SQRT(s.var_x    * s.var_y),       4) AS corr_same_day,
  ROUND(sl.cov_xlag1_y / SQRT(sl.var_xlag1 * sl.var_y), 4) AS corr_lag1_day
FROM stats s
CROSS JOIN stats_lag1 sl;


/* 23. Automated Data-Quality Checks (Completeness & Constants)
Identify columns with > 10% nulls or constant values:
*/

WITH stats AS (
  SELECT
    COUNT(*)                               AS total_rows,
    SUM(CASE WHEN warehouse_inventory_level IS NULL THEN 1 ELSE 0 END)            AS null_inv,
    COUNT(DISTINCT warehouse_inventory_level)                                    AS uniq_inv,
    SUM(CASE WHEN handling_equipment_availability IS NULL THEN 1 ELSE 0 END)       AS null_equip,
    COUNT(DISTINCT handling_equipment_availability)                               AS uniq_equip,
    -- repeat for any other critical column...
    SUM(CASE WHEN supplier_reliability_score IS NULL THEN 1 ELSE 0 END)           AS null_sup_rel,
    COUNT(DISTINCT supplier_reliability_score)                                    AS uniq_sup_rel
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT
  total_rows,
  null_inv*100/total_rows  AS pct_null_inventory,
  CASE WHEN uniq_inv = 1 THEN 'CONSTANT' ELSE 'VARIED' END  AS inv_variability,
  null_equip*100/total_rows AS pct_null_equipment,
  CASE WHEN uniq_equip = 1 THEN 'CONSTANT' ELSE 'VARIED' END AS equip_variability,
  null_sup_rel*100/total_rows AS pct_null_supplier_rel,
  CASE WHEN uniq_sup_rel = 1 THEN 'CONSTANT' ELSE 'VARIED' END AS sup_rel_variability
FROM stats;


/* 24. Enrich & Split by Key Categories
Demonstrate deriving a risk bucket and time-of-day slot to feed dashboards or models:
*/

WITH enriched AS (
  SELECT
    *,
    CASE 
      WHEN route_risk_level < 3 THEN 'Low'
      WHEN route_risk_level < 6 THEN 'Moderate'
      WHEN route_risk_level < 8 THEN 'High'
      ELSE 'Severe'
    END AS risk_bucket,
    CASE
      WHEN HOUR(timestamp) BETWEEN 0 AND 5  THEN 'Night'
      WHEN HOUR(timestamp) BETWEEN 6 AND 11 THEN 'Morning'
      WHEN HOUR(timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
      ELSE 'Evening'
    END AS time_of_day
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT
  risk_bucket,
  time_of_day,
  ROUND(AVG(delay_probability),4)         AS avg_delay_prob,
  ROUND(AVG(disruption_likelihood_score),4) AS avg_disruption_prob
FROM enriched
GROUP BY 1,2
ORDER BY risk_bucket, time_of_day;


/* 25. Predictive-Modeling Prep: Feature Assembly
Package all model features into a single CTE that your ML pipeline can pull from:
Usage: Point your notebook or ETL to SELECT * FROM feature_set to retrieve a clean, feature-ready table for modeling.
*/

WITH feature_set AS (
  SELECT
    timestamp,
    traffic_congestion_level,
    weather_condition_severity,
    route_risk_level,
    eta_variation_hours,
    fatigue_monitoring_score,
    HOUR(timestamp)          AS hour,
    DAYOFWEEK(timestamp)     AS dayofweek,
    warehouse_inventory_level,
    loading_unloading_time,
    handling_equipment_availability,
    supplier_reliability_score,
    lead_time_days,
    iot_temperature,
    driver_behavior_score,
    disruption_likelihood_score  AS label_disruption,
    delivery_time_deviation      AS label_delay
  FROM logistic_supply_chain.dynamic_supply_chain_logistics_dataset
)
SELECT * FROM feature_set;