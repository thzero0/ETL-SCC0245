-- Qual é a quantidade meteoros avistados em observações de acordo com a precipitação total na hora anterior. (drill-across)
SELECT weather.weather_precipitation as precipitation,
  SUM(observations.obs_cnt_meteor) as meteors_counted
FROM 
  (SELECT 
      fact_observations.fk_start_date as obs_date,
      dim_time.hour as obs_hour,
      dim_time.minute as obs_minute,
      dim_time.second as obs_second,
      dim_local.latitude as obs_latitude,
      dim_local.longitude as obs_longitude,
      fact_observations.id_session as obs_id_session,
      fact_observations.cnt_meteor as obs_cnt_meteor
    FROM imo-weather-scc0245.data_warehouse.fact_observations as fact_observations
      INNER JOIN imo-weather-scc0245.data_warehouse.dim_time as dim_time
      ON fact_observations.fk_start_time = dim_time.sk_time
      INNER JOIN imo-weather-scc0245.data_warehouse.dim_local as dim_local
      ON fact_observations.fk_local = dim_local.sk_local
  ) as observations -- end of query 1
  JOIN
  (SELECT
      fact_weather.fk_date as weather_date,
      dim_time_weather.hour as weather_hour,
      dim_local_weather.latitude as weather_latitude,
      dim_local_weather.longitude as weather_longitude,
      fact_weather.precipitation as weather_precipitation
    FROM imo-weather-scc0245.data_warehouse.fact_weather as fact_weather
      INNER JOIN imo-weather-scc0245.data_warehouse.mv_dim_time_weather as dim_time_weather
      ON fact_weather.fk_time = dim_time_weather.sk_time
      INNER JOIN imo-weather-scc0245.data_warehouse.mv_dim_local_weather as dim_local_weather
      ON fact_weather.fk_local = dim_local_weather.sk_local
    WHERE
      fact_weather.precipitation IS NOT NULL
  ) as weather -- end of query 2
  ON 
    observations.obs_date = weather.weather_date AND
    observations.obs_hour = weather.weather_hour AND
    observations.obs_latitude = weather.weather_latitude AND 
    observations.obs_longitude = weather.weather_longitude 

GROUP BY weather.weather_precipitation
ORDER BY weather.weather_precipitation ASC


