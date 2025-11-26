-- Qual é a média de temperatura mês a mês (Roll-up) durante os horários entre 6:00 e 8:00 (slice and dice).

SELECT
  dim_date.month_name as month,
  ROUND(AVG(fact_weather.temperature_2m), 2) AS mean_temperature
FROM imo-weather-scc0245.data_warehouse.fact_weather as fact_weather
  INNER JOIN imo-weather-scc0245.data_warehouse.mv_dim_time_weather as dim_weather_time
  ON fact_weather.fk_time = dim_weather_time.sk_time
  INNER JOIN imo-weather-scc0245.data_warehouse.dim_date as dim_date
  ON fact_weather.fk_date = dim_date.pk_date
WHERE hour BETWEEN 6 AND 8 
GROUP BY dim_date.month_name