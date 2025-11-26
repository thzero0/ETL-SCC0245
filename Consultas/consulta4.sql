-- Para cada tipo de tempo, mostrar o número de meteoros contados ao longo do ano de 2022 (drill-across e slice and dice).
WITH joined AS (
  SELECT
    weather_type,
    observations.cnt_meteor AS meteor_count
  FROM
    (
      SELECT 
        fact_observations.fk_local AS fk_local,
        fact_observations.fk_start_date AS fk_date,
        dim_time.hour AS hour,
        fact_observations.cnt_meteor AS cnt_meteor
      FROM imo-weather-scc0245.data_warehouse.fact_observations AS fact_observations
      INNER JOIN imo-weather-scc0245.data_warehouse.dim_time AS dim_time
        ON fact_observations.fk_start_time = dim_time.sk_time
      WHERE fact_observations.fk_start_date BETWEEN 20220101 AND 20221231
    ) AS observations -- fim query 1
    JOIN
    (
      SELECT
        fact_weather.fk_local AS fk_local,
        fact_weather.fk_date AS fk_date,
        dim_time.hour AS hour,
        dim_weather_code.description AS weather_type
      FROM imo-weather-scc0245.data_warehouse.fact_weather AS fact_weather
      INNER JOIN imo-weather-scc0245.data_warehouse.dim_time AS dim_time
        ON fact_weather.fk_time = dim_time.sk_time
      INNER JOIN imo-weather-scc0245.data_warehouse.dim_weather_code AS dim_weather_code
        ON fact_weather.fk_weather_code = dim_weather_code.sk_weather_code
      WHERE fact_weather.fk_date BETWEEN 20220101 AND 20221231
    ) AS weather -- fim query 2

    -- Join drill across
    ON observations.fk_local = weather.fk_local
    AND observations.fk_date = weather.fk_date
    AND observations.hour = weather.hour
)
SELECT *
FROM joined
PIVOT (
  SUM(meteor_count)
  FOR weather_type IN (
    'Clear sky',
    'Overcast',
    'Mainly clear',
    'Drizzle light intensity',
    'Drizzle moderate intensity',
    'Rain slight intensity',
    'Rain moderate intensity'
  )
);
