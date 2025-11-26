-- Quantos meteoros foram observados no mês de Janeiro (Roll-up) que são do tipo de chuva QUA (slice and dice).
SELECT dim_shower.IAU_code as Shower, SUM(fact_observations.cnt_meteor) as meteors_counted
FROM 
    imo-weather-scc0245.data_warehouse.fact_observations as fact_observations 
    INNER JOIN
      imo-weather-scc0245.data_warehouse.dim_shower as dim_shower ON fact_observations.fk_shower = dim_shower.sk_shower

WHERE 
  REGEXP_CONTAINS(CAST(fact_observations.fk_start_date AS string), r'^\d{4}01\d{2}$') AND
  dim_shower.IAU_code = 'QUA'
GROUP BY dim_shower.IAU_code
ORDER BY SUM(fact_observations.cnt_meteor) DESC