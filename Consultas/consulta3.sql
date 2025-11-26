-- No ano de 2022 (slice and dice), para cada tipo de chuva de meteoros de contou mais de 50 meteoros (Roll-up), mostrar a magnitude média de avistamentos de meteoros (Pivot).

SELECT
  dim_shower.IAU_code as Meteor_Shower,
  ROUND( 
    SAFE_DIVIDE(
      SUM(
        fact_magnitude.cnt_mag_neg_6 * -6 +
        fact_magnitude.cnt_mag_neg_5 * -5 +
        fact_magnitude.cnt_mag_neg_4 * -4 +
        fact_magnitude.cnt_mag_neg_3 * -3 +
        fact_magnitude.cnt_mag_neg_2 * -2 +
        fact_magnitude.cnt_mag_neg_1 * -1 +
        fact_magnitude.cnt_mag_0    *  0 +
        fact_magnitude.cnt_mag_1    *  1 +
        fact_magnitude.cnt_mag_2    *  2 +
        fact_magnitude.cnt_mag_3    *  3 +
        fact_magnitude.cnt_mag_4    *  4 +
        fact_magnitude.cnt_mag_5    *  5 +
        fact_magnitude.cnt_mag_6    *  6
      ),
      SUM(
        fact_magnitude.cnt_mag_neg_6 +
        fact_magnitude.cnt_mag_neg_5 +
        fact_magnitude.cnt_mag_neg_4 +
        fact_magnitude.cnt_mag_neg_3 +
        fact_magnitude.cnt_mag_neg_2 +
        fact_magnitude.cnt_mag_neg_1 +
        fact_magnitude.cnt_mag_0 +
        fact_magnitude.cnt_mag_1 +
        fact_magnitude.cnt_mag_2 +
        fact_magnitude.cnt_mag_3 +
        fact_magnitude.cnt_mag_4 +
        fact_magnitude.cnt_mag_5 +
        fact_magnitude.cnt_mag_6
      )
  ), 2) as magnitude_mean
FROM imo-weather-scc0245.data_warehouse.fact_magnitude as fact_magnitude
  INNER JOIN imo-weather-scc0245.data_warehouse.dim_shower as dim_shower
    ON fact_magnitude.fk_shower = dim_shower.sk_shower
WHERE 
  REGEXP_CONTAINS(CAST(fact_magnitude.fk_start_date AS string), r'2022\d{4}$')
GROUP BY dim_shower.IAU_code
HAVING 
  SUM(
    fact_magnitude.cnt_mag_neg_6 +
    fact_magnitude.cnt_mag_neg_5 +
    fact_magnitude.cnt_mag_neg_4 +
    fact_magnitude.cnt_mag_neg_3 +
    fact_magnitude.cnt_mag_neg_2 +
    fact_magnitude.cnt_mag_neg_1 +
    fact_magnitude.cnt_mag_0 +
    fact_magnitude.cnt_mag_1 +
    fact_magnitude.cnt_mag_2 +
    fact_magnitude.cnt_mag_3 +
    fact_magnitude.cnt_mag_4 +
    fact_magnitude.cnt_mag_5 +
    fact_magnitude.cnt_mag_6
  ) > 50

ORDER BY
 magnitude_mean DESC;