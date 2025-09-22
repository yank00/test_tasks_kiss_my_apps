/*Витягуємо всі дані з таблиці окремо, також надаємо потрібні типи даних де потрібно */
with main as(
  SELECT uuid,
  PARSE_TIMESTAMP('%d.%m.%Y %H:%M:%S', event_timestamp) AS event_timestamp,
  event_name,
  product_id,
  is_trial,
  period,
  trial_period,
  CAST(REPLACE(revenue_usd, ',', '.') AS FLOAT64) AS revenue_usd,
  transaction_id,
  refunded_transaction_id
  FROM pet-projects-472515.my_dataset.subscribtions
  ), final as (
  SELECT uuid,
    --Використала тут ARRAY_AGG оскільки LAST_VALUE не буде працювати з групуванням
    ARRAY_AGG(product_id ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS current_product_id,
    MIN(CASE
      WHEN is_trial=TRUE
      THEN event_timestamp END) AS trial_started_time,
    MIN(CASE
      WHEN event_name="purchase"
      THEN event_timestamp END) AS first_purchase_time,
    MAX(CASE
      WHEN event_name="purchase"
      THEN event_timestamp END) AS last_purchase_time,
    COUNT(CASE
      WHEN event_name="purchase"
      then transaction_id END) AS total_purchases,
    --Тут врахувала всі випадки, оскільки з purchase у нас може бути is_trial = TRUE, не враховую cancellation та refund
    SUM(revenue_usd) AS total_revenue_usd,
      MAX(CASE
        WHEN event_name = 'trial' AND is_trial = TRUE THEN DATETIME_ADD(event_timestamp, INTERVAL trial_period DAY)
        WHEN event_name = 'purchase' AND is_trial = FALSE THEN DATETIME_ADD(event_timestamp, INTERVAL period DAY)
        WHEN event_name = 'purchase' AND is_trial = TRUE THEN DATETIME_ADD(event_timestamp, INTERVAL trial_period DAY)
        ELSE NULL END) AS expiration_time,
    MAX(CASE
      WHEN event_name = "cancellation"
      THEN event_timestamp END) AS cancelation_time,
    MAX (CASE
      WHEN event_name = 'refund'
      then event_timestamp END) AS refund_time
  FROM main
  GROUP BY uuid
  )
  SELECT * FROM final
  -- В googlesheets заздалегідь перевірила к-сть унікальних айдішок користувача - 10483, як і маємо в результаті. Тобто всі дані були відображені.