/*Результат я побудувала таким чином, що транзація яка відповідає за повернення не виступає окремим рядком в ланцюжку,оскільки нам не так важливий сам факт повернення, як саме дохід який ми отримали від конкретної транзакції */
--Формуємо запит з основною інформацією про всі транзакції користувачів
WITH main AS (
  SELECT
    uuid,
    product_id,
    transaction_id,
    PARSE_TIMESTAMP('%d.%m.%Y %H:%M:%S', event_timestamp) AS event_timestamp,
    CAST(REPLACE(revenue_usd, ',', '.') AS FLOAT64) AS revenue_usd
  FROM pet-projects-472515.my_dataset.subscribtions
  WHERE refunded_transaction_id IS NULL
  --Тут окремо виділяємо інформацію про повернення
),refunded AS(
  SELECT
  refunded_transaction_id,
  CAST(REPLACE(revenue_usd, ',', '.') AS FLOAT64) AS refunded_revenue
  FROM pet-projects-472515.my_dataset.subscribtions
  WHERE refunded_transaction_id IS NOT NULL
  --Джоінимо наші запити для того, щоб в подальшому мати змогу відняти суму яку було повернуто. Оглянувши датасет, можна побачити що сума повернення не завжди відповідає сумі транзакції, тому використовуємо саме віднімання а не просто заміняємо на 0. Також на одну транзакцію може припадати тільки одне повернення, тому потреби сумувати суму повернення нема.
), base AS (
  SELECT uuid, product_id, transaction_id, event_timestamp, revenue_usd, refunded_revenue
  FROM main
  LEFT JOIN refunded
   ON  transaction_id=refunded_transaction_id
--Фінальний запит, вибираємо потрібні стовпці, обчислюємо дохід, обов'язково використовуємо віконні функції щоб рядки не пропадали
), final AS (
  SELECT uuid,
  product_id,
  transaction_id,
  FIRST_VALUE(transaction_id) OVER (PARTITION BY uuid, product_id ORDER BY event_timestamp) AS original_transaction_id,
  CASE
    WHEN refunded_revenue IS NOT NULL
    THEN revenue_usd+refunded_revenue ELSE revenue_usd
    END AS revenue_usd,
  ROW_NUMBER() over(partition by uuid, product_id order by event_timestamp)
  FROM base
)
SELECT * FROM final
--В результаті маємо 35803 рядки. Тобто, в загальному в таблиці ми маємо 36005 рядків даних, віднімаємо поверненя -202=35803. Всі рядки відображено, дані не втрачено.
