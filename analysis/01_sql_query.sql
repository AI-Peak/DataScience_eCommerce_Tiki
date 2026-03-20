--1. Phân tích phân bổ GMV proxy theo category 
WITH gmv_table AS (
    SELECT 
        category,
        SUM(CAST(price AS BIGINT) * quantity_sold) AS gmv_proxy
    FROM products_clean
    WHERE price IS NOT NULL AND quantity_sold IS NOT NULL
    GROUP BY category
)
SELECT 
    category,
    gmv_proxy,
    ROUND(gmv_proxy * 100.0 / SUM(gmv_proxy) OVER (), 2) AS gmv_pct,
    ROUND(
        SUM(gmv_proxy) OVER (ORDER BY gmv_proxy DESC)
        * 100.0 / SUM(gmv_proxy) OVER (), 2
    ) AS cumulative_gmv_pct
FROM gmv_table
ORDER BY gmv_proxy DESC;

--2. So sánh mức giảm giá trung bình theo category
SELECT 
    category,
    COUNT(*) product_count,
    ROUND(AVG(discount_rate),4) avg_discount_rate
FROM products_clean
GROUP BY category
HAVING COUNT(*) >= 30
ORDER BY avg_discount_rate DESC;

--3. Phân tích sự khác biệt giữa Best-seller (top 10%) và Non Best-seller
WITH ranked AS (
    SELECT *,
        CASE
            WHEN quantity_sold >= (
                SELECT MIN(quantity_sold)
                FROM (
                    SELECT TOP 10 PERCENT quantity_sold
                    FROM products_clean
                    ORDER BY quantity_sold DESC
                ) AS top_products
            )
            THEN 1
            ELSE 2
        END AS decile
    FROM products_clean
)
SELECT 
    CASE 
        WHEN decile = 1 THEN 'Best Seller'
        ELSE 'Non Best Seller'
    END seller_group,
    COUNT(*) product_count,
    CAST(AVG(CAST(price AS DECIMAL(18,2)))AS DECIMAL(18,2)) AS avg_price,
    ROUND(AVG(discount_rate),4) avg_discount,
    ROUND(AVG(rating_average),2) avg_rating,
    ROUND(AVG(reviews_count),2) avg_reviews
FROM ranked
GROUP BY 
    CASE 
        WHEN decile = 1 THEN 'Best Seller'
        ELSE 'Non Best Seller'
    END; 

--4. Phân nhóm theo mức rating
WITH rating_group AS (
    SELECT *,
        CASE 
            WHEN rating_average >= 4.5 THEN 'Excellent (>= 4.5)'
            WHEN rating_average >= 4 THEN 'Good (4 - 4.49)'
            WHEN rating_average >= 3 THEN 'Average (3 - 3.99)'
            ELSE 'Low (< 3)'
        END rating_bin
    FROM products_clean
)
SELECT 
    rating_bin,
    COUNT(*) product_count,
    ROUND(AVG(quantity_sold),0) avg_quantity_sold,
    ROUND(AVG(reviews_count),0) avg_reviews,
    ROUND(AVG(price),2) avg_price
FROM rating_group
GROUP BY rating_bin
ORDER BY avg_quantity_sold DESC;

