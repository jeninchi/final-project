-- Final Exam: SQL-- Our analysis is based on two datasets: Taxi and Weather. The Taxi dataset was initially obtained via an API, processed in Python, and underwent exploratory data analysis (EDA) and cleaning before being imported into Snowflake. A key transformation applied was rounding timestamps to the nearest hour, which enables accurate joins with the weather data. The Weather dataset was sourced from Open-Meteo, providing historical weather conditions such as temperature and precipitation. This structured approach allows us to analyze how weather impacts taxi rides, fares, and tipping behavior.
-- Inspection
-- Here, we are looking at the first 10 rows of each table. 
SELECT * FROM TAXI.PUBLIC.TAXI LIMIT 10; 
SELECT * FROM TAXI.PUBLIC.WEATHER LIMIT 10; -- Query 1: Counting total rides per hour by date
SELECT ROUNDED_DATETIME AS hour, COUNT(*) AS total_rides
FROM TAXI.PUBLIC.TAXI
GROUP BY 1 -- 1 refers to the first column in the SELECT statement. In this case, ROUNDED_DATETIME
ORDER BY 1;
-- This query gives an overview of ride volume per hour, helping us identify daily demand trends. 

-- Query 2: Join 1 - Weather variables and total rides by hour by date
SELECT t.ROUNDED_DATETIME, w.TEMPERATURE_2MF, w.PRECIPITATION, w.RAIN, COUNT(*) AS total_rides
FROM TAXI.PUBLIC.TAXI as t
LEFT JOIN TAXI.PUBLIC.WEATHER as w
ON t.ROUNDED_DATETIME = w.TIME
GROUP BY t.ROUNDED_DATETIME, w.TEMPERATURE_2MF, w.PRECIPITATION, w.RAIN
ORDER BY 1;
-- This join merges weather data with taxi ride counts, allowing us to see how weather conditions impact ride volume. 

-- Query 3: Join 2 - What is the average fare & distance per hour when there's precipitation vs. no precipitation by hour of the day?
SELECT 
    EXTRACT(HOUR FROM t.ROUNDED_DATETIME) AS hour_of_day,
    CASE 
        WHEN w.PRECIPITATION > 0 THEN 'YES PRECIPITATION'
        ELSE 'NO PRECIPITATION'
    END AS rain_status,
    AVG(t.TOTAL_AMOUNT) AS avg_fare,
    AVG(t.TRIP_DISTANCE) AS avg_distance
FROM TAXI.PUBLIC.TAXI t
JOIN TAXI.PUBLIC.WEATHER w 
ON t.ROUNDED_DATETIME = w.TIME
GROUP BY 1, 2
ORDER BY hour_of_day, rain_status DESC;
-- This query examines how precipitation affects trip fares and distances, helping to assess how weather influences taxi usage.
-- We notice from the output that not all hours include information with precipitation/no precipitation. In addition, we see no clear trend on whether precipitation affects average fare or distance.

-- Query 4: Join 3 - Tip percentage by ride length and weather condition
SELECT 
    CASE 
        WHEN w.PRECIPITATION > 0 THEN 'Rainy/Snowy'
        ELSE 'Clear Weather'
    END AS weather_condition,
    CASE 
        WHEN t.TRIP_DISTANCE < 2 THEN 'Short Ride (<2 miles)'
        WHEN t.TRIP_DISTANCE BETWEEN 2 AND 5 THEN 'Medium Ride (2-5 miles)'
        ELSE 'Long Ride (>5 miles)'
    END AS ride_length_category,
    AVG(t.TIP_AMOUNT / NULLIF(t.TOTAL_AMOUNT, 0)) * 100 AS avg_tip_percentage
FROM TAXI.PUBLIC.TAXI t
JOIN TAXI.PUBLIC.WEATHER w 
ON t.ROUNDED_DATETIME = w.TIME
GROUP BY 1, 2
ORDER BY avg_tip_percentage DESC;-- This analysis reveals how tipping behavior varies based on ride length and weather conditions. 
-- Tip percentage is higher for short rides with rainy/snowy weather. However, most people generally stick with around 9-10% tips. Finally, we see no information for clear weather and short rides; we assume this is because people choose to walk in that scenario. 

-- Query 5: Window Function 1 - Rank Top Busiest Hours for Taxi Rides
SELECT EXTRACT(HOUR FROM ROUNDED_DATETIME) AS hour_of_day,
       COUNT(*) AS ride_count,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
FROM TAXI.PUBLIC.TAXI
GROUP BY 1
ORDER BY rank;
-- This query ranks hours by ride volume, helping to pinpoint peak demand periods.
-- Evening hours are the most popular (4 pm onwards), with 7 pm being the most popular hour. This pattern makes sense since our data is highly skewed around New Year's Eve, when people are out and about. In parallel, morning hours between 2 am and 7 am are significantly less popular. 

-- Query 6: Window Function 2 - Calculating ride percentage change by hour of the day
SELECT EXTRACT(HOUR FROM ROUNDED_DATETIME) AS hour_of_day,
       COUNT(*) AS ride_count,
       LAG(COUNT(*)) OVER (ORDER BY EXTRACT(HOUR FROM ROUNDED_DATETIME)) AS previous_hour_rides,
       (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY EXTRACT(HOUR FROM ROUNDED_DATETIME))) / 
       NULLIF(LAG(COUNT(*)) OVER (ORDER BY EXTRACT(HOUR FROM ROUNDED_DATETIME)), 0) * 100 AS ride_percentage_change
FROM TAXI.PUBLIC.TAXI
GROUP BY 1
ORDER BY 1;
-- By comparing ride counts hour over hour, this query identifies periods of sudden demand increases or drops.
-- In conjunction with our previous query, we see a 55% increase in ride count between 6 a.m. and 7 a.m. 

-- Query 7: Advanced Aggregation 1 (Group By) - Calculating average fare for each hour of the day
SELECT EXTRACT(HOUR FROM ROUNDED_DATETIME) AS hour_of_day, 
       AVG(TOTAL_AMOUNT) AS avg_fare
FROM TAXI.PUBLIC.TAXI
GROUP BY 1
ORDER BY avg_fare DESC;
-- This query highlights which hours have the highest and lowest average fares, which is helpful for pricing insights.
-- Average fare is highest for less popular times of the day, such as 6 a.m. 

-- Query 8: Advanced Aggregation 2 (Cube) - Analyzing the impact of temperature on average fare  
SELECT 
    EXTRACT(HOUR FROM t.ROUNDED_DATETIME) AS hour_of_day, 
    CASE 
        WHEN w.TEMPERATURE_2MF < 32 THEN 'Below Freezing'
        WHEN w.TEMPERATURE_2MF BETWEEN 32 AND 50 THEN 'Cold'
        WHEN w.TEMPERATURE_2MF BETWEEN 50 AND 70 THEN 'Mild'
        ELSE 'Warm'
    END AS temperature_category,
    AVG(t.TOTAL_AMOUNT) AS avg_fare
FROM TAXI.PUBLIC.TAXI t
JOIN TAXI.PUBLIC.WEATHER w 
ON t.ROUNDED_DATETIME = w.TIME
GROUP BY CUBE(1,2)
ORDER BY avg_fare DESC NULLS LAST;-- Using CUBE, this query generates subtotal aggregations of average fare across different temperature categories and hours.
-- In addition to time of the day, temperature adds another factor that may affect average fare. For instance, we see that our dataset's highest average fare is 6 am at below-freezing temperatures. -- Query 9: Subquery 1 - Finding the most expensive fare per mile by hour of day
SELECT hour_of_day, avg_trip_distance, avg_fare_per_mile, avg_total_amount
FROM (
    SELECT EXTRACT(HOUR FROM ROUNDED_DATETIME) AS hour_of_day,
           AVG(TRIP_DISTANCE) AS avg_trip_distance,
           AVG(TOTAL_AMOUNT / NULLIF(TRIP_DISTANCE, 0)) AS avg_fare_per_mile,
           AVG(TOTAL_AMOUNT) AS avg_total_amount
    FROM TAXI.PUBLIC.TAXI
    GROUP BY 1
)
ORDER BY avg_fare_per_mile ASC, avg_trip_distance DESC
LIMIT 10;
-- This query helps identify hours with the highest fares per mile, which may indicate premium pricing trends.
-- In addition to being the most expensive average fare, 6 a.m. is also the most expensive fare per mile. For instance, it seems people are not concerned about expensive taxi fares at 6 a.m., and even less so if it's below freezing. 

-- Query 10: Subquery 2 - Finding the cheapest fare per mile by hour of day
SELECT hour_of_day, avg_fare_per_mile, avg_trip_distance, avg_total_amount
FROM (
    SELECT EXTRACT(HOUR FROM ROUNDED_DATETIME) AS hour_of_day,
           AVG(TOTAL_AMOUNT / NULLIF(TRIP_DISTANCE, 0)) AS avg_fare_per_mile,
           AVG(TRIP_DISTANCE) AS avg_trip_distance,
           AVG(TOTAL_AMOUNT) AS avg_total_amount
    FROM TAXI.PUBLIC.TAXI
    GROUP BY 1
)
ORDER BY avg_fare_per_mile DESC
LIMIT 10;
-- This analysis identifies the hours passengers get the best price per mile, offering insight into fare efficiency.
-- We notice that at 7 p.m. fares are the lowest per mile. As we saw previously, 7 p.m. is the most popular time. Demand and supply seem to be better matched for this hour, leading to lower prices.
