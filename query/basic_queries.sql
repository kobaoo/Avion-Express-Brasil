SELECT * FROM customers LIMIT 10;
SELECT * FROM geolocation WHERE geolocation_city LIKE 'brasilia%' ORDER BY geolocation_zip_code_prefix;
SELECT * FROM customers WHERE customer_zip_code_prefix = 39330;
SELECT g.geolocation_state, COUNT(*) 
FROM geolocation g 
INNER JOIN customers c ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix 
GROUP BY g.geolocation_state 
ORDER BY count DESC;