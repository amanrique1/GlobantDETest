WITH mean_2021 AS (
    SELECT AVG(emp_count) AS mean_count
    FROM (
        SELECT COUNT(e.id) AS emp_count
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.id
        WHERE EXTRACT(YEAR FROM e.datetime) = 2021
        GROUP BY d.department
    ) dc
)
SELECT
    department,
    COUNT(1) AS hired
FROM employees e
INNER JOIN departments d ON e.department_id = d.id
WHERE EXTRACT(YEAR FROM datetime) = 2021
GROUP BY department
HAVING COUNT(1) > (SELECT mean_count FROM mean_2021)
ORDER BY HIRED DESC