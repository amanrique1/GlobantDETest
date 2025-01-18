SELECT
    department,
    job,
    SUM(CASE WHEN quarter = 'Q1' THEN count ELSE 0 END) AS Q1,
    SUM(CASE WHEN quarter = 'Q2' THEN count ELSE 0 END) AS Q2,
    SUM(CASE WHEN quarter = 'Q3' THEN count ELSE 0 END) AS Q3,
    SUM(CASE WHEN quarter = 'Q4' THEN count ELSE 0 END) AS Q4
FROM (
    SELECT
        department,
        job,
        quarter,
        COUNT(CASE WHEN EXTRACT(YEAR FROM datetime) = 2021 THEN 1 ELSE 0 END) AS count -- Only count those from 2021
    FROM (
        SELECT
            id,
            'Q' || CEIL(EXTRACT(MONTH FROM datetime) / 3.0) AS quarter,
            job_id,
            department_id,
            datetime
        FROM employees
    ) emp
    INNER JOIN jobs j ON j.id = emp.job_id
    INNER JOIN departments d ON d.id = emp.department_id
    GROUP BY department, job, quarter
) q
GROUP BY department, job
ORDER BY department, job