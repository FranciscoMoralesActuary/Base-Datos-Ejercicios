use insurance;
# El día de la semana que más fraudes hay
SELECT 
    DayOfWeekClaimed, COUNT(*) AS reclamaciones
FROM
    fraud
WHERE
    FraudFound_P = 1
GROUP BY DayOfWeekClaimed
ORDER BY reclamaciones DESC
;
# La comparación entre la edad promedio de las mujeres casadas que hacen fraude vs
# las que no hacen fraude
SELECT 
    CASE
        WHEN FraudFound_P = 1 THEN 'Sí'
        ELSE 'No'
    END AS Fraude,
    AVG(age) AS edadPromedio
FROM
    fraud
WHERE
    Sex = 'Female'
        AND MaritalStatus = 'Married'
GROUP BY CASE
    WHEN FraudFound_P = 1 THEN 'Sí'
    ELSE 'No'
END;
# ¿Cuál es la marca que más y menos fraudes tiene para hombres y para mujeres?
SELECT 
    Make,
    SUM(CASE
        WHEN Sex = 'Male' THEN 1
        ELSE 0
    END) AS fraudes_hombre,
    SUM(CASE
        WHEN Sex = 'Female' THEN 1
        ELSE 0
    END) AS fraudes_mujeres
FROM
    fraud
WHERE
    FraudFound_P = 1
GROUP BY make
ORDER BY fraudes_mujeres DESC

