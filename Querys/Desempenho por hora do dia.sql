SELECT 
    DATEPART(HOUR, t.Timestamp) AS Hora,
    FORMAT(AVG(p.Production_Speed_units_per_hr), '0.00') AS VelocidadeMediaProducao,
    CONCAT(FORMAT(AVG(p.Error_Rate_Percent), '0.00'), '%') AS TaxaErroMedia,
    COUNT(*) AS TotalRegistros
FROM FactProduction p
JOIN DimTime t ON p.TimeKey = t.TimeKey
GROUP BY DATEPART(HOUR, t.Timestamp)
ORDER BY Hora;