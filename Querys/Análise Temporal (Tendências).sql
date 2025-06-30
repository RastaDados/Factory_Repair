SELECT
    CAST(t.Timestamp AS DATE) AS Data,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeMediaProducao,
    CONCAT(FORMAT(AVG(p.Error_Rate_Percent), 'N2'), '%') AS TaxaErroMedia,
    CONCAT(FORMAT(AVG(q.Quality_Control_Defect_Rate_Percent), 'N2'), '%') AS TaxaDefeitoMedia,
    COUNT(*) AS TotalRegistros
FROM FactProduction p
JOIN FactQuality q ON p.MachineKey = q.MachineKey AND p.TimeKey = q.TimeKey
JOIN DimTime t ON p.TimeKey = t.TimeKey
GROUP BY CAST(t.Timestamp AS DATE)
ORDER BY Data;