SELECT
    m.MachineID,

    FORMAT(CAST(SUM(CASE WHEN p.Operation_Mode = 'Active' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 'N2') AS Disponibilidade,
    
    FORMAT(AVG(p.Production_Speed_units_per_hr) / MAX(p.Production_Speed_units_per_hr), 'N2') AS Desempenho,
    
    
    FORMAT(1 - AVG(q.Quality_Control_Defect_Rate_Percent / 100), 'N2') AS Qualidade,
    
  
    FORMAT(
        (CAST(SUM(CASE WHEN p.Operation_Mode = 'Active' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) *
        (AVG(p.Production_Speed_units_per_hr) / MAX(p.Production_Speed_units_per_hr)) *
        (1 - AVG(q.Quality_Control_Defect_Rate_Percent / 100)),
        'N2'
    ) AS OEE,
    
    COUNT(*) AS TotalRegistros
FROM FactProduction p
JOIN FactQuality q ON p.MachineKey = q.MachineKey AND p.TimeKey = q.TimeKey
JOIN DimMachine m ON p.MachineKey = m.MachineKey
GROUP BY m.MachineID
ORDER BY OEE DESC;