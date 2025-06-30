SELECT TOP 10
    m.MachineID,
    CONCAT(FORMAT(AVG(q.Quality_Control_Defect_Rate_Percent), 'N2'), '%') AS TaxaDefeitoMedia,
    COUNT(*) AS TotalRegistros
FROM FactQuality q
JOIN DimMachine m ON q.MachineKey = m.MachineKey
GROUP BY m.MachineID
ORDER BY TaxaDefeitoMedia DESC;