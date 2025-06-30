SELECT 
    m.MachineID,
    FORMAT(AVG(p.Production_Speed_units_per_hr) / MAX(p.Production_Speed_units_per_hr), '0.00') AS Disponibilidade,
    CONCAT(FORMAT(1 - AVG(q.Quality_Control_Defect_Rate_Percent / 100), '0.00'), '%') AS Qualidade,
    FORMAT((AVG(p.Production_Speed_units_per_hr) / MAX(p.Production_Speed_units_per_hr)) * (1 - AVG(q.Quality_Control_Defect_Rate_Percent / 100)), '0.00') AS OEE
FROM FactProduction p
JOIN FactQuality q ON p.MachineKey = q.MachineKey AND p.TimeKey = q.TimeKey
JOIN DimMachine m ON p.MachineKey = m.MachineKey
GROUP BY m.MachineID
ORDER BY OEE DESC;