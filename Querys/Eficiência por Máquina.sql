SELECT 
    m.MachineID,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeMédiaProdução,
    CONCAT(FORMAT(AVG(p.Error_Rate_Percent), 'N2'), '%') AS TaxaErroMédia,
    p.Efficiency_Status AS StatusEficiência,
    COUNT(*) AS ContagemLeituras
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
GROUP BY m.MachineID, p.Efficiency_Status
ORDER BY VelocidadeMédiaProdução DESC;