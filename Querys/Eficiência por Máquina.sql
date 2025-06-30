SELECT 
    m.MachineID,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeM�diaProdu��o,
    CONCAT(FORMAT(AVG(p.Error_Rate_Percent), 'N2'), '%') AS TaxaErroM�dia,
    p.Efficiency_Status AS StatusEfici�ncia,
    COUNT(*) AS ContagemLeituras
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
GROUP BY m.MachineID, p.Efficiency_Status
ORDER BY VelocidadeM�diaProdu��o DESC;