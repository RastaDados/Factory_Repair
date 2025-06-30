SELECT TOP 10
    m.MachineID,
    FORMAT(AVG(p.Power_Consumption_kW / p.Production_Speed_units_per_hr), 'N2') AS ConsumoPorUnidade,
    FORMAT(AVG(p.Power_Consumption_kW), 'N2') AS ConsumoMedioEnergia,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeMediaProducao
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
WHERE p.Operation_Mode = 'Active'
GROUP BY m.MachineID
ORDER BY ConsumoPorUnidade DESC;