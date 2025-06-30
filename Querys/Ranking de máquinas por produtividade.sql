SELECT
    m.MachineID,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeMediaProducao,
    RANK() OVER (ORDER BY AVG(p.Production_Speed_units_per_hr) DESC) AS RankProdutividade,
	CONCAT(FORMAT(AVG(p.Error_Rate_Percent), 'N2'), '%') AS TaxaErroMedia,
    RANK() OVER (ORDER BY AVG(p.Error_Rate_Percent)) AS RankQualidade,
    FORMAT(AVG(p.Power_Consumption_kW / p.Production_Speed_units_per_hr), 'N2') AS EficienciaEnergetica,
    RANK() OVER (ORDER BY AVG(p.Power_Consumption_kW / p.Production_Speed_units_per_hr)) AS RankEficienciaEnergetica
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
WHERE p.Operation_Mode = 'Active'
GROUP BY m.MachineID
ORDER BY RankProdutividade;