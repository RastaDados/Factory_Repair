SELECT
    Operation_Mode AS ModoOperacao,
    FORMAT(AVG(Power_Consumption_kW), 'N2') AS ConsumoMedioEnergia,
    FORMAT(MIN(Power_Consumption_kW), 'N2') AS ConsumoMinimo,
    FORMAT(MAX(Power_Consumption_kW), 'N2') AS ConsumoMaximo,
    COUNT(*) AS TotalRegistros
FROM FactProduction
GROUP BY Operation_Mode
ORDER BY ConsumoMedioEnergia DESC;