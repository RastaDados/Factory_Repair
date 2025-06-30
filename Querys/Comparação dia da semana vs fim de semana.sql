SELECT
    CASE 
        WHEN DATEPART(WEEKDAY, t.Timestamp) IN (1, 7) THEN 'Fim de Semana'
        ELSE 'Dia de Semana'
    END AS TipoDia,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeMediaProducao,
    CONCAT(FORMAT(AVG(p.Error_Rate_Percent), 'N2'), '%') AS TaxaErroMedia,
    FORMAT(AVG(p.Power_Consumption_kW), 'N2') AS ConsumoMedioEnergia,
    COUNT(*) AS TotalRegistros
FROM FactProduction p
JOIN DimTime t ON p.TimeKey = t.TimeKey
GROUP BY CASE 
        WHEN DATEPART(WEEKDAY, t.Timestamp) IN (1, 7) THEN 'Fim de Semana'
        ELSE 'Dia de Semana'
    END;