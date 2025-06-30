SELECT 
    CASE 
        WHEN p.Operation_Mode = 'Active' THEN 'Ativa'
        ELSE 'Ociosa/Manutenção'
    END AS StatusOperacao,
    COUNT(*) AS TotalRegistros,
    FORMAT(AVG(p.Temperature_C), 'N2') AS TemperaturaMedia,
    FORMAT(AVG(p.Vibration_Hz), 'N2') AS VibracaoMedia,
    FORMAT(AVG(p.Power_Consumption_kW), 'N2') AS ConsumoEnergiaMedio
FROM FactProduction p
GROUP BY CASE 
        WHEN p.Operation_Mode = 'Active' THEN 'Ativa'
        ELSE 'Ociosa/Manutenção'
    END;