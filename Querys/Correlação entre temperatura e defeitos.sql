WITH TempRanges AS (
    SELECT
        m.MachineID,
        CASE
            WHEN p.Temperature_C < 50 THEN 'Baixa (<50)'
            WHEN p.Temperature_C BETWEEN 50 AND 70 THEN 'Normal (50-70)'
            WHEN p.Temperature_C BETWEEN 70 AND 85 THEN 'Alta (70-85)'
            ELSE 'Muito Alta (>85)'
        END AS FaixaTemperatura,
        q.Quality_Control_Defect_Rate_Percent
    FROM FactProduction p
    JOIN FactQuality q ON p.MachineKey = q.MachineKey AND p.TimeKey = q.TimeKey
    JOIN DimMachine m ON p.MachineKey = m.MachineKey
)
SELECT
    FaixaTemperatura,
    CONCAT(FORMAT(AVG(Quality_Control_Defect_Rate_Percent), 'N2'), '%') AS TaxaDefeitoMedia,
    COUNT(*) AS TotalRegistros
FROM TempRanges
GROUP BY FaixaTemperatura
ORDER BY 
    CASE FaixaTemperatura
        WHEN 'Baixa (<50)' THEN 1
        WHEN 'Normal (50-70)' THEN 2
        WHEN 'Alta (70-85)' THEN 3
        ELSE 4
    END;