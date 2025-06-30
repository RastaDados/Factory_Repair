WITH UltimasLeituras AS (
    SELECT
        p.MachineKey,
        p.Operation_Mode,
        p.Temperature_C,
        p.Vibration_Hz,
        p.Production_Speed_units_per_hr,
        p.Error_Rate_Percent,
        p.Efficiency_Status,
        t.Timestamp,
        ROW_NUMBER() OVER (PARTITION BY p.MachineKey ORDER BY t.Timestamp DESC) AS RN
    FROM FactProduction p
    JOIN DimTime t ON p.TimeKey = t.TimeKey
)
SELECT
    m.MachineID,
    ul.Operation_Mode AS StatusAtual,
    ul.Temperature_C AS TempAtual,
    ul.Vibration_Hz AS VibracaoAtual,
    ul.Production_Speed_units_per_hr AS VelocidadeProducaoAtual,
    CONCAT(FORMAT(ul.Error_Rate_Percent, 'N2'), '%') AS TaxaErroAtual,
    ul.Efficiency_Status AS StatusEficiencia,
    ul.Timestamp AS UltimaLeitura
FROM UltimasLeituras ul
JOIN DimMachine m ON ul.MachineKey = m.MachineKey
WHERE ul.RN = 1
ORDER BY m.MachineID;