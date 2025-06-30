WITH Falhas AS (
    SELECT 
        m.MachineID,
        t.Timestamp,
        p.Error_Rate_Percent,
        LAG(p.Temperature_C, 1) OVER (PARTITION BY m.MachineID ORDER BY t.Timestamp) AS TempAnterior,
        LAG(p.Vibration_Hz, 1) OVER (PARTITION BY m.MachineID ORDER BY t.Timestamp) AS VibAnterior,
        LAG(p.Power_Consumption_kW, 1) OVER (PARTITION BY m.MachineID ORDER BY t.Timestamp) AS PowerAnterior
    FROM FactProduction p
    JOIN DimMachine m ON p.MachineKey = m.MachineKey
    JOIN DimTime t ON p.TimeKey = t.TimeKey
    WHERE p.Error_Rate_Percent > 5  
)
SELECT
    MachineID,
    FORMAT(AVG(TempAnterior), 'N2') AS TemperaturaMediaAntesFalha,
    FORMAT(AVG(VibAnterior), 'N2') AS VibracaoMediaAntesFalha,
    FORMAT(AVG(PowerAnterior), 'N2') AS ConsumoEnergiaMedioAntesFalha,
    COUNT(*) AS TotalFalhasAnalisadas
FROM Falhas
GROUP BY MachineID;