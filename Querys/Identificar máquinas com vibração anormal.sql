SELECT
    m.MachineID,
    FORMAT(AVG(p.Vibration_Hz), 'N2') AS VibracaoMedia,
    FORMAT(STDEV(p.Vibration_Hz), 'N2') AS DesvioPadraoVibracao,
    FORMAT(MAX(p.Vibration_Hz), 'N2') AS VibracaoMaxima,
    COUNT(*) AS TotalRegistros
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
WHERE p.Operation_Mode = 'Active'
GROUP BY m.MachineID
HAVING AVG(p.Vibration_Hz) > 3.5 OR STDEV(p.Vibration_Hz) > 1.2
ORDER BY VibracaoMedia DESC;