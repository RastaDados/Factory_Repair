SELECT
    m.MachineID,
    'Temperatura Alta' AS TipoAlerta,
    FORMAT(p.Temperature_C, 'N2') AS Valor,
    t.Timestamp
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
JOIN DimTime t ON p.TimeKey = t.TimeKey
WHERE p.Temperature_C > 85  -- Limite de temperatura
UNION ALL
SELECT
    m.MachineID,
    'Vibração Excessiva' AS TipoAlerta,
    FORMAT(p.Vibration_Hz, 'N2') AS Valor,
    t.Timestamp
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
JOIN DimTime t ON p.TimeKey = t.TimeKey
WHERE p.Vibration_Hz > 4.5  -- Limite de vibração
ORDER BY Timestamp DESC;