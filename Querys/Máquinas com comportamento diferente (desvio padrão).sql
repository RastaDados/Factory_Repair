WITH DesviosPorMaquina AS (
    SELECT
        m.MachineID,
        STDEV(p.Production_Speed_units_per_hr) AS DesvioPadraoProducao,
        STDEV(p.Temperature_C) AS DesvioPadraoTemperatura,
        STDEV(p.Vibration_Hz) AS DesvioPadraoVibracao
    FROM FactProduction p
    JOIN DimMachine m ON p.MachineKey = m.MachineKey
    WHERE p.Operation_Mode = 'Active'
    GROUP BY m.MachineID
),

MediasDesvios AS (
    SELECT
        AVG(DesvioPadraoProducao) AS MediaDesvioProducao,
        AVG(DesvioPadraoTemperatura) AS MediaDesvioTemperatura,
        AVG(DesvioPadraoVibracao) AS MediaDesvioVibracao
    FROM DesviosPorMaquina
)

SELECT
    d.MachineID,
    FORMAT(d.DesvioPadraoProducao, 'N2') AS DesvioPadraoProducao,
    FORMAT(d.DesvioPadraoTemperatura, 'N2') AS DesvioPadraoTemperatura,
    FORMAT(d.DesvioPadraoVibracao, 'N2') AS DesvioPadraoVibracao,
    FORMAT(m.MediaDesvioProducao, 'N2') AS MediaDesvioProducao,
    FORMAT(m.MediaDesvioTemperatura, 'N2') AS MediaDesvioTemperatura,
    FORMAT(m.MediaDesvioVibracao, 'N2') AS MediaDesvioVibracao
FROM DesviosPorMaquina d
CROSS JOIN MediasDesvios m
WHERE d.DesvioPadraoProducao > m.MediaDesvioProducao
   OR d.DesvioPadraoTemperatura > m.MediaDesvioTemperatura
   OR d.DesvioPadraoVibracao > m.MediaDesvioVibracao
ORDER BY d.DesvioPadraoProducao DESC;