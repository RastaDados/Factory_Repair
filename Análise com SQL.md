### Conectando com o MongoDB (Staging) e Carregando os Dados

```python
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

#Conectando no MongoDB - Sataging
client = MongoClient('mongodb://localhost:27017/')
db = client['IndustrialDataLake']
collection = db['MachineSensorData']

#Carregando os dados
df = pd.read_csv('dados/manufacturing_6G_dataset.csv')

#Criando um dicionário e convertendo nesse formato para inserir os dados no MongoDB
data = df.to_dict('records')
collection.insert_many(data)

print(f"Dados inseridos com sucesso! Total: {collection.count_documents({})} registros.")
```

<hr>

### Criação das Colunas do DW - SQL Server

```python
import pyodbc
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

#Configurando a conexão
SQL_SERVER = 'DESKTOP-U623P07'  
SQL_DATABASE = 'FactoryRepair'


#Realizando a conexão com o SQL Server
def create_sql_connection():
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};trusted_connection=yes;'
    return pyodbc.connect(conn_str)

#Criando a função para criar as tabelas no - DW SQL Server
def create_dw_tables():
    try:
        conn = create_sql_connection()
        cursor = conn.cursor()
        
        #Criando a tabela DimMachine 
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimMachine')
        CREATE TABLE DimMachine (
            MachineKey INT IDENTITY(1,1) PRIMARY KEY,
            MachineID INT NOT NULL,
            LoadDate DATETIME DEFAULT GETDATE(),
            CONSTRAINT UQ_MachineID UNIQUE (MachineID)
        );
        """)
        
        #Criando a tabela DimTime 
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimTime')
        CREATE TABLE DimTime (
            TimeKey INT IDENTITY(1,1) PRIMARY KEY,
            Timestamp DATETIME NOT NULL,
            Hour INT,
            Minute INT,
            LoadDate DATETIME DEFAULT GETDATE(),
            CONSTRAINT UQ_Timestamp UNIQUE (Timestamp)
        );
        """)
        
        #Criando a tabela FactProduction 
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactProduction')
        CREATE TABLE FactProduction (
            ProductionKey INT IDENTITY(1,1) PRIMARY KEY,
            MachineKey INT NOT NULL FOREIGN KEY REFERENCES DimMachine(MachineKey),
            TimeKey INT NOT NULL FOREIGN KEY REFERENCES DimTime(TimeKey),
            Temperature_C FLOAT,
            Vibration_Hz FLOAT,
            Power_Consumption_kW FLOAT,
            Production_Speed_units_per_hr FLOAT,
            Error_Rate_Percent FLOAT,
            Efficiency_Status VARCHAR(10),
            Operation_Mode VARCHAR(20),
            LoadDate DATETIME DEFAULT GETDATE(),
            CONSTRAINT UQ_MachineTime UNIQUE (MachineKey, TimeKey)
        );
        """)
        
        #Criando a tabela FactQuality 
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactQuality')
        CREATE TABLE FactQuality (
            QualityKey INT IDENTITY(1,1) PRIMARY KEY,
            MachineKey INT NOT NULL FOREIGN KEY REFERENCES DimMachine(MachineKey),
            TimeKey INT NOT NULL FOREIGN KEY REFERENCES DimTime(TimeKey),
            Quality_Control_Defect_Rate_Percent FLOAT,
            Network_Latency_ms FLOAT,
            Packet_Loss_Percent FLOAT,
            Predictive_Maintenance_Score FLOAT,
            LoadDate DATETIME DEFAULT GETDATE(),
            CONSTRAINT UQ_MachineTimeQuality UNIQUE (MachineKey, TimeKey)
        );
        """)
        
        conn.commit()
        print("Tabelas criadas com sucesso no Data Warehouse!")
        
    except Exception as e:
        print(f"Erro ao criar tabelas: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

#Executando a função de criação das tabelas acima
create_dw_tables()
```

<hr>

### Pipeline de ETL

```python
import pyodbc
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import time

#Conectando novamente ao SQL Server
SQL_SERVER = 'DESKTOP-U623P07'  
SQL_DATABASE = 'FactoryRepair'

#Criando uma função para criar a conexão com tratamento de erros
def create_sql_connection(max_retries=3, retry_delay=5):
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};trusted_connection=yes;'
    
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(conn_str, timeout=30)
            print("Conexão com SQL Server estabelecida com sucesso!")
            return conn
        except pyodbc.Error as e:
            print(f"Tentativa {attempt + 1} de {max_retries} - Erro ao conectar ao SQL Server: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Aguardando {retry_delay} segundos antes de tentar novamente...")
                time.sleep(retry_delay)
    
    raise Exception("Não foi possível estabelecer conexão com o SQL Server após várias tentativas")

def load_to_sql_dw(df):
    conn = None
    cursor = None
    
    try:
        #Estabelecendo a conexão
        conn = create_sql_connection()
        cursor = conn.cursor()
        
        print("Iniciando processo de carga no Data Warehouse...")
        
        #Convertendo os tipos de dados do DataFrame
        print("Convertendo tipos de dados...")
        df['Machine_ID'] = df['Machine_ID'].astype(int)  # Converter para int nativo do Python
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        #Carregando a dimensão Máquina 
        print("Carregando dimensão Máquina...")
        machines = df[['Machine_ID']].drop_duplicates()
        machine_keys = {}
        
        for _, row in machines.iterrows():
            try:
                machine_id = int(row['Machine_ID'])  
                
                #Verificando se a máquina já existe
                cursor.execute("SELECT MachineKey FROM DimMachine WHERE MachineID = ?", machine_id)
                result = cursor.fetchone()
                
                if result:
                    machine_keys[machine_id] = result[0]
                else:
                    #Inserindo a nova máquina
                    cursor.execute("""
                    INSERT INTO DimMachine (MachineID) OUTPUT INSERTED.MachineKey VALUES (?)
                    """, machine_id)
                    machine_keys[machine_id] = cursor.fetchone()[0]
                    
            except Exception as e:
                print(f"Erro ao processar máquina ID {row['Machine_ID']}: {str(e)}")
                continue
        
        conn.commit()
        print(f"Dimensão Máquina carregada. Total: {len(machine_keys)} máquinas.")
        
        #Carregando a dimensão Tempo
        print("Carregando dimensão Tempo...")
        times = df[['Timestamp']].drop_duplicates()
        time_keys = {}
        
        for _, row in times.iterrows():
            try:
                timestamp = row['Timestamp'].to_pydatetime()  #Convertendo para datetime 
                hour = timestamp.hour
                minute = timestamp.minute
                
                #Verificando se o timestamp já existe
                cursor.execute("SELECT TimeKey FROM DimTime WHERE Timestamp = ?", timestamp)
                result = cursor.fetchone()
                
                if result:
                    time_keys[timestamp] = result[0]
                else:
                    #Inserindo o novo registro de tempo
                    cursor.execute("""
                    INSERT INTO DimTime (Timestamp, Hour, Minute) 
                    OUTPUT INSERTED.TimeKey 
                    VALUES (?, ?, ?)
                    """, timestamp, hour, minute)
                    time_keys[timestamp] = cursor.fetchone()[0]
                    
            except Exception as e:
                print(f"Erro ao processar timestamp {row['Timestamp']}: {str(e)}")
                continue
        
        conn.commit()
        print(f"Dimensão Tempo carregada. Total: {len(time_keys)} registros temporais.")
        
        #Carregando a fato Produção em lotes 
        print("Carregando fato Produção...")
        batch_size = 1000
        total_rows = len(df)
        inserted_rows = 0
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_values = []
            
            for _, row in batch.iterrows():
                try:
                    machine_id = int(row['Machine_ID'])
                    timestamp = row['Timestamp'].to_pydatetime()
                    
                    machine_key = machine_keys.get(machine_id)
                    time_key = time_keys.get(timestamp)
                    
                    if machine_key is None or time_key is None:
                        continue
                    
                    #Convertendo todos os valores 
                    batch_values.append((
                        int(machine_key),
                        int(time_key),
                        float(row['Temperature_C']),
                        float(row['Vibration_Hz']),
                        float(row['Power_Consumption_kW']),
                        float(row['Production_Speed_units_per_hr']),
                        float(row['Error_Rate_%']),
                        str(row['Efficiency_Status']),
                        str(row['Operation_Mode'])
                    ))
                except Exception as e:
                    print(f"Erro ao preparar linha {i}: {str(e)}")
                    continue
            
            if batch_values:
                try:
                    cursor.executemany("""
                    INSERT INTO FactProduction (
                        MachineKey, TimeKey, Temperature_C, Vibration_Hz, 
                        Power_Consumption_kW, Production_Speed_units_per_hr, 
                        Error_Rate_Percent, Efficiency_Status, Operation_Mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch_values)
                    inserted_rows += len(batch_values)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f"Erro ao inserir lote {i//batch_size}: {str(e)}")
        
        print(f"Fato Produção carregado. Total: {inserted_rows} registros inseridos.")
        
        #Carregando a fato Qualidade em lotes 
        print("Carregando fato Qualidade...")
        inserted_rows = 0
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_values = []
            
            for _, row in batch.iterrows():
                try:
                    machine_id = int(row['Machine_ID'])
                    timestamp = row['Timestamp'].to_pydatetime()
                    
                    machine_key = machine_keys.get(machine_id)
                    time_key = time_keys.get(timestamp)
                    
                    if machine_key is None or time_key is None:
                        continue
                    
                    #Convertendo todos os valores
                    batch_values.append((
                        int(machine_key),
                        int(time_key),
                        float(row['Quality_Control_Defect_Rate_%']),
                        float(row['Network_Latency_ms']),
                        float(row['Packet_Loss_%']),
                        float(row['Predictive_Maintenance_Score'])
                    ))
                except Exception as e:
                    print(f"Erro ao preparar linha {i}: {str(e)}")
                    continue
            
            if batch_values:
                try:
                    cursor.executemany("""
                    INSERT INTO FactQuality (
                        MachineKey, TimeKey, Quality_Control_Defect_Rate_Percent,
                        Network_Latency_ms, Packet_Loss_Percent, Predictive_Maintenance_Score
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """, batch_values)
                    inserted_rows += len(batch_values)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f"Erro ao inserir lote {i//batch_size}: {str(e)}")
        
        print(f"Fato Qualidade carregado. Total: {inserted_rows} registros inseridos.")
        
        print("Carga no Data Warehouse concluída com sucesso!")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erro fatal durante a carga: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def run_etl_pipeline():
    print("\n=== Iniciando pipeline ETL melhorado ===")
    
    try:
        #Extração dos dados
        print("\n[ETAPA 1] Extração dos dados do MongoDB")
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['IndustrialDataLake']
        collection = db['MachineSensorData']
        
        #Testando a conexão com o MongoDB
        client.server_info()
        
        #Extraindo do MongoDB
        print("Executando query no MongoDB...")
        data = list(collection.find({}, {'_id': 0}))
        
        if not data:
            raise Exception("Nenhum dado encontrado no MongoDB")
        
        df = pd.DataFrame(data)
        print(f"Extraídos {len(df)} registros do MongoDB")
        
        #Transformando os Dados
        print("\n[ETAPA 2] Transformação dos dados")
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df['Hour'] = df['Timestamp'].dt.hour
        df['Minute'] = df['Timestamp'].dt.minute
        
        #Verificando os dados faltantes
        print("Verificando dados faltantes...")
        missing_data = df.isnull().sum()
        if missing_data.any():
            print("Aviso: Dados faltantes encontrados:")
            print(missing_data[missing_data > 0])
            # Preencher ou remover valores nulos conforme necessário
            df.fillna(0, inplace=True)
        
        #Realizando a carga
        print("\n[ETAPA 3] Carga no SQL Server DW")
        load_to_sql_dw(df)
        
        print("\n=== Pipeline ETL concluído com sucesso! ===")
        
    except Exception as e:
        print(f"\n!!! Falha no pipeline ETL: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

#Executando todo o pipeline de ETL completo
if __name__ == "__main__":
    run_etl_pipeline()
```

<hr>

### Querys - Análises com SQL

#### Eficiência por Máquina

```python
conn = create_sql_connection()
cursor = conn.cursor()

#Executando a consulta
query = '''
SELECT 
    m.MachineID,
    AVG(p.Production_Speed_units_per_hr) AS VelocidadeMediaProducao,
    AVG(p.Error_Rate_Percent) AS TaxaMediaErros,
    p.Efficiency_Status,
    COUNT(*) AS ContagemLeituras
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
GROUP BY m.MachineID, p.Efficiency_Status
ORDER BY VelocidadeMediaProducao DESC;
'''

df = pd.read_sql_query(query, conn)

#Formatando as colunas 
df['VelocidadeMediaProducao'] = df['VelocidadeMediaProducao'].apply(lambda x: f'R${x:,.2f}')
df['TaxaMediaErros'] = df['TaxaMediaErros'].apply(lambda x: f'{x:,.2f}%')

print(df)
```

![image](https://github.com/user-attachments/assets/680a1405-bdae-4271-b152-555355c7db25)

#### OEE (Eficácia Geral do Equipamento)

```python
query = '''
SELECT 
    m.MachineID,
    AVG(p.Production_Speed_units_per_hr) / MAX(p.Production_Speed_units_per_hr) AS Disponibilidade,
    1 - AVG(q.Quality_Control_Defect_Rate_Percent / 100) AS Qualidade,
    (AVG(p.Production_Speed_units_per_hr) / MAX(p.Production_Speed_units_per_hr)) * 
    (1 - AVG(q.Quality_Control_Defect_Rate_Percent / 100)) AS OEE
FROM FactProduction p
JOIN FactQuality q ON p.MachineKey = q.MachineKey AND p.TimeKey = q.TimeKey
JOIN DimMachine m ON p.MachineKey = m.MachineKey
GROUP BY m.MachineID
ORDER BY OEE DESC;
'''

df = pd.read_sql_query(query, conn)

#Formatando as colunas 
df['Disponibilidade'] = df['Disponibilidade'].apply(lambda x: f'{x:,.2f}')
df['Qualidade'] = df['Qualidade'].apply(lambda x: f'{x*100:,.2f}%')
df['OEE'] = df['OEE'].apply(lambda x: f'{x:,.2f}')

print(df)
```

![image](https://github.com/user-attachments/assets/4f6f061c-c705-40e1-a28a-2ee8f26bd274)

#### Análise de Temperatura vs Defeitos

```python
query = '''
WITH TempRanges AS (
    SELECT
        m.MachineID,
        CASE
            WHEN p.Temperature_C < 50 THEN 'Baixa (<50)'
            WHEN p.Temperature_C BETWEEN 50 AND 70 THEN 'Normal (50-70)'
            WHEN p.Temperature_C BETWEEN 70 AND 85 THEN 'Alta (70-85)'
            ELSE 'Muito Alta (>85)'
        END AS FaixaTemperatura,
        q.Quality_Control_Defect_Rate_Percent AS DefectRate
    FROM FactProduction p
    JOIN FactQuality q ON p.MachineKey = q.MachineKey AND p.TimeKey = q.TimeKey
    JOIN DimMachine m ON p.MachineKey = m.MachineKey
    JOIN DimTime t ON p.TimeKey = t.TimeKey
)
SELECT
    FaixaTemperatura,
    AVG(DefectRate) AS TaxaDefeitoMedia,
    COUNT(*) AS TotalRegistros
FROM TempRanges
GROUP BY FaixaTemperatura
ORDER BY TaxaDefeitoMedia DESC;
'''

df = pd.read_sql_query(query, conn)

#Formatando as colunas 
df['TaxaDefeitoMedia'] = df['TaxaDefeitoMedia'].apply(lambda x: f'{x:,.2f}%')

print(df)
```

![image](https://github.com/user-attachments/assets/9579b4cd-6e16-49fc-94e2-23a4f69cf741)

#### Análise de Desempenho por Turno / Horário

```python
#Configurando a conexão no SQL Novamente
SQL_SERVER = 'DESKTOP-U623P07' 
SQL_DATABASE = 'FactoryRepair'

#Função para criar conexão com o tratamento caso der erro de conexão
def create_sql_connection(max_retries=3, retry_delay=5):
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};trusted_connection=yes;'
    
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(conn_str, timeout=30)
            print("Conexão com SQL Server estabelecida com sucesso!")
            return conn
        except pyodbc.Error as e:
            print(f"Tentativa {attempt + 1} de {max_retries} - Erro ao conectar ao SQL Server: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Aguardando {retry_delay} segundos antes de tentar novamente...")
                time.sleep(retry_delay)
    
    raise Exception("Não foi possível estabelecer conexão com o SQL Server após várias tentativas")

def load_to_sql_dw(df):
    conn = None
    cursor = None

conn = create_sql_connection()
cursor = conn.cursor()

query = '''
SELECT 
    DATEPART(HOUR, t.Timestamp) AS Hora,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeMediaProducao,
    CONCAT(FORMAT(AVG(p.Error_Rate_Percent), 'N2'), '%') AS TaxaErroMedia,
    COUNT(*) AS TotalRegistros
FROM FactProduction p
JOIN DimTime t ON p.TimeKey = t.TimeKey
GROUP BY DATEPART(HOUR, t.Timestamp)
ORDER BY Hora;
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/6e7c3bbf-0a43-4114-a403-dbc56dc7a2f2)

#### Comparação Entre Máquinas Ativas e Ociosas

```python
query = '''
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
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/a6893176-b9b0-4dd9-8019-bb9fa1665b81)

#### Máquinas Com Maior Taxa de Defeitos

```python
query = '''
SELECT TOP 10
    m.MachineID,
    CONCAT(FORMAT(AVG(q.Quality_Control_Defect_Rate_Percent), 'N2'), '%') AS TaxaDefeitoMedia,
    COUNT(*) AS TotalRegistros
FROM FactQuality q
JOIN DimMachine m ON q.MachineKey = m.MachineKey
GROUP BY m.MachineID
ORDER BY TaxaDefeitoMedia DESC;
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/dbc4cc1e-75b2-4986-996d-a5eb5ba5381c)

#### Correlação Entre Temperatura e Defeitos

```python
query = '''
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
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/260c2f18-15a9-475a-a100-9b9f8eb744c8)

#### Consumo de Energia Por Modo de Operação

```python
query = '''
SELECT
    Operation_Mode AS ModoOperacao,
    FORMAT(AVG(Power_Consumption_kW), 'N2') AS ConsumoMedioEnergia,
    FORMAT(MIN(Power_Consumption_kW), 'N2') AS ConsumoMinimo,
    FORMAT(MAX(Power_Consumption_kW), 'N2') AS ConsumoMaximo,
    COUNT(*) AS TotalRegistros
FROM FactProduction
GROUP BY Operation_Mode
ORDER BY ConsumoMedioEnergia DESC;
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/0826e54a-cbb8-40ff-83ec-3b148e85cb52)

#### Máquinas Menos Eficientes Energeticamente

```python
query = '''
SELECT TOP 10
    m.MachineID,
    FORMAT(AVG(p.Power_Consumption_kW / p.Production_Speed_units_per_hr), 'N2') AS ConsumoPorUnidade,
    FORMAT(AVG(p.Power_Consumption_kW), 'N2') AS ConsumoMedioEnergia,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeMediaProducao
FROM FactProduction p
JOIN DimMachine m ON p.MachineKey = m.MachineKey
WHERE p.Operation_Mode = 'Active'
GROUP BY m.MachineID
ORDER BY ConsumoPorUnidade DESC;
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/5322fdb5-5f59-48d4-8872-4cbaad00a93e)

#### Identificar Máquinas Com Vibração Anormal  

```python
query = '''
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
'''

df = pd.read_sql_query(query, conn)


print(df)
```

![image](https://github.com/user-attachments/assets/0e85fbd5-b33c-4290-a831-ea126bd93a0e)

#### Padrões Antes de Falhas (Análise de Tendência)

```python
query = '''
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
    WHERE p.Error_Rate_Percent > 5  -- Considerando como "falha" quando ErrorRate > 5%
)
SELECT
    MachineID,
    FORMAT(AVG(TempAnterior), 'N2') AS TemperaturaMediaAntesFalha,
    FORMAT(AVG(VibAnterior), 'N2') AS VibracaoMediaAntesFalha,
    FORMAT(AVG(PowerAnterior), 'N2') AS ConsumoEnergiaMedioAntesFalha,
    COUNT(*) AS TotalFalhasAnalisadas
FROM Falhas
GROUP BY MachineID;
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/7422cb5b-96e4-4266-8ce1-be3b55d10fce)

#### Análise de Eficiência Geral (OEE)

```python
query = '''
SELECT
    m.MachineID,
    -- Disponibilidade (Tempo Ativo / Tempo Total)
    FORMAT(CAST(SUM(CASE WHEN p.Operation_Mode = 'Active' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 'N2') AS Disponibilidade,
    
    -- Desempenho (Produção Real / Produção Teórica Máxima)
    FORMAT(AVG(p.Production_Speed_units_per_hr) / MAX(p.Production_Speed_units_per_hr), 'N2') AS Desempenho,
    
    -- Qualidade (1 - Taxa de Defeitos)
    FORMAT(1 - AVG(q.Quality_Control_Defect_Rate_Percent / 100), 'N2') AS Qualidade,
    
    -- OEE Total (Disponibilidade × Desempenho × Qualidade)
    FORMAT(
        (CAST(SUM(CASE WHEN p.Operation_Mode = 'Active' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) *
        (AVG(p.Production_Speed_units_per_hr) / MAX(p.Production_Speed_units_per_hr)) *
        (1 - AVG(q.Quality_Control_Defect_Rate_Percent / 100)),
        'N2'
    ) AS OEE,
    
    COUNT(*) AS TotalRegistros
FROM FactProduction p
JOIN FactQuality q ON p.MachineKey = q.MachineKey AND p.TimeKey = q.TimeKey
JOIN DimMachine m ON p.MachineKey = m.MachineKey
GROUP BY m.MachineID
ORDER BY OEE DESC;
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/2b4ac10d-a3a9-4522-aff3-360bf8c5deef)

#### Análise Temporal (Tendências)

```python
query = '''
SELECT
    CAST(t.Timestamp AS DATE) AS Data,
    FORMAT(AVG(p.Production_Speed_units_per_hr), 'N2') AS VelocidadeMediaProducao,
    FORMAT(AVG(p.Error_Rate_Percent), 'N2') AS TaxaErroMedia,
    FORMAT(AVG(q.Quality_Control_Defect_Rate_Percent), 'N2') AS TaxaDefeitoMedia,
    COUNT(*) AS TotalRegistros
FROM FactProduction p
JOIN FactQuality q ON p.MachineKey = q.MachineKey AND p.TimeKey = q.TimeKey
JOIN DimTime t ON p.TimeKey = t.TimeKey
GROUP BY CAST(t.Timestamp AS DATE)
ORDER BY Data;
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/9e11067b-1c7b-4198-b4d1-39505421a807)

#### Comparação dia da Semana vs Fim de Semana 

```python
query = '''
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
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/27a5247b-7542-41d8-b25a-e3412ea7f21c)

#### Status Atual das Máquinas (Última Leitura de Cada Uma)

```python
query = '''
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
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/30c08bbb-d0a1-4981-80b6-028b002fe62b)

#### Alertas de Possíveis Problemas

```python
query = '''
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
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/0f1cef08-d588-4411-a25a-6da30c663b12)

#### Ranking de Máquinas por Produtividade

```python
query = '''
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
'''

df = pd.read_sql_query(query, conn)


print(df)
```

![image](https://github.com/user-attachments/assets/7c5c83cf-0cb2-47a3-9d48-3aeca3409caa)

#### Máquinas Com Comportamento Diferente (Desvio Padrão)

```python
query = '''
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
'''

df = pd.read_sql_query(query, conn)

print(df)
```

![image](https://github.com/user-attachments/assets/6f15a965-e271-4e72-816c-b81f3d92b4e2)

