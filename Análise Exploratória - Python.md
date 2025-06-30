### Imports e Carregando os Dados

```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import warnings
warnings.filterwarnings('ignore')

#Carregando os dados
df = pd.read_csv('dados/manufacturing_6G_dataset.csv')

#Visualização inicial
print("Primeiras linhas do dataset:")
display(df.head())

print("\nInformações sobre o dataset:")
display(df.info())

print("\nEstatísticas descritivas:")
display(df.describe())

#Verificando se tem valores nulos
print("\nValores nulos por coluna:")
display(df.isnull().sum())

#Verificando se tem dados duplicados
print("\nNúmero de linhas duplicadas:", df.duplicated().sum())

#Análise da eficiência das máquinas
print("\nDistribuição da Eficiência:")
display(df['Efficiency_Status'].value_counts(normalize=True))
```

![image](https://github.com/user-attachments/assets/d13c1fdb-2e5d-4d11-9e76-a39da0414374)

![image](https://github.com/user-attachments/assets/a39019b1-c2ac-4a34-bbe2-6fdf54cf692a)

![image](https://github.com/user-attachments/assets/7d05dd00-d678-4f87-8c90-f16b51decb73)

![image](https://github.com/user-attachments/assets/ecbbfcbb-4838-40a5-a3f8-ef9d66fe215c)

<hr>

### Visualizações Iniciais

```python
#Distribuição do status de eficiência das máquinas
fig = px.pie(df, names='Efficiency_Status', title='Distribuição do Status de Eficiência')
fig.show()

#Distribuição das variáveis numéricas que estão sendo selecionadas
num_vars = ['Temperature_C', 'Vibration_Hz', 'Power_Consumption_kW', 'Quality_Control_Defect_Rate_%', 'Production_Speed_units_per_hr']
for var in num_vars:
    fig = px.histogram(df, x=var, nbins=30, title=f'Distribuição de {var}')
    fig.show()

#Matriz de correlação
corr_matrix = df.select_dtypes(include=['float64', 'int64']).corr()
fig = px.imshow(corr_matrix, text_auto=True, aspect="auto", title='Matriz de Correlação')
fig.show()
```

![image](https://github.com/user-attachments/assets/031d1f61-f570-462b-880f-7b0b15d8316e)

![image](https://github.com/user-attachments/assets/390cdc88-6bb7-482e-8e8e-0e22b88f7558)

![image](https://github.com/user-attachments/assets/0b671ba8-0c59-4243-b2c1-2ff0bd9c7b17)

![image](https://github.com/user-attachments/assets/0fa971c9-09a9-42ce-90b6-9513c4ae7605)

![image](https://github.com/user-attachments/assets/97e51225-06c0-42cd-8e95-c2240a483e3c)

![image](https://github.com/user-attachments/assets/d0d86005-18de-43ea-8d18-f9cb87a01037)

![image](https://github.com/user-attachments/assets/0c8b922b-cc82-4e73-9bfc-4854f75cd560)

<hr>

### Limpeza e Preparação dos Dados

```python
#Codificando as variáveis categóricas
le = LabelEncoder()
df['Operation_Mode'] = le.fit_transform(df['Operation_Mode'])
df['Efficiency_Status_encoded'] = le.fit_transform(df['Efficiency_Status'])

#Calculando a disponibilidade por cada máquina
availability = df.groupby('Machine_ID')['Operation_Mode'].apply(lambda x: (x == 1).mean()).reset_index()
availability.columns = ['Machine_ID', 'Availability']

#Calculando o desempenho de cada máquina
max_speed = df['Production_Speed_units_per_hr'].max()
df['Performance'] = df['Production_Speed_units_per_hr'] / max_speed

#Calculando a qualidade 
df['Quality'] = 1 - (df['Quality_Control_Defect_Rate_%'] / 100)

# Calcular OEE
df['OEE'] = df['Performance'] * df['Quality'] * availability.loc[df['Machine_ID']-1, 'Availability'].values

#Verificando o OEE por máquina
oee_by_machine = df.groupby('Machine_ID')['OEE'].mean().sort_values(ascending=False).reset_index()
fig = px.bar(oee_by_machine, x='Machine_ID', y='OEE', title='OEE por Máquina')
fig.show()

#Separando as features e targets
X = df.drop(['Timestamp', 'Efficiency_Status', 'Efficiency_Status_encoded'], axis=1)
y = df['Efficiency_Status_encoded']

#Realizando a Normalização dos dados
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X.select_dtypes(include=['float64', 'int64']))
```

![image](https://github.com/user-attachments/assets/3a77724f-ada0-4e56-855b-88eabd87ea9a)

<hr>

### Análise Exploratória Detalhada

```python
#Selecionando apenas as colunas numéricas
numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
print("Colunas numéricas disponíveis:", numeric_cols)

#Análise de OEE por modo de operação das máquinas
fig = px.box(df, x='Operation_Mode', y='OEE', points="all", 
             title='Distribuição de OEE por Modo de Operação',
             labels={'Operation_Mode': 'Modo de Operação (0=Idle, 1=Active, 2=Maintenance)'})
fig.show()

#Relação entre temperatura e eficiência da máquina
fig = px.scatter(df, x='Temperature_C', y='OEE', color='Efficiency_Status',
                 title='Relação entre Temperatura e OEE',
                 trendline="lowess")
fig.show()

#Relação entre vibração e eficiência
fig = px.scatter(df, x='Vibration_Hz', y='OEE', color='Efficiency_Status',
                 title='Relação entre Vibração e OEE',
                 trendline="lowess")
fig.show()

#Análise de correlação com OEE usando apenas as colunas numéricas
oee_corr = df[numeric_cols].corr()['OEE'].sort_values(ascending=False)
fig = px.bar(oee_corr, title='Correlação com OEE')
fig.update_layout(showlegend=False)
fig.show()

#Análise temporal do OEE 
df['Timestamp'] = pd.to_datetime(df['Timestamp'])
oee_time = df.resample('30T', on='Timestamp')['OEE'].mean().reset_index()
fig = px.line(oee_time, x='Timestamp', y='OEE', title='Variação do OEE ao longo do tempo')
fig.show()

#Análise multivariada com PairPlot para as variáveis mais relevantes
top_vars = oee_corr.index[1:6]  #Pegando as 5 variáveis mais correlacionadas com OEE e faõ a exclusão do próprio OEE
fig = px.scatter_matrix(df[list(top_vars)+['Efficiency_Status']],
                       dimensions=top_vars,
                       color='Efficiency_Status',
                       title='Relações entre Variáveis Principais')
fig.show()

#Heatmap de correlação
corr_matrix = df[numeric_cols].corr()
fig = px.imshow(corr_matrix, 
                text_auto=True, 
                aspect="auto", 
                title='Matriz de Correlação (Apenas Variáveis Numéricas)',
                color_continuous_scale='RdBu',
                zmin=-1, zmax=1)
fig.show()
```

![image](https://github.com/user-attachments/assets/c947abda-e417-4ed6-80d7-368844065f3a)

![image](https://github.com/user-attachments/assets/0dbfa888-20f5-4334-8c4e-44366af6b592)

![image](https://github.com/user-attachments/assets/6d9a2ac3-d730-4136-b0b9-0e4b5c38716e)

![image](https://github.com/user-attachments/assets/7296c93f-9acf-4db0-b199-2a4b4219273d)

![image](https://github.com/user-attachments/assets/08c863c4-8cbd-4d22-9768-8bb23059b148)

![image](https://github.com/user-attachments/assets/a5fb0fe9-13c4-4757-82c8-ad80a25bad6b)

<hr>

### Modelagem Preditiva

```python
#Dividindo os dados em treino e teste
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

#Treinando o modelo Random Forest
rf = RandomForestClassifier(n_estimators=100, random_state=42)
rf.fit(X_train, y_train)

#Avaliando o modelo
y_pred = rf.predict(X_test)
print(classification_report(y_test, y_pred))

#Matriz de confusão
conf_matrix = confusion_matrix(y_test, y_pred)
fig = px.imshow(conf_matrix, 
                labels=dict(x="Predicted", y="Actual", color="Count"),
                x=['Low', 'Medium', 'High'],
                y=['Low', 'Medium', 'High'],
                title='Matriz de Confusão')
fig.show()

#Exibindo a importância das features
feature_importance = pd.DataFrame({
    'Feature': X.columns,
    'Importance': rf.feature_importances_
}).sort_values('Importance', ascending=False)

fig = px.bar(feature_importance, x='Feature', y='Importance', 
             title='Importância das Features no Modelo')
fig.show()
```

![image](https://github.com/user-attachments/assets/181ae2f5-c9bb-4a90-9d5a-3a8389a580b1)

![image](https://github.com/user-attachments/assets/01a9a497-dc99-4355-baa1-6969eaa3779d)

![image](https://github.com/user-attachments/assets/538b7569-959d-4ec1-9c7e-bce9325819ba)

<hr>

### Realizando a Clusterização

```python
#Aplicando o K-Means
kmeans = KMeans(n_clusters=3, random_state=42)
clusters = kmeans.fit_predict(X_scaled)

#Adicionando os clusters ao dataframe
df['Cluster'] = clusters

#Visualizando os clusters
from sklearn.decomposition import PCA

pca = PCA(n_components=2)
principal_components = pca.fit_transform(X_scaled)
principal_df = pd.DataFrame(data=principal_components, columns=['PC1', 'PC2'])
principal_df['Cluster'] = clusters
principal_df['Efficiency_Status'] = df['Efficiency_Status']

fig = px.scatter(principal_df, x='PC1', y='PC2', color='Cluster',
                 title='Visualização dos Clusters (PCA)',
                 hover_data=['Efficiency_Status'])
fig.show()

#Analisando as características dos clusters (apenas colunas numéricas)
cluster_analysis = df.groupby('Cluster').mean(numeric_only=True)
display(cluster_analysis)
```

![image](https://github.com/user-attachments/assets/5b02f97f-40e6-43e1-b40b-11da741b9f6a)

![image](https://github.com/user-attachments/assets/6b4ac0f4-093a-48a4-8a05-0c9f8d728c3c)

<hr>

### Predição de Falhas

```python
#Definindo o status das falhas
df['Failure'] = ((df['Efficiency_Status'] == 'Low') & 
                 (df['Predictive_Maintenance_Score'] < 0.3) & 
                 (df['Error_Rate_%'] > 10)).astype(int)

#Verificando a distribuição da nova variável de falha
print("Distribuição de Falhas:")
print(df['Failure'].value_counts())

#Preparando os dados para modelagem
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_curve, auc, precision_recall_curve, average_precision_score

#Selecionando as features relevantes
features = ['Temperature_C', 'Vibration_Hz', 'Power_Consumption_kW', 
            'Network_Latency_ms', 'Packet_Loss_%', 'Quality_Control_Defect_Rate_%',
            'Production_Speed_units_per_hr', 'Predictive_Maintenance_Score', 
            'Error_Rate_%', 'Operation_Mode']

X = df[features]
y = df['Failure']

#Dividindo os dados em treino e teste
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

#Treinando o modelo
rf_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
rf_model.fit(X_train, y_train)

#Prevendo a probabilidades
y_proba = rf_model.predict_proba(X_test)[:, 1]

#Calculando as métricas
fpr, tpr, thresholds = roc_curve(y_test, y_proba)
roc_auc = auc(fpr, tpr)

precision, recall, _ = precision_recall_curve(y_test, y_proba)
avg_precision = average_precision_score(y_test, y_proba)
```

![image](https://github.com/user-attachments/assets/ea91daf4-8290-45ee-996b-3954944240cb)

<hr>

### Curva ROC

```python
#Curva ROC
fig_roc = px.area(
    x=fpr, y=tpr,
    title=f'Curva ROC (AUC = {roc_auc:.2f})',
    labels=dict(x='Taxa de Falsos Positivos', y='Taxa de Verdadeiros Positivos'),
    width=700, height=500
)
fig_roc.add_shape(
    type='line', line=dict(dash='dash'),
    x0=0, x1=1, y0=0, y1=1
)
fig_roc.update_yaxes(scaleanchor="x", scaleratio=1)
fig_roc.update_xaxes(constrain='domain')
fig_roc.show()
```

![image](https://github.com/user-attachments/assets/1d402529-f069-44b8-9b60-54fbee49cf0a)

<hr>

### Curva Precision-Recall

```python
#Curva de Precision-Recall
fig_pr = px.area(
    x=recall, y=precision,
    title=f'Curva Precision-Recall (AP = {avg_precision:.2f})',
    labels=dict(x='Recall', y='Precision'),
    width=700, height=500
)
fig_pr.add_shape(
    type='line', line=dict(dash='dash'),
    x0=0, x1=1, y0=0.5, y1=0.5
)
fig_pr.update_yaxes(range=[0, 1.1])
fig_pr.update_xaxes(range=[0, 1.1])
fig_pr.show()
```

![image](https://github.com/user-attachments/assets/49ded167-6ca6-4cf1-bd0e-b2ab7cb6a520)

<hr>

### Distribuição das Probabilidades de Falha

```python
#Criando um dataframe com as probabilidades de falha
prob_df = pd.DataFrame({
    'Probability': y_proba,
    'Actual': y_test,
    'Predicted': (y_proba > 0.5).astype(int)
})

#Gráfico de distribuição da probabilidade de falha ocorrer em uma máquina
fig_dist = px.histogram(
    prob_df, x='Probability', color='Actual', nbins=50,
    title='Distribuição das Probabilidades de Falha',
    labels={'Probability': 'Probabilidade de Falha', 'count': 'Contagem'},
    barmode='overlay',
    opacity=0.7,
    width=800, height=500
)
fig_dist.update_layout(
    legend_title_text='Falha Real',
    xaxis_title='Probabilidade de Falha Predita',
    yaxis_title='Contagem'
)
fig_dist.show()
```

![image](https://github.com/user-attachments/assets/0c9eed17-e155-4ade-b2bd-07e251fcf23c)

<hr>

### Probabilidade de Falha Por Máquina

```python
#Calculando as probabilidades para todas as máquinas
df['Failure_Probability'] = rf_model.predict_proba(X)[:, 1]

#Média de falha por máquina
machine_probs = df.groupby('Machine_ID')['Failure_Probability'].mean().reset_index()

#Criando o gráfico de probabilidade de falha por máquina
fig_machines = px.bar(
    machine_probs.sort_values('Failure_Probability', ascending=False),
    x='Machine_ID', y='Failure_Probability',
    title='Probabilidade Média de Falha por Máquina',
    labels={'Machine_ID': 'ID da Máquina', 'Failure_Probability': 'Probabilidade de Falha'},
    width=1000, height=500
)
fig_machines.update_layout(xaxis_tickangle=-45)
fig_machines.show()
```

![image](https://github.com/user-attachments/assets/02a747ff-fbc7-49b4-9ec6-89b8e1c96680)

<hr>

### Relação Entre Variáveis Críticas e Probabilidade de Falha

```python
import plotly.express as px

#Gráfico de dispersão em 2D (Sempre use Esse em 2D)
fig_2d = px.scatter(
    df.sample(1000, random_state=42),
    x='Temperature_C', 
    y='Error_Rate_%',
    color='Failure_Probability',
    title='Relação entre Temperatura e Taxa de Erro (colorido por Probabilidade de Falha)',
    labels={
        'Temperature_C': 'Temperatura (°C)',
        'Error_Rate_%': 'Taxa de Erro (%)',
        'Failure_Probability': 'Prob. Falha'
    },
    width=900, 
    height=700
)

fig_2d.update_traces(marker=dict(size=8, opacity=0.7))
fig_2d.show()
```

![01](https://github.com/user-attachments/assets/5f8438b3-e48c-491b-8dba-0e509ed160ee)

<hr>

### Importância das Features no Modelo de Predição

```python
#Obtendo a importância das features
feature_importance = pd.DataFrame({
    'Feature': features,
    'Importance': rf_model.feature_importances_
}).sort_values('Importance', ascending=False)

#Criando o gráfico de importância das features
fig_importance = px.bar(
    feature_importance,
    x='Importance', y='Feature',
    title='Importância das Variáveis na Predição de Falhas',
    labels={'Importance': 'Importância', 'Feature': 'Variável'},
    width=800, height=500
)
fig_importance.show()
```

![image](https://github.com/user-attachments/assets/01393b3a-2952-49b8-947b-d9b459a02151)










