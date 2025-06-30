## Controle de Produção e Qualidade Industrial

<hr>

### Sobre o Projeto

Este projeto foi desenvolvido para otimizar a eficiência operacional de uma fábrica industrial, utilizando dados de sensores de máquinas para monitorar desempenho, prever falhas e melhorar a manutenção preventiva. O foco principal foi o cálculo do OEE (Overall Equipment Effectiveness), um KPI crítico para avaliar a produtividade de equipamentos.

#### Dores Sanadas:

- <b>Falhas não previstas:</b> Modelo preditivo de falhas reduz paradas não planejadas.
- <b>Baixa eficiência operacional:</b> Identificação de máquinas com OEE abaixo do esperado.
- <b>Manutenção reativa:</b> Transição para manutenção preditiva baseada em dados.

<hr>

### Problemas de Negócio e Perguntas Chaves

#### Problema Identificado

- 86,5% das máquinas operavam com eficiência baixa.
- Falhas frequentes aumentavam custos de manutenção e reduziam produção.
- Ausência de monitoramento em tempo real de variáveis críticas (temperatura, vibração).

#### Perguntas-Chave Respondidas:

- <b>Qual é o OEE médio das máquinas?</b>
- Resposta: OEE médio de 0,58, abaixo do ideal (0,85).

- <b>Quais fatores mais impactam a eficiência?</b>
- Resposta: Temperatura, vibração e taxa de erro são os maiores influenciadores.

- <b>É possível prever falhas antes que ocorram?</b>
- Resposta: Sim, com 85% de precisão usando machine learning.

- <b>Quais máquinas exigem prioridade na manutenção?</b>
- Resposta: Máquinas 39, 15 e 8 apresentam maior risco de falha.

<hr>

### Principais KPIs e Tendências

<table>
  <thead>
    <tr>
      <th>KPI</th>
      <th>Valor Médio</th>
      <th>Tendência</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>OEE (Eficiência Global)</td>
      <td>0,58</td>
      <td>Estável (variação mais ou menos de 5%)</td>
    </tr>
    <tr>
      <td>Taxa de Falhas</td>
      <td>12%</td>
      <td>Crescente (Aumentou 2% nas últimas 2h)</td>
    </tr>
    <tr>
      <td>Tempo Médio entre Falhas</td>
      <td>3,2 h</td>
      <td>Decrescente (Diminuiu 0,5h)</td>
    </tr>
  </tbody>
</table>

#### Como se relacionam com os objetivos?

- <b>OEE baixo:</b> Indica perda de produtividade.
- <b>Taxa de falhas crescente:</b> Sinaliza necessidade de manutenção preventiva.

<hr>

### Segmentação e Comparação

<table>
  <thead>
    <tr>
      <th>Segmento</th>
      <th>OEE Médio</th>
      <th>Taxa de Falhas</th>
      <th>Ação Recomendada</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Máquinas Ativas</td>
      <td>0,65</td>
      <td>10%</td>
      <td>Monitorar Vibração</td>
    </tr>
    <tr>
      <td>Máquinas Ociosas</td>
      <td>0,40</td>
      <td>5%</td>
      <td>Reduzir Tempo Ocioso</td>
    </tr>
    <tr>
      <td>Máquinas em Manutenção</td>
      <td>0,20</td>
      <td>25%</td>
      <td>Revisar Procedimentos</td>
    </tr>
  </tbody>
</table>

<b>Insight:</b> Máquinas em modo Ocioso têm baixo OEE, mas também menor taxa de falhas.

<hr>

### Recomendações Que Eu Sugiro

#### Prioridade Alta 

- Implementar alertas em tempo real para temperatura maiores que 80°C e vibração maiores que 4Hz.
- Otimizar turnos das 02:00 às 04:00 (período de menor OEE).

#### Prioridade Média 

- Automatizar coleta de dados para incluir histórico de manutenções.
- Dashboard em tempo real para monitorar OEE e falhas preditas.

<hr>

### Conclusão

#### Principais Achados

- 86,5% das máquinas operam com eficiência baixa – oportunidade de ganho de produtividade.
- Modelo de falhas preditivo alcançou 85% de precisão – reduzirá custos de manutenção.
- Temperatura e vibração são críticas – exigem monitoramento contínuo.
