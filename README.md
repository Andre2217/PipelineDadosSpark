# Pipeline de Dados - NYC Yellow Taxi

## Descrição
Este projeto implementa uma **pipeline de ingestão, transformação e tratamento de dados** de corridas de táxi de Nova York (NYC Yellow Taxi) usando **PySpark**.  

O objetivo é baixar dados brutos, tratar, enriquecer, aplicar filtros de qualidade e gerar arquivos prontos para análise.

---

## Estrutura do Projeto

/PipelineSpark.py -> Código principal da pipeline
/dados/ -> Pasta criada automaticamente ao rodar o código
/bruto/ -> Dados brutos baixados da internet
/saida_csv_particionado/ -> Dados tratados particionados por ano/mes
/saida_csv_unico/ -> Dados tratados em um único CSV (limitado a 200k linhas)

---

## Como rodar

Clone o repositório:
```bash
git clone https://github.com/SEU-USUARIO/SEU-REPO.git
cd SEU-REPO
```
Instale as dependências:
```bash
pip install pyspark
```
Execute o script:
```bash
python PipelineSpark.py
```

Ao rodar o código, a pasta dados será criada automaticamente com:

bruto/ → arquivos .parquet baixados da internet

saida_csv_particionado/ → CSVs tratados separados por ano e mês

saida_csv_unico/ → CSV tratado em um único arquivo (limitado a 200k linhas para teste)

- Transformações e Limpeza

→Coerção de tipos (datas, números, valores monetários)

→Enriquecimento: cálculo de ano, mes, duracao_minutos, velocidade_media_kmh

→Filtros de qualidade:
  Valores monetários ≥ 0
  Duração plausível (até 24h)
  Corridas com distância ou valor > 0
  Velocidade média plausível (< 200 km/h)

→Deduplicação por chave composta
