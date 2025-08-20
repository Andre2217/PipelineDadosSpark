import os, urllib.request, glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import col


pasta_bruto = "/content/PipelineDadosSpark/dados/bruto"
os.makedirs(pasta_bruto, exist_ok=True)

base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
arquivos = ["yellow_tripdata_2023-01.parquet"]

for a in arquivos:
    url = f"{base}/{a}"
    destino = f"{pasta_bruto}/{a}"
    if not os.path.exists(destino):
        print("Baixando:", url)
        urllib.request.urlretrieve(url, destino)

print("Arquivos brutos baixados:")
print(glob.glob(pasta_bruto + "/*.parquet"))



sessao = (SparkSession.builder
          .appName("PipelineTaxiPTBR")
          .config("spark.sql.session.timeZone", "UTC")
          .config("spark.sql.shuffle.partitions", "200")
          .getOrCreate())

sessao.sparkContext.setLogLevel("WARN")
print("Versão Spark:", sessao.version)



caminho_bruto = "/content/PipelineDadosSpark/dados/bruto/*.parquet"
dados_brutos = sessao.read.parquet(caminho_bruto)

print("Linhas lidas (bruto):", dados_brutos.count())
dados_brutos.printSchema()
dados_brutos.show(5, truncate=False)




# ---- mapeamento de nomes (bruto -> português) ----
mapa_colunas = {
    "vendor_id": "id_fornecedor",
    "tpep_pickup_datetime": "coleta_datahora",
    "tpep_dropoff_datetime": "entrega_datahora",
    "passenger_count": "passageiros",
    "trip_distance": "distancia_km",
    "RatecodeID": "id_tarifa",
    "store_and_fwd_flag": "armazenar_encaminhar",
    "PULocationID": "id_local_coleta",
    "DOLocationID": "id_local_entrega",
    "payment_type": "tipo_pagamento",
    "fare_amount": "valor_corrida",
    "extra": "extra",
    "mta_tax": "taxa_mta",
    "tip_amount": "valor_gorjeta",
    "tolls_amount": "valor_pedagio",
    "improvement_surcharge": "sobretaxa_melhoria",
    "total_amount": "valor_total",
    "congestion_surcharge": "sobretaxa_congestao",
    "airport_fee":"taxa_aeroporto"
}

colunas_existentes = dados_brutos.columns
dados = dados_brutos
for antigo, novo in mapa_colunas.items():
    if antigo in colunas_existentes:
        dados = dados.withColumnRenamed(antigo, novo)
        
# ---- coerção de tipos importantes ----
if "coleta_datahora" in dados.columns:
    dados = dados.withColumn("coleta_datahora", F.to_timestamp("coleta_datahora"))
if "entrega_datahora" in dados.columns:
    dados = dados.withColumn("entrega_datahora", F.to_timestamp("entrega_datahora"))
    
# monetárias e numéricas
colunas_monetarias = ["valor_corrida","extra","taxa_mta","valor_gorjeta","valor_pedagio",
                      "sobretaxa_melhoria","sobretaxa_congestao","valor_total"]
for c in colunas_monetarias:
    if c in dados.columns:
        dados = dados.withColumn(c, F.col(c).cast("double"))

if "distancia_km" in dados.columns:
    dados = dados.withColumn("distancia_km", F.col("distancia_km").cast("double"))

if "passageiros" in dados.columns:
    dados = dados.withColumn("passageiros", F.col("passageiros").cast("int"))
    
# armazenar_encaminhar para 'SIM'/'NAO'
if "armazenar_encaminhar" in dados.columns:
    dados = dados.withColumn("armazenar_encaminhar", F.upper(F.trim(F.col("armazenar_encaminhar"))))
    dados = dados.withColumn(
        "armazenar_encaminhar",
        F.when(F.col("armazenar_encaminhar") == "Y", F.lit("SIM"))
         .when(F.col("armazenar_encaminhar") == "N", F.lit("NAO"))
         .otherwise(F.col("armazenar_encaminhar"))
    )
    
# ---- enriquecimento (ano/mes, duracao, velocidade) ----
if "coleta_datahora" in dados.columns:
    dados = dados.withColumn("ano", F.year("coleta_datahora"))
    dados = dados.withColumn("mes", F.month("coleta_datahora"))

if set(["coleta_datahora","entrega_datahora"]).issubset(set(dados.columns)):
    dados = dados.withColumn("duracao_minutos",
                             (F.unix_timestamp("entrega_datahora") - F.unix_timestamp("coleta_datahora"))/60.0)
    dados = dados.withColumn("duracao_minutos",
                             F.when(F.col("duracao_minutos") > 0, F.col("duracao_minutos")).otherwise(F.lit(None)))
    if "distancia_km" in dados.columns:
        dados = dados.withColumn(
            "velocidade_media_kmh",
            F.when(F.col("duracao_minutos") > 0,
                   F.col("distancia_km") / (F.col("duracao_minutos")/60.0)
            ).otherwise(F.lit(None))
        )

# tipo_pagamento descrição
if "tipo_pagamento" in dados.columns:
    dados = dados.withColumn("tipo_pagamento", F.col("tipo_pagamento").cast("int"))
    dados = (dados
        .withColumn("tipo_pagamento_descricao",
            F.when(F.col("tipo_pagamento")==1, F.lit("CartaoCredito"))
             .when(F.col("tipo_pagamento")==2, F.lit("Dinheiro"))
             .when(F.col("tipo_pagamento")==3, F.lit("SemCobranca"))
             .when(F.col("tipo_pagamento")==4, F.lit("Disputa"))
             .when(F.col("tipo_pagamento")==5, F.lit("Desconhecido"))
             .when(F.col("tipo_pagamento")==6, F.lit("Anulado"))
             .otherwise(F.lit("Outro"))
        )
    )


#filtros de qualidade
linhas_iniciais = dados.count()

# valores monetários não-negativos
for c in [c for c in colunas_monetarias if c in dados.columns]:
    dados = dados.filter((F.col(c).isNull()) | (F.col(c) >= 0))

# duração plausível (até 24h)
if "duracao_minutos" in dados.columns:
    dados = dados.filter((F.col("duracao_minutos").isNull()) | (F.col("duracao_minutos") <= 24*60))

# remover corridas sem distância e sem valor
dados_limpos = dados.filter(~((col("distancia_km") == 0) & (col("valor_total") == 0)))

# remover corridas com velocidade média impossível (> 300 km/h)
dados_limpos = dados_limpos.filter((col("velocidade_media_kmh") >= 0) & (col("velocidade_media_kmh") < 300))


# deduplicação por chave composta
chaves = [c for c in ["coleta_datahora","entrega_datahora","id_fornecedor",
                      "id_local_coleta","id_local_entrega","valor_total"] if c in dados.columns]
linhas_pre_dedup = dados.count()
dados = dados.dropDuplicates(subset=chaves) if chaves else dados
linhas_pos_dedup = dados.count()

linhas_finais = dados.count()

print("Linhas iniciais:", linhas_iniciais)
print("Removidas por filtros:", linhas_iniciais - linhas_pre_dedup)
print("Duplicatas removidas:", linhas_pre_dedup - linhas_pos_dedup)
print("Linhas finais:", linhas_finais)

# dados.select(*[c for c in ["coleta_datahora","entrega_datahora","passageiros",
#                            "distancia_km","valor_total","tipo_pagamento","tipo_pagamento_descricao",
#                            "ano","mes","duracao_minutos","velocidade_media_kmh"] if c in dados.columns])\
#      .show(10, truncate=False)
# dados_limpos.show(30, truncate=False)

#Saida em vairas pastas separadas por ano/mes
pasta_saida_part = "/content/PipelineDadosSpark/dados/saida_csv_particionado"
(dados
 .write.mode("overwrite")
 .option("header", "true")
 .partitionBy("ano","mes")
 .csv(pasta_saida_part)
)
print("Gerado em:", pasta_saida_part)

#Saida em uma unica pasta
pasta_saida_unico = "/content/PipelineDadosSpark/dados/saida_csv_unico"
(dados
 .limit(200_000)
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", "true")
 .csv(pasta_saida_unico)
)
print("Gerado em:", pasta_saida_unico)

