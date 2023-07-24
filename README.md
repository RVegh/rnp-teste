
# Case Vertigo/RNP - Dev Python Jr(Dados)

Este repositório tem como objetivo responder as perguntas e desafios propostos no case encontrado no arquivo RNP_TESTE.
Resumidamente, se trata de uma série de desafios de engenharia de dados junto a algumas questões de negócio a serem respondidas utilizando como base o banco de dados fictício fornecido.


    ![Logo](https://cdn.pop-pr.rnp.br/logo-rnp.png)



## Detalhes do case

1. Utilize o Database Type Ecommerce disponível em https://uibakery.io/sql-playground para realizar as seguintes atividades:

2. Desenhe o diagrama de ER do banco. Imagem, DBML ou diagrama são aceitáveis.

3. Crie um notebook no Databricks Community ( https://community.cloud.databricks.com )

4. No notebook crie as querys ou código que respondam as seguintes perguntas:

    - Qual país possui a maior quantidade de itens cancelados?

    - Qual o faturamento da linha de produto mais vendido, considere os itens com status  'Shipped', cujo o pedido foi realizado no ano de 2005?
    
    - Nome, sobrenome e email dos vendedores do Japão, o local-part do e-mail deve estar mascarado.

5. Salve os resultados em formato delta

6. Armazene todos os artefatos gerados em um github público.

Documente no README.md todo o processo realizado.

Nos envie os links e arquivos ao final para avaliação.


## Instalação

Caso necessário, é recomendado a instalação das bibliotecas pertinentes a resolução do case através do arquivo requirements.txt, como demonstrado abaixo:
```bash
  pip install -r requirements.txt
```

## Diagrama Entidade - Relacionamento
Realizei o diagrama a partir da ferramenta de diagramas do DBeaver.

![alt text](https://github.com/RVegh/rnp-teste/blob/master/diagrama-%20ER.png?raw=true)

## Observações importantes
Inicialmente, parti da linha de raciocínio de que graças ao ambiente fornecido pelo Databricks Community, conseguiria realizar todo o case, desde a conexão com o banco de dados via jdbc utilizando a string de conexão fornecida, até o armazenamento das respostas em formato Delta apenas utlizando SQL e Pyspark, porém, devido a alguns erros na plataforma e na atualização das configurações do cluster, não foi possível utilizar esta abordagem ideal.

### Entre os principais erros, estão:

Durante a conexão via Pyspark diretamente com o banco de dados, o seguinte erro foi apontado:

```bash
# Create the SparkSession
spark = SparkSession.builder \
    .appName('RNP-TESTE') \
    .config('spark.jars', 'dbfs:/FileStore/jars/1804a3cc_8a18_4ac2_bd0b_b02d0c511941-postgresql_42_6_0-9ae1e.jar') \
    .getOrCreate()

connection_string = "postgresql://kskocgzinxdjzxbefhvzukcm%40psql-mock-database-cloud:jezjgtkxislmdytscsmfzaey@psql-mock-database-cloud.postgres.database.azure.com:5432/ecom1689961191152kwdflhebkfqsxgdn"

query = '''
    SELECT table_name
    FROM information_schema.tables
    '''

df = spark.read \
    .format("jdbc") \
    .option("url", connection_string) \
    .option("query", query) \
    .load()

java.sql.SQLException: No suitable driver
```
Após troubleshooting, descobri que era uma incompatibilidade/falta do driver do Postgresql adequado, porém, mesmo seguindo o procedimento correto de instalação do driver.jar e configuração na SparkSession, não foi possível resolver a tempo para entrega do case.

Como alternativa, tentei utilizar o Pandas para conexão e consulta no banco de dados, para posteriormente converter para um dataframe Pyspark e salvar em formato delta, porém, também sofri com um conflito na chamada do módulo Sqlalchemy.

```bash
AttributeError: 'OptionEngine' object has no attribute 'execute' 
```
## Resolução 
A partir das barreiras encontradas, precisei utilizar a via menos performática, utilizando a biblioteca Polars do Python para executar as consultas, converter para um dataframe do Pandas, para finalmente converter para Pyspark e salvar em formato Delta.

Preferiria não realizar todo esse fluxo, porém foi a solução que encontrei para demonstrar o máximo de habilidades possíveis, sem prejudicar o tempo de entrega do case.

Como pode ser visualizado no notebook disponibilizado no diretório src, utilizei duas funções: query_data e dataframe_to_delta, a primeira para realizar a consulta sql, e a segunda para as conversões e armazenamento em formato delta.

```bash
'''
A função abaixo retorna um dataframe a partir de uma consulta SQL.
    Params:
        query: str - Consulta SQL a ser executada      
'''
def query_data(query : str): 
    try:
        df = pl.read_database(query, connection_string)
        display(df)

        return df
    except Exception as e:
        print(f'Erro: {e}')

'''
A função abaixo converte um dataframe polars para pandas, 
depois pySpark e salva o dataframe em formato Delta.
    Params:
        file_name: str - Nome do arquivo onde o parquet será armazenado.
'''
def dataframe_to_delta(file_name : str):
    try:
        df_pandas = df_polars.to_pandas()
        df_pyspark = spark.createDataFrame(df_pandas)
        display(df_pyspark)
        file_path = f'/dbfs/Users/devrvegh@gmail.com/dados_delta/{file_name}'
        df_pyspark.write.format('delta').save(f'{file_path}')
    except Exception as e:
        print(f'Erro: {e}')
```

## Considerações
 - Todos os artefatos derivados do armazenamento em delta podem ser encontrados na pasta dados_delta.
 - As queries utilizadas podem ser encontradas no diretório src.
 - Caso necessário, o notebook utilizado pode ser executado localmente, sem uso do Databricks, apenas utilizando o Jupyter ou vscode.
 
# Case Vertigo/RNP - Dev Python Jr(Dados)

Este repositório tem como objetivo responder as perguntas e desafios propostos no case encontrado no arquivo RNP_TESTE.
Resumidamente, se trata de uma série de desafios de engenharia de dados junto a algumas questões de negócio a serem respondidas utilizando como base o banco de dados fictício fornecido.


    ![Logo](https://cdn.pop-pr.rnp.br/logo-rnp.png)



## Detalhes do case

1. Utilize o Database Type Ecommerce disponível em https://uibakery.io/sql-playground para realizar as seguintes atividades:

2. Desenhe o diagrama de ER do banco. Imagem, DBML ou diagrama são aceitáveis.

3. Crie um notebook no Databricks Community ( https://community.cloud.databricks.com )

4. No notebook crie as querys ou código que respondam as seguintes perguntas:

    - Qual país possui a maior quantidade de itens cancelados?

    - Qual o faturamento da linha de produto mais vendido, considere os itens com status  'Shipped', cujo o pedido foi realizado no ano de 2005?
    
    - Nome, sobrenome e email dos vendedores do Japão, o local-part do e-mail deve estar mascarado.

5. Salve os resultados em formato delta

6. Armazene todos os artefatos gerados em um github público.

Documente no README.md todo o processo realizado.

Nos envie os links e arquivos ao final para avaliação.


## Instalação

Caso necessário, é recomendado a instalação das bibliotecas pertinentes a resolução do case através do arquivo requirements.txt, como demonstrado abaixo:
```bash
  pip install -r requirements.txt
```

## Diagrama Entidade - Relacionamento
Realizei o diagrama a partir da ferramenta de diagramas do DBeaver.

![alt text](https://github.com/RVegh/rnp-teste/blob/master/diagrama-%20ER.png?raw=true)

## Observações importantes
Inicialmente, parti da linha de raciocínio de que graças ao ambiente fornecido pelo Databricks Community, conseguiria realizar todo o case, desde a conexão com o banco de dados via jdbc utilizando a string de conexão fornecida, até o armazenamento das respostas em formato Delta apenas utlizando SQL e Pyspark, porém, devido a alguns erros na plataforma e na atualização das configurações do cluster, não foi possível utilizar esta abordagem ideal.

### Entre os principais erros, estão:

Durante a conexão via Pyspark diretamente com o banco de dados, o seguinte erro foi apontado:

```bash
# Create the SparkSession
spark = SparkSession.builder \
    .appName('RNP-TESTE') \
    .config('spark.jars', 'dbfs:/FileStore/jars/1804a3cc_8a18_4ac2_bd0b_b02d0c511941-postgresql_42_6_0-9ae1e.jar') \
    .getOrCreate()

connection_string = "postgresql://kskocgzinxdjzxbefhvzukcm%40psql-mock-database-cloud:jezjgtkxislmdytscsmfzaey@psql-mock-database-cloud.postgres.database.azure.com:5432/ecom1689961191152kwdflhebkfqsxgdn"

query = '''
    SELECT table_name
    FROM information_schema.tables
    '''

df = spark.read \
    .format("jdbc") \
    .option("url", connection_string) \
    .option("query", query) \
    .load()

java.sql.SQLException: No suitable driver
```
Após troubleshooting, descobri que era uma incompatibilidade/falta do driver do Postgresql adequado, porém, mesmo seguindo o procedimento correto de instalação do driver.jar e configuração na SparkSession, não foi possível resolver a tempo para entrega do case.

Como alternativa, tentei utilizar o Pandas para conexão e consulta no banco de dados, para posteriormente converter para um dataframe Pyspark e salvar em formato delta, porém, também sofri com um conflito na chamada do módulo Sqlalchemy.

```bash
AttributeError: 'OptionEngine' object has no attribute 'execute' 
```
## Resolução 
A partir das barreiras encontradas, precisei utilizar a via menos performática, utilizando a biblioteca Polars do Python para executar as consultas, converter para um dataframe do Pandas, para finalmente converter para Pyspark e salvar em formato Delta.

Preferiria não realizar todo esse fluxo, porém foi a solução que encontrei para demonstrar o máximo de habilidades possíveis, sem prejudicar o tempo de entrega do case.

Como pode ser visualizado no notebook disponibilizado no diretório src, utilizei duas funções: query_data e dataframe_to_delta, a primeira para realizar a consulta sql, e a segunda para as conversões e armazenamento em formato delta.

```bash
'''
A função abaixo retorna um dataframe a partir de uma consulta SQL.
    Params:
        query: str - Consulta SQL a ser executada      
'''
def query_data(query : str): 
    try:
        df = pl.read_database(query, connection_string)
        display(df)

        return df
    except Exception as e:
        print(f'Erro: {e}')

'''
A função abaixo converte um dataframe polars para pandas, 
depois pySpark e salva o dataframe em formato Delta.
    Params:
        file_name: str - Nome do arquivo onde o parquet será armazenado.
'''
def dataframe_to_delta(file_name : str):
    try:
        df_pandas = df_polars.to_pandas()
        df_pyspark = spark.createDataFrame(df_pandas)
        display(df_pyspark)
        file_path = f'/dbfs/Users/devrvegh@gmail.com/dados_delta/{file_name}'
        df_pyspark.write.format('delta').save(f'{file_path}')
    except Exception as e:
        print(f'Erro: {e}')
```

## Considerações
 - Todos os artefatos derivados do armazenamento em delta podem ser encontrados na pasta dados_delta.
 - As queries utilizadas podem ser encontradas no diretório src.
 - Caso necessário, o notebook utilizado pode ser executado localmente, sem uso do Databricks, apenas utilizando o Jupyter ou vscode.
 