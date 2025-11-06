# ETL Automatizado com Apache Airflow — AdventureWorks 

## Introdução

Este projeto tem como objetivo construir e orquestrar um pipeline de ETL automatizado utilizando o Apache Airflow, com base no conjunto de dados do AdventureWorks. A solução aplica conceitos de modelagem dimensional, extração de dados com Python, e análise de indicadores (KPIs) em um ambiente integrado com SQL Server e PostgreSQL.


## 🧩 Estrutura do Projeto

```
POSTGRES_INGESTION_AIRFLOW-MAIN/
├── dags/                  # DAG principal do Airflow (ETL.py)
│   └── scripts/           # Funções de extração, transformação e carga (ext.py)
├── config/                # Configuração de conexões (config.py)
├── SQL/                   # Consultas SQL para análise de KPIs
├── docker-compose.yaml    # Infraestrutura com Airflow e dependências
└── requirements.txt       # Dependências do projeto
```

## ⚙️ Tecnologias Utilizadas
- 	🐍 Python 3.12
-	🛠️ Apache Airflow 2.7+
-	🧮 Pandas, SQLAlchemy, PyODBC, Psycopg2
-	🗄️ SQL Server 2022 (AdventureWorks)
- 🐘 PostgreSQL 15
-	🐳 Docker + Docker Compose


## 🧱 Modelo Multidimensional

O modelo segue um **esquema estrela**, com a tabela fato principal `fato_vendas` conectada às dimensões:

**Fato Principal**
- `fato_vendas`

**Dimensões**
- `dim_cliente`
- `dim_produto`
- `dim_pessoa`
- `dim_territorio`
- `dim_data`

## 📊 Indicadores (KPIs)

| # | Indicador | Descrição |
|---|------------|-----------|
| 1 | Receita Total | Soma total de vendas realizadas |
| 2 | Lucro Estimado |Receita líquida após custo estimado |
| 3 | Lucro Total | Receita líquida após custos |
| 4 | Ticket Médio | Valor médio por pedido |
| 5 | Clientes Ativos | Total de clientes únicos com pedidos |
| 6 | Tempo Médio entre Pedidos | Média de dias entre compras consecutivas |
| 7 | Produtos Mais Vendidos | Ranking por quantidade vendida |
| 8 | Receita por Região | Faturamento agrupado por território |
| 9 | Margem por Produto | Lucro total por item |
| 10 | Distribuição Geográfica|Número de clientes por país |

---

## 🚀 Execução da ETL

### 1️⃣ Criar o banco de dados PostgreSQL
Crie um banco no PostgreSQL com o nome , que será utilizado como destino dos dados transformados.

### 2️⃣ Execução do DAG


O script irá:

✅ Extrair dados do SQL Server (AdventureWorks)  
✅ Transformar e padronizar os dados  
✅ Carregar as tabelas no PostgreSQL  

As tabelas criadas serão:
- `dim_pessoa`
- `dim_cliente`
- `dim_produto`
- `dim_territorio`
- `dim_data`
- `fato_vendas`

---

## 📈  Exemplo de Consulta (KPI: Produtos Mais Vendidos)

```sql
SELECT p.nome_produto, SUM(f."OrderQty") AS total_qtd
FROM fato_vendas f
JOIN dim_produto p ON f."ProductID" = p.id_produto
GROUP BY p.nome_produto
ORDER BY total_qtd DESC
LIMIT 10;
```

---

## 📚 Autor
**Hebert Souza Raphalsky do Nascimento**  
---
