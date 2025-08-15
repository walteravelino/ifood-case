# Pipeline NYC Taxi Data

Pipeline de processamento para dados de táxis de Nova York.

## Estrutura do Projeto

```
ifood-case/
├── analysis/
│   └── case_questions.sql  # Queries para leitura 
├── src/
│   ├── config.py       # Configurações de paths e tabelas
│   ├── pipeline.py     # Lógica de processamento
│   └── jobs/
│       └── run_pipeline.py  # Job principal
├── README.md
└── requirements.txt
```

## Pré-requisitos

- Databricks Runtime 14.3+
- Unity Catalog habilitado
- Volume criado: `workspace.default.nyc`

## Como Executar

1. Carregue os arquivos Parquet para:
   ```
   dbfs:/Volumes/workspace/default/nyc/landing/
   ```

2. Execute o pipeline principal:
   ```python
   %run /Workspace/Users/seu_user/ifood-case/src/jobs/run_pipeline
   ```

## Camadas de Dados

1. **Bronze**: Dados brutos lidos do Volume
2. **Silver**: Dados limpos e padronizados
3. **Gold**: 
   - Média mensal de valores (yellow taxis)
   - Média horária de passageiros em maio (todos táxis)

## Requerimentos

```text
pyspark>=3.5
```

## Contato

Para problemas ou dúvidas, abra uma issue no repositório.