![Language](https://img.shields.io/badge/Language-English-blue)

# Data Extraction DAG with Sensitive Data Encryption

This DAG automates the extraction, transformation (encryption), and loading (ETL) workflow for data originating from BigQuery. 
The central objective is to ensure that sensitive data is stored and encrypted using a key + salt value.

## Methodology and Technologies Involved

**Orchestration:** Apache Airflow operating with the PythonOperator.

**Data Storage:** Google BigQuery (source and destination) and Google Cloud Storage (temporary file staging).

**Encryption:** cryptography library. It uses the AES-256 algorithm in CBC mode with PKCS7 padding (Symmetric Key + Salt Value).

**Key Security:** Integration with a Password Vault (via REST API). In this case, Conjur was used for the dynamic retrieval of the secret_key and the salt value.

**Batch Processing (Chunking):** Use of pandas and Thread Pool to process large volumes of data in parallel without exceeding worker memory limits.

## Features

**Load Control:** The check_max_dt task compares the load dates of the source and destination. Processing only occurs if there is new data at the source.

**Source Consolidation:** Performs a UNION ALL between the landzone and trusted layers, removing duplicates and ensuring the integrity of unique IDs.

**Symmetric Encryption with Salt:** Uses AES-256 with a symmetric key, generating key derivation via PBKDF2 with a fixed salt, providing a security layer while generating consistent values for potential decryption if necessary.

**Automated Cleanup Management:** The script automatically cleans up temporary BigQuery tables, local CSV files, and GCS objects upon completion (success or failure).

**Observability:** Detailed logging and execution status persistence in a centralized control table (log_carga_dados).

## Deployment Requirements

For this DAG to function correctly, the following requirements must be met:

+ Airflow Variables: There must be a JSON variable named PAR_AR_CRIPTOGRAFIA_LEGADO containing:

+ conf: Project IDs, datasets, tables, staging bucket, and encryption parameters (iterations, key size).

+ cofre: Authentication URLs and identifiers for key retrieval.

**Connections:**

+ google_cloud_default: Connection with read/write permissions for BigQuery and Storage.

**Python Libraries:**

+ cryptography

+ pandas

+ google-cloud-storage

+ google-cloud-bigquery

+ Network Access: The Airflow worker must have outbound permission to the Password Vault URL (API).

***
![Português](https://img.shields.io/badge/Idioma-Portugu%C3%AAs-green)

# DAG Extratora que realiza Criptografia de Dados Sensíveis
Esta DAG automatiza o fluxo de extração, transformação (criptografia) e carregamento (ETL) de dados provenientes do BigQuery. 
O objetivo central é garantir que dados sensíveis sejam armazenados e criptografados utilizando key + salt value.

## Metodologia e Tecnologias Envolvidas

**Orquestração**: Apache Airflow operando com PythonOperator.

**Armazenamento de Dados**: Google BigQuery (origem e destino) e Google Cloud Storage (staging de arquivos temporários).

**Criptografia**: Biblioteca cryptography. Utiliza o algoritmo AES-256 em modo CBC com preenchimento PKCS7 (Chave simétrica + Salt Value).

**Segurança de Chaves**: Integração com Cofre de Senhas (via API REST), para este caso foi utilizado o Conjur, onde foi realizada a recuperação dinâmica da secret_key e do salt value.

**Processamento em Lote (Chunking)**: Uso de pandas e Thread Pool para processar grandes volumes de dados em paralelo sem estourar a memória do worker.

## Funcionalidades

**Controle de carga**: A task check_max_dt compara as datas de carga da origem e do destino. O processamento só ocorre se houver dados novos na origem.

**Consolidação de Origens**: Realiza um UNION ALL entre as camadas landzone e trusted, removendo duplicatas e garantindo a integridade dos IDs únicos.

**Criptografia Simétrica com Salt**: Uso AES-256 com chave simétrica, gera derivação de chave via PBKDF2 com salt fixo, provendo uma camada de segurança e gera valores fixos para possivel descritoptografia se necessário.

**Gerenciamento Automático de Limpeza**: O script limpa automaticamente as tabelas temporárias do BigQuery, arquivos CSV locais e objetos no GCS após a conclusão (sucesso ou falha).

**Observabilidade**: Registro detalhado de logs e persistência do status de execução em uma tabela de controle centralizada (log_carga_dados).

## Requisitos de Implantação

 Para o funcionamento correto desta DAG, os seguintes requisitos devem ser atendidos:

+ Variáveis do Airflow: Deve existir uma variável JSON chamada PAR_AR_CRIPTOGRAFIA_LEGADO contendo:

+ conf: IDs de projeto, datasets, tabelas, bucket de staging e parâmetros de criptografia (iterações, tamanho da chave).

+ cofre: URLs de autenticação e identificadores para busca das chaves.

**Conexões (Connections):**

+ google_cloud_default: Conexão com permissões de leitura/escrita no BigQuery e Storage.

**Bibliotecas Python:**

+ cryptography

+ pandas

+ google-cloud-storage

+ google-cloud-bigquery

+ Acesso de Rede: O worker do Airflow deve ter permissão de saída para a URL do cofre de senhas (API).
