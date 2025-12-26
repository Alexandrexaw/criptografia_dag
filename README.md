[EN]

[PT]

## DAG Extratora para Criptografia de Dados Sensíveis
Esta DAG automatiza o fluxo de extração, transformação (criptografia) e carregamento (ETL) de dados provenientes do BigQuery. 
O objetivo central é garantir que dados sensíveis sejam armazenados de forma segura, utilizando criptografia antes de sua disponibilização em tabelas de consumo.

### Metodologia e Tecnologias Envolvidas
A solução foi construída utilizando uma arquitetura de processamento híbrido (Cloud + Local Worker) para otimizar a performance:

Orquestração: Apache Airflow operando com PythonOperator.

Armazenamento de Dados: Google BigQuery (origem e destino) e Google Cloud Storage (staging de arquivos temporários).

Criptografia: Biblioteca cryptography. Utiliza o algoritmo AES-256 em modo CBC com preenchimento PKCS7 (Chave simétrica + Salt Value).

Segurança de Chaves: Integração com Cofre de Senhas (via API REST), para este caso foi utilizado o Conjur, onde foi realizada a recuperação dinâmica da secret_key e do salt.

Processamento em Lote (Chunking): Uso de pandas e Thread Pool para processar grandes volumes de dados em paralelo sem estourar a memória do worker.

### Funcionalidades

Controle de carga: A task check_max_dt compara as datas de carga da origem e do destino. O processamento só ocorre se houver dados novos na origem, economizando recursos computacionais.

Consolidação de Origens: Realiza um UNION ALL entre as camadas landzone e trusted, removendo duplicatas e garantindo a integridade dos IDs únicos (GPON).

Criptografia Simétrica com Salt: Uso AES-256 com chave simétrica, gera derivação de chave via PBKDF2 com salt fixo, provendo uma camada de segurança e gera valores fixos para possivel descritoptografia se necessário.

Gerenciamento Automático de Limpeza: O script limpa automaticamente as tabelas temporárias do BigQuery, arquivos CSV locais e objetos no GCS após a conclusão (sucesso ou falha).

Observabilidade: Registro detalhado de logs e persistência do status de execução em uma tabela de controle centralizada (log_carga_dados).

### Requisitos de Implantação

Para o funcionamento correto desta DAG, os seguintes requisitos devem ser atendidos:

Variáveis do Airflow: Deve existir uma variável JSON chamada PAR_AR_CRIPTOGRAFIA_LEGADO contendo:

conf: IDs de projeto, datasets, tabelas, bucket de staging e parâmetros de criptografia (iterações, tamanho da chave).

cofre: URLs de autenticação e identificadores para busca das chaves.

Conexões (Connections):

google_cloud_default: Conexão com permissões de leitura/escrita no BigQuery e Storage.

Bibliotecas Python:

cryptography

pandas

google-cloud-storage

google-cloud-bigquery

Acesso de Rede: O worker do Airflow deve ter permissão de saída para a URL do cofre de senhas (API).
