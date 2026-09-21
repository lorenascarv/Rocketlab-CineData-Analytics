# Projeto CineData Analytics

Projeto da atividade **RocketLab 2026.2 — Engenharia de Dados**, implementando um pipeline ETL completo em Databricks, seguindo a Arquitetura Medalhão, com modelagem dimensional em Star Schema e uma tabela de contexto para alimentar um assistente de IA.

# Sumário

- [Camada Bronze](#camada-bronze)
- [Camada Silver](#camada-silver)
- [Camada Gold](#camada-gold)
- [Desafio de Analytics](#desafio-de-analytics)
- [Print Job](printjob.PNG)
- [Arquivo .yaml](job.yaml)

---

## Camada Bronze

- As 5 tabelas foram ingeridas sem qualquer alteração estrutural ou de conteúdo, em modo `append`, com a coluna `ingestion_datetime` adicionada no momento da carga.
- A cotação do dólar foi obtida via API do Banco Central (PTAX), com os últimos 7 dias corridos consultados através de widgets de data no notebook. Após uma pesquisa, resolvi usar o final de semana assim como fica na bolsa de valores, com o mesmo valor da sexta-feira.

---

## Camada Silver
 
### `tb_info_filmes`
- **Normalização e tradução do status**: antes de traduzir, removi hífens soltos e padronizei caixa para capturar variações como `"post_production"`, `"Post-Production"` etc. Valores não mapeáveis viram `"Não Informado"`.
- **Datas multi-formato**: usei `try_to_date` (em vez de `to_date`) para que um formato incompatível gere `NULL` em vez de interromper o pipeline inteiro. A ordem de tentativa dos formatos importa quando há ambiguidade (ex.: `01-02-2018` pode ser 1º de fevereiro ou 2 de janeiro). Priorizei `MM-dd-yyyy` logo após `yyyy-MM-dd`, com base em exemplos encontrados na base (ex.: `04-25-2018`, que só é válido como mês-dia-ano, já que não existe o 25º mês).
  - Após o tratamento, restaram **0,07% de registros nulos** (64 de 97.879). Investigação manual confirmou que a maioria já era nula na origem, e o restante eram fragmentos de texto (trechos de sinopse) migrados para a coluna errada por um deslocamento de colunas (*column shift*) presente na base bruta — conversão genuinamente impossível, conforme previsto pelo enunciado.
- **Deduplicação**: mantido apenas o registro mais recente por `id_filme`, com base em `ingestion_datetime`.

### `tb_financeiro_filmes`
- Valores textuais de ausência de dado (`"Unknown"`, `"Não Informado"`, `"N/A"` etc.) são convertidos para `NULL`.
- Valores zerados ou negativos em orçamento/receita são tratados como ausentes.
- **Cálculo de lucro e margem protegido contra a "casca de banana" dos nulos**: o lucro só é calculado quando ambos, orçamento e receita, existem; a margem percentual só é calculada quando o orçamento é não-nulo e diferente de zero.
- **Conversão para BRL e fallback de cotação**: a tabela `silver.tb_cotacao_dolar` só cobre 7 dias corridos. Como a maioria dos filmes tem `data_lancamento` muito anterior a essa janela, o join direto por data deixaria quase todos os valores em BRL nulos. Decidi que quando não existe cotação para o dia exato do lançamento, usa a cotação mais recente disponível na tabela. 
- **Deduplicação**: mesma lógica de `tb_info_filmes`.

### `tb_metricas_engajamento`
- **Popularidade com separador decimal inconsistente**: quando o valor contém vírgula, ela é tratada como separador decimal; se não, o ponto é tratado normalmente.
- **Conversão segura de tipos**: usei `try_cast` para tolerar textos fora de contexto sem interromper o pipeline, esses valores viram `NULL`.
- **Regras de escala de negócio**: notas médias (TMDB/IMDb) fora do intervalo 0–10 são descartadas (`NULL`); contagens de votos e popularidade negativas também são invalidadas.
- **Deduplicação**: mesma lógica das outras tabelas.

### `tb_avaliacoes_usuarios`
- Registros duplicados são removidos.
- Notas fora da escala 0–10 são descartadas.
- Comentários vazios ou compostos só por espaços em branco recebem o texto padronizado `"Sem comentário"`.

### `tb_generos`
- Separadores são padronizados antes do split/explode.
- A primeira versão do filtro só verificava se o texto continha apenas letras/espaços/hífen, o que não foi suficiente, partes de trechos de sinopse, nomes de personagens, também são compostos só por letras, e passavam pelo filtro disfarçados de gênero. Decidi listar apenas os gêneros válidos no padrão TMDB para ser comparado, qualquer valor fora dessa lista foi descartado.

### `tb_pessoas_empresas`
- Deixa `cast`, `directors`, `writers` e `production_companies` em uma dimensão unificada, com a coluna `tipo_entidade` mapeando cada origem ao papel da pessoa.
- Capitalização padronizada (`initcap`) e duplicatas removidas por `(id_filme, nome_pessoa, tipo_entidade)`.
- **Problema encontrado**: diferente de gêneros, nome de pessoa/empresa não é uma lista de valores válidos para comparar. A limpeza inicial deixava passar muito lixo vindo da base bruta, nomes de arquivo de imagem, caminhos, trechos de sinopse e outros fragmentos de texto migrados para a coluna errada.
- **Solução adotada**, aplicadas em sequência sobre cada valor após o split/explode:
  - Remove aspas, colchetes, barras invertidas e crase (`" [ ] \ ' \``) antes do split, já que esses caracteres não aparecem em nomes legítimos mas são comuns em fragmentos mal formatados.
  - Descarta valores vazios após `trim`.
  - Descarta strings com mais de 40 caracteres, nomes de pessoa/empresa raramente excedem esse tamanho, isso já descartou as sinopses.
  - Descarta valores terminados em extensão de imagem (`.jpg`, `.jpeg`, `.png`, `.gif`, `.webp`).
  - Descarta valores contendo `/` — indício de caminho de arquivo.
  - Descarta valores que começam com caracteres que não fazem sentido no início de um nome (`@`, `(`, `)`, dígitos, `>`, aspas, hífen).
  - Descarta valores contendo pontuação de frase (`.`, `?`, `!`, `_`).
  - Descarta valores com mais de 4 palavras, nomes de pessoa/empresa raramente passam disso.
  - Descarta valores contendo palavras comuns de sinopse em inglês.
- **Limitação reconhecida**: por não ser uma validação contra lista fechada, ela pode acabar descartando nomes legítimos que coincidam com os padrões ou deixar passar algum texto que seja curto, sem pontuação e sem as palavras da lista, mas foi a única solução que encontrei.
- Capitalização padronizada (`initcap`) e duplicatas removidas por `(id_filme, nome_pessoa, tipo_entidade)`.

### `tb_cotacao_dolar`
- Construída uma série temporal contínua, um registro por dia, sem buracos, e dias sem cotação recebem o valor do último dia útil disponível.

---

## Camada Gold
 
- Chaves substitutas geradas via `row_number()` sobre uma ordenação estável, em vez de `monotonically_increasing_id()`, para manter chaves sequenciais e mais fáceis de auditar.
- **`fact_movies_performance`**: construída com `LEFT JOIN` a partir de `dim_movies` — filmes sem registro correspondente em `tb_financeiro_filmes` ou `tb_metricas_engajamento` permanecem na fato com métricas nulas, em vez de serem excluídos. Validação de grão (contagem total vs. `sk_movie_id` distintos) confirma ausência de duplicação.
- **`dim_people`**: como o requisito exige a coluna `tipo_pessoa` como atributo obrigatório da dimensão, uma mesma pessoa que atue em papéis diferentes gera duas linhas distintas, uma por combinação, nome, papel. 
- **Bridge tables (`bridge_movie_genre`, `bridge_movie_person`, `bridge_movie_company`)**: construídas com `INNER JOIN` entre a Silver e as dimensões correspondentes, seguido de `.distinct()` para evitar linhas repetidas. `bridge_movie_person` faz o join pelo par `(nome_pessoa, tipo_pessoa)`, não só pelo nome, já que `dim_people` pode conter a mesma pessoa em papéis diferentes.
- **Tabela de contexto (`gold_genai_movies_context`)**: campos com risco real de nulo (diretor, sinopse, atores, receita, orçamento) recebem valores de fallback textual (ex. `"Diretor não informado"`, `"valor não informado"`) antes da concatenação final, evitando que a função `concat`/`||` invalide o documento inteiro por causa de um único campo nulo.
  - A primeira versão usava `F.lit("0.00")` como fallback para receita/orçamento nulos. Isso foi corrigido, porque "US$ 0.00" diz que o filme não faturou nada, diferente de "dado desconhecido", um assistente de IA consumindo esse texto tiraria uma conclusão errada a partir de um valor ausente disfarçado de zero.
  - Atores principais são agregados via `collect_list` + `concat_ws` a partir da bridge `sk_movie_id → sk_person_id (Ator)`.
---

## Desafio de Analytics
 
Data limite considerada para os recortes de "últimos 2/5 anos": `2026-02-19`.
 
| # | Pergunta | Resposta |
|---|----------|----------|
| 1 | Receita total (R$) somada de todos os filmes | `U$ 834.732.810.004.24` |
| 2 | Top 5 filmes por popularidade | `1. Blue beetle; 2. Gran Turismo; 3. La Fellinette; 4. The Fear Footage 2: Curse of the Tape; 5. wwe survivor series 2018` |
| 3 | Quantidade de filmes por gênero (maior → menor) | <img width="326" height="489" alt="image" src="https://github.com/user-attachments/assets/97323351-f2bc-4b96-991c-eb34119d945f" /> |
| 4 | Top 10 filmes por receita (US$/R$) com ranking | <img width="594" height="285" alt="image" src="https://github.com/user-attachments/assets/f9215453-de79-46d7-90c1-7a3d32d4851f" /> |
| 5 | Ator com mais participações nos últimos 2 anos | `Kevin Hart` |
| 6 | Produtora com maior lucro nos últimos 5 anos | `Universal Pictures` |

 - Achei o resultado dos filmes por popularidade estranho, não sei se deveria ter uma faixa específica da nota de popularidade, mas achei estranho.
---

