/* 
+--------------------------------------------------------------------------------------------+
|UNIVERSIDADE FEDERAL DE SANTA CATARINA - UFSC                                               |
+--------------------------------------------------------------------------------------------+
| Programa de Pós-Graduação em Ciência da Informação                                         |
| Tese: CONEXÕES PARLAMENTARES: GRAFOS LEGISLATIVOS PARA IMPACTO DECISIVO EM PROJETOS DE LEI |                                                           |  
| Discente: Manoel Camilo de Sousa Netto                                                     |
| Orientador: Professor Doutor Adilson Luiz Pinto                                            |
+--------------------------------------------------------------------------------------------+
| Descrição do Código: SQL de geração da Rede Social de Concordâncias baseadas nos votos.    |
| O objetivo do Script SQL é obter uma rede na qual os deputados são interligados            |
| por arestas cujo peso significa a quantidade de concordâncias entre esses dois             |
| parlamentares. Cada trecho de Script possui breve comentário acerca do que está sendo      |
| executado.                                                                                 |   
| De forma resumida o script executa as seguintes tarefas:                                   |
|      - [ Tarefa Analítica 4 ]: Exclui votações não relevantes;                             |
|      - [ Tarefa Analítica 5 ]: Cria Rede de Concordâncias;                                 |
|      - [ Tarefa Analítica 6 ]: Exclui as concordâncias não duradouras (circunstanciais)    |
|      - [ Tarefa Analítica 7 ]: Título da tarefa: Identificar, qualitativamente,            |
|                                comunidades legislativas ideológicas por meio da            |
|                                Ideologia partidária conhecida.                             |
|                                                                                            |
| Tempo aproximado de execução do script: 2min 44seg                                         |
| Hardware utilizado: Computador com processador MD Ryzen 5,2.10 GHz, Windows 11, 16 Gb RAM  |  
| Software: Sistema Operacional Windows 11, MySQL 5.7.44                                     |
+--------------------------------------------------------------------------------------------+
| Convenções:                                                                                |
|     - As tabelas originais - advindas do site de fontes abertas da Câmara dos deputados    | 
|       adotam a nomenclatura iniciada com "TB_", enquanto as tabelas geradas pela           |
|       pesquisa iniciam-se com "_TB_".                                                      |
|     - As tabelas temporárias (usadas apenas para processamento intermediário) possuem a    |
|       nomenclatura finalizada como sufixo "_TEMP".                                         |
|     - Cada trecho de script possui breve comentário acerca do que está sendo executado.    |
|     - A versão integral deste e dos demais scripts podem ser baixados em:                  |
|       https://github.com/ManoelCamilo/tese                                                 |
+--------------------------------------------------------------------------------------------+
*/
 
/*
+--------------------------------------------------------------------------------------------+
| (SCRIPT_SQL.SECAO.SEM_NUMERO) Título: Fixação do ano da Legislatura                                                
+--------------------------------------------------------------------------------------------+
*/
SET @ANO='2022';
SELECT @ANO;

/*
+--------------------------------------------------------------------------------------------+
| Referência: SCRIPT_SQL_TAREFA_04 
| Tarefa do Fluxograma: [ Tarefa Analítica 4 ] 
| Título da tarefa: Exclui votações não relevantes
| Descrição: votações relevantes, segundo a metodologia adotada, são aquelas com 
| quórum acima da mediana. Além disso, nessa seção são executadas diversas tarefas meramente 
| operacionais, sem lógica metodológica envolvida e que visam apenas otimização de consultas, 
| tais como criação de índices, criação de chaves e conversão de tipos.
+--------------------------------------------------------------------------------------------+
*/

/* Passo 1 - Otimização de tabelas e totalização de votos para calcular quórum
==========
    - Criação de uma chave primária na tabela de votações (TB_VOTC) [Otimização].
    - Criação de relacionamento (chave estrangeira) entre a tabela de votos (TB_VOTO) e  
      a tabela de votações (TB_VOTC) com objetivo de agilizar as consultas [Otimização]. 
    - Para calcular o quórum é necessário totalizar os votos de cada votação. Assim, foi  
      criada a tabela “_TB_QTD_VOTO”  c/ a quantidade total de votos de cada uma delas. 
     
*/
CREATE INDEX PK_TB_VOTC ON TB_VOTC(ID);
ALTER TABLE 
      TB_VOTO 
    ADD CONSTRAINT FK_TB_VOTC
    FOREIGN KEY(IDVOTACAO) 
    REFERENCES TB_VOTC(ID);

DROP TABLE IF EXISTS _TB_QTD_VOTO;
CREATE TABLE _TB_QTD_VOTO AS 
SELECT ID,
       COUNT(*) AS QUANT_VOT,
       100*TRUNCATE(((COUNT(*))/513),4)  AS PER_VOT       
 FROM TB_VOTC VTC, 
      TB_VOTO VT 
 WHERE 
     VTC.ID = VT.IDVOTACAO AND 
     (VOTO ='Sim' OR VOTO='Não') AND 
     YEAR(VT.dataHoraVoto)=@ANO
 GROUP BY ID;


/* Passo 2 - Cálculo da mediana e exclusão de votações não relevantes (quórum<=mediana)
==========
|    - Cálculo da Mediana de quórum 
|    - Exclusão de votações não relevantes (Excluídas se quórum <= mediana). 
|    - Cria uma tabela de votos “_TB_VOTO” contendo apenas os votos das votações relevantes
|      (será utilizada na geração das Arestas da Rede Social de Concordância) 
|    
*/

SET @rowindex := -1;
SET @MEDIANA_QUORUM:=(SELECT AVG(d.PER_VOT) AS MEDIAN 
  FROM
    (SELECT @rowindex:=@rowindex + 1 AS rowindex,
          _TB_QTD_VOTO.PER_VOT AS PER_VOT
     FROM _TB_QTD_VOTO
     ORDER BY _TB_QTD_VOTO.PER_VOT) AS D
  WHERE
     D.rowindex IN (FLOOR(@rowindex / 2), CEIL(@rowindex / 2)));

SELECT @MEDIANA_QUORUM;


/* Passo 3
==========
          Exclui da tabela de totalizações de votos (_TB_QTD_VOTO) aqueles votos das votações 
          com quórum <= mediana dos quóruns
*/
DELETE FROM _TB_QTD_VOTO WHERE PER_VOT<=@MEDIANA_QUORUM;


/* Passo 4
==========
          Cria uma tabela temporária _TB_VOTO contendo os votos do ano fixado 
          no início do script, bem como apenas os votos das votações relevantes
          ( _TB_QTD_VOTO contém apenas as votações relevantes)
*/
DROP TABLE IF EXISTS _TB_VOTO;
CREATE TABLE _TB_VOTO AS 
      SELECT * FROM TB_VOTO 
        WHERE YEAR(DATAHORAVOTO)=@ANO AND 
              IDVOTACAO IN (SELECT IDVOTACAO FROM _TB_QTD_VOTO);


/* Passo 5
==========
         Na tabela de votos, separando o ano da data de votação, armazenando-o isolado numa coluna do tipo 
         INTEIRO para agilizar consultas posteriores, pois as consultas usando tipo inteiro são mais rápidas
         do que consultas que usam tipo VARCHAR (string)
*/
ALTER TABLE _TB_VOTO ADD COLUMN ANO INT;
UPDATE _TB_VOTO SET ANO= YEAR(DATAHORAVOTO);

/* Passo 6
==========
         Obtendo o Identificador único do Deputado (DEPUTADO_ID, do tipo VARCHAR) e armazenando o valor
         dele numa nova coluna criada do tipo INTEIRO (DEPUTADO_ID_INT INT) para otimizar consultas 
         futuras, pois as consultas usando tipo inteiro são mais rápidas do que consultas que usam tipo 
         VARCHAR (string)
*/
ALTER TABLE _TB_VOTO ADD COLUMN DEPUTADO_ID_INT INT;
UPDATE _TB_VOTO SET DEPUTADO_ID_INT = DEPUTADO_ID; 

/* Passo 7
==========
         Convertendo o ID da votação (IDVOTACAO, tipo VARCHAR)e armazenando o valor dele numa 
         nova coluna criada do tipo BIG INTEGER, pois as consultas usando tipo inteiro são mais
         rápidas do que consultas que usam tipo VARCHAR (string). Nessa conversão foram retirados 
         os caracteres de hífen ("-") que separavam o dígito verificador.
*/
ALTER TABLE _TB_VOTO ADD COLUMN IDVOTACAO_INT BIGINT;
UPDATE _TB_VOTO SET IDVOTACAO_INT = REPLACE(IDVOTACAO,"-","");

/* Passo 8
==========
         Excluindo os votos dos tipos abstenções e Obstruções - tratadas como ruído por serem 
         um universo insignificante do total. Em 2022 foram 2.919 ocorrências de 194.316 (0,015% do total).
         Foram mantidos apenas os votos reais - ou seja, aqueles com valor "Sim" (aprovar) ou "Não" (rejeitar).
*/
DELETE FROM _TB_VOTO WHERE (VOTO<>'Sim' AND VOTO<>'Não');

/* Passo 9
==========
         Transformando os votos de texto (VOTO, tipo VARCHAR) em inteiros. 
         "Sim" passou a ser 1 (um) e "Não" passa a ser 0 (zero), ambos armazenados 
         em uma novas coluna do tipo Inteiro (VOTO_INT). Consultas usando "joins" 
         entre tabelas que comparam tipos inteiros são mais rápidas do que consultas 
         que comparam textos.
*/
ALTER TABLE _TB_VOTO ADD COLUMN VOTO_INT INT;
UPDATE _TB_VOTO SET VOTO_INT=1 WHERE VOTO='Sim';
UPDATE _TB_VOTO SET VOTO_INT=0 WHERE VOTO='Não';

/* Passo 10
===========
         Consultas futuras exigirão “JOINS” entre colunas dos identificadores de votação e de Deputado. Foram criados índices envolvendo essas colunas para otimizar o tempo de execução.
*/
CREATE INDEX IDX_DEP_VTC ON _TB_VOTO(DEPUTADO_ID_INT,IDVOTACAO_INT);
CREATE INDEX IDX_VTC_DEP ON _TB_VOTO(IDVOTACAO_INT,DEPUTADO_ID_INT);

/*
+--------------------------------------------------------------------------------------------+
| Referência: SCRIPT_SQL_TAREFA_05 
| Tarefa do Fluxograma: [ Tarefa Analítica 5 ] 
| Título da tarefa: Criar Rede de Concordâncias
| Descrição: Criação da Rede de concordâncias – A rede de concordâncias é uma tabela de    
|       arestas de peso(weight) que unem deputados pela quantidade de votos iguais para a 
|       mesma votação relevante
+--------------------------------------------------------------------------------------------+
*/


/* Passo 1
==========
        Criação da tabela temporária “_TB_ARESTAS_TEMP” para   
        armazenamento de processamentos intermediários. A consulta faz um “auto” JOIN entre a 
        tabela de votos  ("_TB_VOTO") e ela mesma para contabilizar, para cada par de deputados 
        diferentes numa mesma votação, a quantidade os votos concordantes (Na mesma votação 
        votaram da mesma forma). No final a rede conterá os campos "source" (nó origem),
        "target" (nó destino) e weight (peso da aresta = quantidade de votos iguais)
*/
DROP TABLE IF EXISTS _TB_ARESTAS_TEMP;
CREATE TABLE _TB_ARESTAS_TEMP AS 
SELECT A.DEPUTADO_ID_INT AS SOURCE,
       B.DEPUTADO_ID_INT AS TARGET, 
       COUNT(*) AS WEIGHT FROM 
(SELECT * FROM _TB_VOTO) A, 
(SELECT * FROM _TB_VOTO) B 
WHERE 
A.DEPUTADO_ID_INT<>B.DEPUTADO_ID_INT AND /*DEPUTADOS DIFERENTES */
A.IDVOTACAO_INT=B.IDVOTACAO_INT AND /* QUE PARTICIPARAM DA MESMA VOTAÇÃO */
A.VOTO_INT=B.VOTO_INT /* E QUE VOTARAM DA MESMA FORMA */
GROUP BY
A.DEPUTADO_ID_INT, B.DEPUTADO_ID_INT;



/* Passo 2
==========
        Considerando que, na Rede Social de concordâncias a ser criada, as arestas não 
        são direcionadas, então podemos afirmar que, para quaisquer arestas que una dois 
        vértices(deputados) X e Y, a aresta (DEPUTADO X,DEPUTADO Y) é a mesma aresta 
        que (DEPUTADO Y, DEPUTADO X).Por essa razão, há necessidade de excluir as arestas 
        duplicadas. Ao final, teremos todas as arestas de rede (SOURCE, TARGET,WEIGHT)
*/


DROP TABLE IF EXISTS TB_ARESTAS;
CREATE TABLE TB_ARESTAS AS
  SELECT 
    LEAST(SOURCE, TARGET) AS SOURCE, 
    GREATEST(SOURCE, TARGET) AS TARGET, 
    WEIGHT AS WEIGHT 
FROM _TB_ARESTAS_TEMP
    GROUP BY 
    LEAST(SOURCE, TARGET), 
    GREATEST(SOURCE, TARGET),
    WEIGHT;


/* Passo 3
==========
        Para otimizar a tabela de arestas foi criado índice nos identificadores dos vértices
*/

CREATE INDEX IDX_TB_ARESTAS ON TB_ARESTAS(SOURCE,TARGET);


/*
+--------------------------------------------------------------------------------------------+
| Referência: SCRIPT_SQL_TAREFA_06 
| Tarefa do Fluxograma: [ Tarefa Analítica 6 ] 
| Título da tarefa: Excluir as concordâncias não duradouras (circunstanciais)
| Título: Para possibilitar que apenas as concordâncias mais proeminentes da Rede sejam  
|        evidenciadas, as concordâncias menos relevantes foram removidas. Para tanto, foi 
|        calculada a mediana das concordâncias (que na rede é expressa peso numérico das 
|        arestas). A arestas com pesos menores ou iguais à mediana dos pesos foram excluídas.
+--------------------------------------------------------------------------------------------+
*/


/* Passo 1
==========
        Cálculo da mediana
*/

SET @rowindex := -1;
SET @MEDIANA_CONCORDANCIA:=(SELECT AVG(d.WEIGHT) as MEDIAN
FROM
   (SELECT @rowindex:=@rowindex + 1 AS rowindex,
          TB_ARESTAS.WEIGHT AS WEIGHT
    FROM TB_ARESTAS
    ORDER BY TB_ARESTAS.WEIGHT) AS d
WHERE
d.rowindex IN (FLOOR(@rowindex / 2), CEIL(@rowindex / 2)));


/* Passo 2
==========
        Exclusão das concordâncias transitórias - assim consideradas pela pesquisa
        aquelas cuja quantidade de concordâncias seja menor ou igual a mediana
*/
DELETE FROM TB_ARESTAS WHERE WEIGHT<=@MEDIANA_CONCORDANCIA;



/*
/*
+--------------------------------------------------------------------------------------------+
| Referência: SCRIPT_SQL_TAREFA_07 
| Tarefa do Fluxograma: [ Tarefa Analítica 7 ] 
| Título da tarefa: Identificar, qualitativamente, comunidades legislativas ideológicas 
           por meio da Ideologia partidária conhecida;  
+--------------------------------------------------------------------------------------------+
*/

/* Passo 1
==========
        O Deputado deve ser vinculado à linha ideológica do partido. 
        Para Deputados que sejam filiados a mais de um partido, o parlamentar será 
        vinculado ao partido pelo qual ele mais votou no exercício da atividade legislativa. 
        _TB_DEPU_PART_MAX é uma tabela gerada a partir de uma consulta que totaliza 
        os votos que um deputado deu para cada partido pelo qual esteve filiado e
        mantém apenas o partido pelo qual ele votou mais vezes. 
*/

DROP TABLE IF EXISTS _TB_DEPU_PART_MAX;
CREATE TABLE _TB_DEPU_PART_MAX AS
SELECT V.DEPUTADO_ID,V.DEPUTADO_URI,V.DEPUTADO_SIGLAPARTIDO,V.QUANTIDADE_DE_VOTOS
FROM (
    SELECT DEPUTADO_ID,DEPUTADO_URI,DEPUTADO_SIGLAPARTIDO, COUNT(*) AS QUANTIDADE_DE_VOTOS
    FROM TB_VOTO
    GROUP BY DEPUTADO_ID,DEPUTADO_URI,DEPUTADO_SIGLAPARTIDO,DEPUTADO_URI
) AS V
INNER JOIN (
    SELECT DEPUTADO_ID,MAX(QUANTIDADE_DE_VOTOS) AS MAX_VOTOS
    FROM (
        SELECT DEPUTADO_ID, DEPUTADO_SIGLAPARTIDO, COUNT(*) AS QUANTIDADE_DE_VOTOS
        FROM TB_VOTO
        GROUP BY DEPUTADO_ID,DEPUTADO_SIGLAPARTIDO,DEPUTADO_URI
    ) AS SUB
    GROUP BY DEPUTADO_ID
) AS M
ON V.DEPUTADO_ID = M.DEPUTADO_ID AND V.QUANTIDADE_DE_VOTOS = M.MAX_VOTOS;


/* Passo 2
==========
Após vincular a ideologia dos partidos aos deputados filiados, já  podemos criar os 
VÉRTICES DE REDE contendo o ID do Deputado, o nome, a sigla do partido e a ideologia partidária.
*/ 
DROP TABLE IF EXISTS TB_VERTICES;
CREATE TABLE TB_VERTICES AS
SELECT 
    PM.DEPUTADO_ID AS ID,
    D.NOME AS label,
    DEPUTADO_SIGLAPARTIDO AS Partido,
    DE_IDE AS Ideologia FROM 
_TB_DEPU_PART_MAX PM,
TB_PART P,
TB_DEPU D
WHERE 
    P.DE_SIG_PAR=PM.DEPUTADO_SIGLAPARTIDO AND
    PM.DEPUTADO_URI=D.URI;



/* Passo 3
==========
        Finalizadas as consultas, apagar as tabelas temporárias
*/
DROP TABLE IF EXISTS _TB_ARESTAS_TEMP;
DROP TABLE IF EXISTS _TB_DEPU_PART_MAX; 
DROP TABLE IF EXISTS _TB_QTD_VOTO;
DROP TABLE IF EXISTS _TB_VOTO;
 
