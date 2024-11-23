/* 
+--------------------------------------------------------------------------------------------+
|UNIVERSIDADE FEDERAL DE SANTA CATARINA - UFSC                                               |
+--------------------------------------------------------------------------------------------+
| Programa de P�s-Gradua��o em Ci�ncia da Informa��o                                         |
| Tese: CONEX�ES PARLAMENTARES: GRAFOS LEGISLATIVOS PARA IMPACTO DECISIVO EM PROJETOS DE LEI |                                                           |  
| Discente: Manoel Camilo de Sousa Netto                                                     |
| Orientador: Professor Doutor Adilson Luiz Pinto                                            |
+--------------------------------------------------------------------------------------------+
| Descri��o do C�digo: SQL de gera��o da Rede Social de Concord�ncias baseadas nos votos.    |
| O objetivo do Script SQL � obter uma rede na qual os deputados s�o interligados            |
| por arestas cujo peso significa a quantidade de concord�ncias entre esses dois             |
| parlamentares. Cada trecho de Script possui breve coment�rio acerca do que est� sendo      |
| executado.                                                                                 |   
| De forma resumida o script executa as seguintes tarefas:                                   |
|      - [ Tarefa Anal�tica 4 ]: Exclui vota��es n�o relevantes;                             |
|      - [ Tarefa Anal�tica 5 ]: Cria Rede de Concord�ncias;                                 |
|      - [ Tarefa Anal�tica 6 ]: Exclui as concord�ncias n�o duradouras (circunstanciais)    |
|      - [ Tarefa Anal�tica 7 ]: T�tulo da tarefa: Identificar, qualitativamente,            |
|                                comunidades legislativas ideol�gicas por meio da            |
|                                Ideologia partid�ria conhecida.                             |
|                                                                                            |
| Tempo aproximado de execu��o do script: 2min 44seg                                         |
| Hardware utilizado: Computador com processador MD Ryzen 5,2.10 GHz, Windows 11, 16 Gb RAM  |  
| Software: Sistema Operacional Windows 11, MySQL 5.7.44                                     |
+--------------------------------------------------------------------------------------------+
| Conven��es:                                                                                |
|     - As tabelas originais - advindas do site de fontes abertas da C�mara dos deputados    | 
|       adotam a nomenclatura iniciada com "TB_", enquanto as tabelas geradas pela           |
|       pesquisa iniciam-se com "_TB_".                                                      |
|     - As tabelas tempor�rias (usadas apenas para processamento intermedi�rio) possuem a    |
|       nomenclatura finalizada como sufixo "_TEMP".                                         |
|     - Cada trecho de script possui breve coment�rio acerca do que est� sendo executado.    |
|     - A vers�o integral deste e dos demais scripts podem ser baixados em:                  |
|       https://github.com/ManoelCamilo/tese                                                 |
+--------------------------------------------------------------------------------------------+
*/
 
/*
+--------------------------------------------------------------------------------------------+
| (SCRIPT_SQL.SECAO.SEM_NUMERO) T�tulo: Fixa��o do ano da Legislatura                                                
+--------------------------------------------------------------------------------------------+
*/
SET @ANO='2022';
SELECT @ANO;

/*
+--------------------------------------------------------------------------------------------+
| Refer�ncia: SCRIPT_SQL_TAREFA_04 
| Tarefa do Fluxograma: [ Tarefa Anal�tica 4 ] 
| T�tulo da tarefa: Exclui vota��es n�o relevantes
| Descri��o: vota��es relevantes, segundo a metodologia adotada, s�o aquelas com 
| qu�rum acima da mediana. Al�m disso, nessa se��o s�o executadas diversas tarefas meramente 
| operacionais, sem l�gica metodol�gica envolvida e que visam apenas otimiza��o de consultas, 
| tais como cria��o de �ndices, cria��o de chaves e convers�o de tipos.
+--------------------------------------------------------------------------------------------+
*/

/* Passo 1 - Otimiza��o de tabelas e totaliza��o de votos para calcular qu�rum
==========
    - Cria��o de uma chave prim�ria na tabela de vota��es (TB_VOTC) [Otimiza��o].
    - Cria��o de relacionamento (chave estrangeira) entre a tabela de votos (TB_VOTO) e  
      a tabela de vota��es (TB_VOTC) com objetivo de agilizar as consultas [Otimiza��o]. 
    - Para calcular o qu�rum � necess�rio totalizar os votos de cada vota��o. Assim, foi  
      criada a tabela �_TB_QTD_VOTO�  c/ a quantidade total de votos de cada uma delas. 
     
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
     (VOTO ='Sim' OR VOTO='N�o') AND 
     YEAR(VT.dataHoraVoto)=@ANO
 GROUP BY ID;


/* Passo 2 - C�lculo da mediana e exclus�o de vota��es n�o relevantes (qu�rum<=mediana)
==========
|    - C�lculo da Mediana de qu�rum 
|    - Exclus�o de vota��es n�o relevantes (Exclu�das se qu�rum <= mediana). 
|    - Cria uma tabela de votos �_TB_VOTO� contendo apenas os votos das vota��es relevantes
|      (ser� utilizada na gera��o das Arestas da Rede Social de Concord�ncia) 
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
          Exclui da tabela de totaliza��es de votos (_TB_QTD_VOTO) aqueles votos das vota��es 
          com qu�rum <= mediana dos qu�runs
*/
DELETE FROM _TB_QTD_VOTO WHERE PER_VOT<=@MEDIANA_QUORUM;


/* Passo 4
==========
          Cria uma tabela tempor�ria _TB_VOTO contendo os votos do ano fixado 
          no in�cio do script, bem como apenas os votos das vota��es relevantes
          ( _TB_QTD_VOTO cont�m apenas as vota��es relevantes)
*/
DROP TABLE IF EXISTS _TB_VOTO;
CREATE TABLE _TB_VOTO AS 
      SELECT * FROM TB_VOTO 
        WHERE YEAR(DATAHORAVOTO)=@ANO AND 
              IDVOTACAO IN (SELECT IDVOTACAO FROM _TB_QTD_VOTO);


/* Passo 5
==========
         Na tabela de votos, separando o ano da data de vota��o, armazenando-o isolado numa coluna do tipo 
         INTEIRO para agilizar consultas posteriores, pois as consultas usando tipo inteiro s�o mais r�pidas
         do que consultas que usam tipo VARCHAR (string)
*/
ALTER TABLE _TB_VOTO ADD COLUMN ANO INT;
UPDATE _TB_VOTO SET ANO= YEAR(DATAHORAVOTO);

/* Passo 6
==========
         Obtendo o Identificador �nico do Deputado (DEPUTADO_ID, do tipo VARCHAR) e armazenando o valor
         dele numa nova coluna criada do tipo INTEIRO (DEPUTADO_ID_INT INT) para otimizar consultas 
         futuras, pois as consultas usando tipo inteiro s�o mais r�pidas do que consultas que usam tipo 
         VARCHAR (string)
*/
ALTER TABLE _TB_VOTO ADD COLUMN DEPUTADO_ID_INT INT;
UPDATE _TB_VOTO SET DEPUTADO_ID_INT = DEPUTADO_ID; 

/* Passo 7
==========
         Convertendo o ID da vota��o (IDVOTACAO, tipo VARCHAR)e armazenando o valor dele numa 
         nova coluna criada do tipo BIG INTEGER, pois as consultas usando tipo inteiro s�o mais
         r�pidas do que consultas que usam tipo VARCHAR (string). Nessa convers�o foram retirados 
         os caracteres de h�fen ("-") que separavam o d�gito verificador.
*/
ALTER TABLE _TB_VOTO ADD COLUMN IDVOTACAO_INT BIGINT;
UPDATE _TB_VOTO SET IDVOTACAO_INT = REPLACE(IDVOTACAO,"-","");

/* Passo 8
==========
         Excluindo os votos dos tipos absten��es e Obstru��es - tratadas como ru�do por serem 
         um universo insignificante do total. Em 2022 foram 2.919 ocorr�ncias de 194.316 (0,015% do total).
         Foram mantidos apenas os votos reais - ou seja, aqueles com valor "Sim" (aprovar) ou "N�o" (rejeitar).
*/
DELETE FROM _TB_VOTO WHERE (VOTO<>'Sim' AND VOTO<>'N�o');

/* Passo 9
==========
         Transformando os votos de texto (VOTO, tipo VARCHAR) em inteiros. 
         "Sim" passou a ser 1 (um) e "N�o" passa a ser 0 (zero), ambos armazenados 
         em uma novas coluna do tipo Inteiro (VOTO_INT). Consultas usando "joins" 
         entre tabelas que comparam tipos inteiros s�o mais r�pidas do que consultas 
         que comparam textos.
*/
ALTER TABLE _TB_VOTO ADD COLUMN VOTO_INT INT;
UPDATE _TB_VOTO SET VOTO_INT=1 WHERE VOTO='Sim';
UPDATE _TB_VOTO SET VOTO_INT=0 WHERE VOTO='N�o';

/* Passo 10
===========
         Consultas futuras exigir�o �JOINS� entre colunas dos identificadores de vota��o e de Deputado. Foram criados �ndices envolvendo essas colunas para otimizar o tempo de execu��o.
*/
CREATE INDEX IDX_DEP_VTC ON _TB_VOTO(DEPUTADO_ID_INT,IDVOTACAO_INT);
CREATE INDEX IDX_VTC_DEP ON _TB_VOTO(IDVOTACAO_INT,DEPUTADO_ID_INT);

/*
+--------------------------------------------------------------------------------------------+
| Refer�ncia: SCRIPT_SQL_TAREFA_05 
| Tarefa do Fluxograma: [ Tarefa Anal�tica 5 ] 
| T�tulo da tarefa: Criar Rede de Concord�ncias
| Descri��o: Cria��o da Rede de concord�ncias � A rede de concord�ncias � uma tabela de    
|       arestas de peso(weight) que unem deputados pela quantidade de votos iguais para a 
|       mesma vota��o relevante
+--------------------------------------------------------------------------------------------+
*/


/* Passo 1
==========
        Cria��o da tabela tempor�ria �_TB_ARESTAS_TEMP� para   
        armazenamento de processamentos intermedi�rios. A consulta faz um �auto� JOIN entre a 
        tabela de votos  ("_TB_VOTO") e ela mesma para contabilizar, para cada par de deputados 
        diferentes numa mesma vota��o, a quantidade os votos concordantes (Na mesma vota��o 
        votaram da mesma forma). No final a rede conter� os campos "source" (n� origem),
        "target" (n� destino) e weight (peso da aresta = quantidade de votos iguais)
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
A.IDVOTACAO_INT=B.IDVOTACAO_INT AND /* QUE PARTICIPARAM DA MESMA VOTA��O */
A.VOTO_INT=B.VOTO_INT /* E QUE VOTARAM DA MESMA FORMA */
GROUP BY
A.DEPUTADO_ID_INT, B.DEPUTADO_ID_INT;



/* Passo 2
==========
        Considerando que, na Rede Social de concord�ncias a ser criada, as arestas n�o 
        s�o direcionadas, ent�o podemos afirmar que, para quaisquer arestas que una dois 
        v�rtices(deputados) X e Y, a aresta (DEPUTADO X,DEPUTADO Y) � a mesma aresta 
        que (DEPUTADO Y, DEPUTADO X).Por essa raz�o, h� necessidade de excluir as arestas 
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
        Para otimizar a tabela de arestas foi criado �ndice nos identificadores dos v�rtices
*/

CREATE INDEX IDX_TB_ARESTAS ON TB_ARESTAS(SOURCE,TARGET);


/*
+--------------------------------------------------------------------------------------------+
| Refer�ncia: SCRIPT_SQL_TAREFA_06 
| Tarefa do Fluxograma: [ Tarefa Anal�tica 6 ] 
| T�tulo da tarefa: Excluir as concord�ncias n�o duradouras (circunstanciais)
| T�tulo: Para possibilitar que apenas as concord�ncias mais proeminentes da Rede sejam  
|        evidenciadas, as concord�ncias menos relevantes foram removidas. Para tanto, foi 
|        calculada a mediana das concord�ncias (que na rede � expressa peso num�rico das 
|        arestas). A arestas com pesos menores ou iguais � mediana dos pesos foram exclu�das.
+--------------------------------------------------------------------------------------------+
*/


/* Passo 1
==========
        C�lculo da mediana
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
        Exclus�o das concord�ncias transit�rias - assim consideradas pela pesquisa
        aquelas cuja quantidade de concord�ncias seja menor ou igual a mediana
*/
DELETE FROM TB_ARESTAS WHERE WEIGHT<=@MEDIANA_CONCORDANCIA;



/*
/*
+--------------------------------------------------------------------------------------------+
| Refer�ncia: SCRIPT_SQL_TAREFA_07 
| Tarefa do Fluxograma: [ Tarefa Anal�tica 7 ] 
| T�tulo da tarefa: Identificar, qualitativamente, comunidades legislativas ideol�gicas 
           por meio da Ideologia partid�ria conhecida;  
+--------------------------------------------------------------------------------------------+
*/

/* Passo 1
==========
        O Deputado deve ser vinculado � linha ideol�gica do partido. 
        Para Deputados que sejam filiados a mais de um partido, o parlamentar ser� 
        vinculado ao partido pelo qual ele mais votou no exerc�cio da atividade legislativa. 
        _TB_DEPU_PART_MAX � uma tabela gerada a partir de uma consulta que totaliza 
        os votos que um deputado deu para cada partido pelo qual esteve filiado e
        mant�m apenas o partido pelo qual ele votou mais vezes. 
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
Ap�s vincular a ideologia dos partidos aos deputados filiados, j�  podemos criar os 
V�RTICES DE REDE contendo o ID do Deputado, o nome, a sigla do partido e a ideologia partid�ria.
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
        Finalizadas as consultas, apagar as tabelas tempor�rias
*/
DROP TABLE IF EXISTS _TB_ARESTAS_TEMP;
DROP TABLE IF EXISTS _TB_DEPU_PART_MAX; 
DROP TABLE IF EXISTS _TB_QTD_VOTO;
DROP TABLE IF EXISTS _TB_VOTO;
 
