import os
import mysql.connector
import pandas as pd
import time
import warnings
import networkx as nx  # pip install networkx
import community as community_louvain  # pip install python-louvain
import requests
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import re
from collections import Counter
import random

warnings.filterwarnings("ignore")

def obter_configuracao(nome_configuracao):
    """
    Lê o valor de uma configuração diretamente de um arquivo CONFIG.INI usando expressões regulares.
    
    Args:
        nome_configuracao (str): Nome da configuração a ser lida.
    
    Returns:
        str: Valor da configuração especificada.
    
    Raises:
        FileNotFoundError: Se o arquivo CONFIG.INI não for encontrado.
        ValueError: Se a configuração não for encontrada no arquivo.
    """
    caminho_arquivo = "CONFIG.INI"  # Nome fixo do arquivo de configurações
    
    # Tenta abrir e ler o arquivo
    try:
        with open(caminho_arquivo, "r") as arquivo:
            conteudo = arquivo.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo de configurações '{caminho_arquivo}' não encontrado.")
    
    # Usa expressão regular para encontrar a configuração no formato NOME=VALOR
    padrao = rf"^{nome_configuracao}=(.+)$"
    match = re.search(padrao, conteudo, re.MULTILINE)
    
    if match:
        return match.group(1).strip()
    else:
        raise ValueError(f"Configuração '{nome_configuracao}' não encontrada no arquivo.")

# Exemplo de uso

# Configurações do servidor MySQL
#MYSQL_HOST = 'localhost'
#MYSQL_USER = 'root'
#MYSQL_PASSWORD = 'Secrel01'

MYSQL_HOST = obter_configuracao("ENDERECO_SERVIDOR_MYSQL")
MYSQL_USER = obter_configuracao("USUARIO_SERVIDOR_MYSQL")
MYSQL_PASSWORD = obter_configuracao("SENHA_SERVIDOR_MYSQL")

print(MYSQL_HOST)
print(MYSQL_USER)
print(MYSQL_PASSWORD)

G = nx.Graph()

#Função para exibir uma caixa de mensagem - se for o caso especificar a cor da mensagem: vermelho, verde, amarelo ou branco
def criar_caixa_ascii(lista, cor='branco'):
    # Definir os códigos de cores ANSI
    cores = {
        'vermelho': '\033[91m',
        'verde': '\033[92m',
        'amarelo': '\033[93m',
        'branco': '\033[97m'
    }
    
    # Determinar a cor selecionada, negrito e reset de cor
    cor_selecionada = cores.get(cor, '\033[97m')
    negrito = '\033[1m'
    reset_cor = '\033[0m'
    
    # Determinar o maior comprimento da linha
    max_len = max(len(linha) for linha in lista)
    
    # Definir o tamanho da caixa, adicionando margem
    largura = max_len + 4  # Mais 4 para as bordas e espaços
    altura = len(lista) + 2  # Mais 2 para a linha superior e inferior da caixa
    
    # Criar a borda superior
    print(cor_selecionada + '╔' + '═' * (largura - 2) + '╗')
    
    # Exibir o título centralizado na parte superior da janela com negrito
    titulo = lista[0]
    print('║ ' + negrito + titulo.center(largura - 4) + reset_cor + cor_selecionada + ' ║')
    print('╠' + '═' * (largura - 2) + '╣')
    
    # Exibir as linhas restantes centralizadas
    for linha in lista[1:]:
        print('║ ' + linha.center(largura - 4) + ' ║')
    
    # Criar a borda inferior
    print('╚' + '═' * (largura - 2) + '╝' + reset_cor)



# Conectando ao servidor MySQL
def connect_mysql():
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Erro: {err}")
        return None

#================================================================================================================
# [ Referência: SCRIPT_PYTHON_TAREFA_01 ]
# Tarefa do Fluxograma: [ Tarefa Operacional 1 ] 
# Título da tarefa: Criar Banco de Dados Relacional (MySQL)
# Descrição: Cria um banco de dados no Software MySQL Server
#================================================================================================================        
def create_database(connection, db_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        criar_caixa_ascii(["Criação do banco de dados","O banco de dados foi criado com sucesso \
        no servidor Mysql 127.0.0.1 (localhost)..."])
    except mysql.connector.Error as err:
        print(f"Erro ao criar o banco de dados: {err}")
    cursor.close()

#================================================================================================================
# [ Referência: SCRIPT_PYTHON_TAREFA_02 ]
# Tarefa do Fluxograma: [ Tarefa Operacional 2 ] 
# Titulo da Tarefa: Realizar download das planilhas nos Dados Abertos da Câmara dos Deputados
# Descrição: Importar planilhas para o Banco de Dados recém-criado. Cada arquivo ".xlsx" gera uma única tabela
# Existe um mapeamento no arquivo "renomear.csv" onde a cada arquivo baixado há um nome de tabela a ser criado
# no banco de dados e uma descrição do conteudo, conforme abaixo:
# url,nome_banco_dados,descricao
#           https://dadosabertos.camara.leg.br/arquivos/deputados/xlsx/deputados.xlsx,TB_DEPU, Tabela de DEPUTADOS
#           https://dadosabertos.camara.leg.br/arquivos/proposicoes/xlsx/proposicoes-2022.xlsx,TB_PROP, Tabela de PROPOSIÇÕES
#           https://dadosabertos.camara.leg.br/arquivos/votacoes/xlsx/votacoes-2022.xlsx,TB_VOTC,Tabela de VOTAÇÕES
#           https://dadosabertos.camara.leg.br/arquivos/votacoesVotos/xlsx/votacoesVotos-2022.xlsx,TB_VOTO, Tabela dos VOTOS
#           https://github.com/ManoelCamilo/tese/raw/main/TB_PART.xlsx,TB_PART,Tabela dos PARTIDOS
#================================================================================================================
def baixar_arquivos_de_dados():
    rename_dict = {}
    
    # Verifica se o arquivo 'renomear.csv' existe
    if os.path.exists('renomear.csv'):
        # Lê o arquivo CSV
        df_rename = pd.read_csv('renomear.csv')
        
        # Baixa os arquivos listados na coluna 'url'
        for index, row in df_rename.iterrows():
            url = row['url']
            descricao = row['descricao']
            nome_banco_dados = row['nome_banco_dados']
            
            # Extrai o nome do arquivo a partir da URL
            filename = os.path.basename(url)
            
            # Faz o download do arquivo
            try:
                response = requests.get(url)
                response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
                
                # Salva o arquivo na mesma pasta do script
                with open(filename, 'wb') as file:
                    file.write(response.content)
                criar_caixa_ascii([f'DOWNLOAD DE ARQUIVO - {descricao}',f'Arquivo {filename} baixado com sucesso.({url})'])
                # Adiciona a entrada ao dicionário de renomeação
                rename_dict[filename] = nome_banco_dados
            except Exception as e:
                print(f"Erro ao baixar {url}: {e}")
    
    return rename_dict

#================================================================================================================
# [ Referência: SCRIPT_PYTHON_TAREFA_03 ]
# Tarefa do Fluxograma: [ Tarefa Operacional 3 ] 
# Título da tarefa: Importar planilhas para o Banco de Dados recém-criado. Cada arquivo "".xlsx" gera uma única tabela
# Descrição: Insere os registros contidos nas planilhas baixadas em tabelas do banco de dados Relacional MySQL.
#================================================================================================================  
def cria_tabelas_e_insere_dados_dos_arquivos(connection, db_name, rename_dict):
    cursor = connection.cursor()
    cursor.execute(f"USE {db_name}")

    # Itera sobre todos os arquivos .xlsx na pasta atual
    for file in os.listdir('.'):
        #não carrega arquivos excel iniciados com "REDE" - para não carregar as planilhas de vértices e arestas geradas pelo próprio programa
        if file.endswith('.xlsx') and not ("REDE" in str(file)):
            
            # Verifica se o nome do arquivo está no dicionário de renomeações
            table_name = rename_dict.get(file, os.path.splitext(file)[0])

            # Carregar a planilha em um DataFrame
            df = pd.read_excel(file)
            
            # Determinar o tamanho necessário para cada campo
            columns = []


                  
            for col in df.columns:
                max_len = df[col].astype(str).map(len).max()
                max_len = max(1, max_len)  # Garante que o tamanho mínimo seja 1
                columns.append(f"`{col}` VARCHAR({max_len})")

            # Criar a tabela
            create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            cursor.execute(create_table_query)
            criar_caixa_ascii(["CRIAÇÃO DE TABELAS NO BANCO DE DADOS",f"Tabela '{table_name}' criada com sucesso."])
          

            # Inserir dados na tabela
            for _, row in df.iterrows():
                insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
                cursor.execute(insert_query, tuple(row))
            
            connection.commit()

            # Contagem de linhas
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            table_row_count = cursor.fetchone()[0]
            criar_caixa_ascii(["Inserção de dados em tabela",f"Tabela '{table_name}' recebeu {table_row_count} registros (Planilha excel original da Câmara dos Deputados possui: {len(df)} linhas)."])
            print()

    cursor.close()

# Executar scripts SQL
def execute_sql_scripts(connection, db_name):
    cursor = connection.cursor()
    cursor.execute(f"USE {db_name}")

    for file in os.listdir('.'):
        if file.endswith('.sql'):
            with open(file, 'r') as sql_file:
                sql_script = sql_file.read()
                try:
                    # Executa múltiplas instruções SQL
                    criar_caixa_ascii(["Executando script SQL contendo [ Tarefa 04 ],[ Tarefa 05 ] [ Tarefa 06 ] e [ Tarefa 07 ] do fluxograma da metodologia",f'Executando script '+str(file)+'...'])
                    for result in cursor.execute(sql_script, multi=True):
                        if result.with_rows:
                            result.fetchall()  # Consumir todos os resultados para evitar o erro "Unread result found"
                    connection.commit()
                except mysql.connector.Error as err:
                    print(f"Erro ao executar script '{file}': {err}")

    cursor.close()



# Função que obtém uma rede que está armazenada em uma tabela do banco de dados
def obter_rede(conn, nome_banco, nome_tabela,G):
    # Monta a consulta SQL usando o nome do banco de dados e da tabela fornecidos
    query = f"""
        SELECT *
        FROM {nome_banco}.{nome_tabela}    """
    
    # Executa a consulta e armazena o resultado em um DataFrame
    df = pd.read_sql(query, conn)

    # Adiciona as arestas à rede a partir do DataFrame
    for _, row in df.iterrows():
        G.add_edge(row['SOURCE'], row['TARGET'], weight=row['WEIGHT'])

    #adicionar atributos dos Vértices - partido, ideologia e nome do deputado
    # Criar cursor para a conexão MySQL

    cursor = conn.cursor(dictionary=True)

    # Consultar todos os registros da tabela
    query = f"SELECT * FROM BD_TESE.TB_VERTICES;"
    cursor.execute(query)

    # Buscar os resultados da tabela
    registros = cursor.fetchall()

    # Iterar sobre os registros e adicionar os atributos correspondentes aos nós da rede
    for registro in registros:
        node_id = registro['ID']  # A coluna "ID" da tabela
        # Converter node_id para o mesmo tipo dos nós da rede (assumindo que o tipo do primeiro nó é representativo)
        sample_node = next(iter(G.nodes))  # Pega um nó de exemplo para saber o tipo
        node_id = type(sample_node)(node_id)  # Converte o node_id para o tipo do nó da rede

        # Verificar se o vértice com o mesmo ID está presente na rede
        if G.has_node(node_id):
            # Remover a coluna 'ID' para não adicioná-la como atributo
            atributos = {k: v for k, v in registro.items() if k != 'ID'}            
            # Atualizar o nó com os atributos restantes da tabela
            nx.set_node_attributes(G, {node_id: atributos})


 #================================================================================================================
 # [ Referência: SCRIPT_PYTHON_TAREFA_08 ]
 # Tarefa do Fluxograma: [ Tarefa Analítica 8 ] 
 # Título da tarefa: Identificar, quantitativamente, comunidades por meio de métricas decorrentes da concordância 
 #                   duradoura e não circunstancial: descoberta de comunidades por meio da maximização da modularidade
 # Descrição: Executa o algoritmo louvain até maximizar a modularidade a atribuir uma comunidade a cada vértice.
 # O script ora executado traz a rede do banco de dados, executa o louvain para maximizar a modularidade e atribui
 # uma comunidade a cada vértice. Ao final cada um dos vértices estará alocado em um grupo (comunidade ou cluster)
 #================================================================================================================   
def cria_comunidades_modularidade(G):

    # Define o valor de resolução (padrão é 1.0, ajuste se necessário)
    resolucao = 1.0  # Ajuste isso para diferentes níveis de granularidade, a resolução padrão é 1 (um)

    particao = community_louvain.best_partition(G, resolution=resolucao)
    nx.set_node_attributes(G, particao, "modularidade")

    criar_caixa_ascii(['Densidade Total da Rede','A densidade total da rede é '+ str(calcular_densidade_ponderada(G,""))])

    modularidades_unicas = set(nx.get_node_attributes(G, 'modularidade').values())

    #imprimir_vertices_com_atributos(G)

    # Laço para percorrer cada modularidade única
    for modularidade in modularidades_unicas:
        densidade_ponderada=str(calcular_densidade_ponderada(G,str(modularidade)))
        criar_caixa_ascii(["Processando comunidade - Cálculo de Densidade: ",f'Módulo {modularidade}...','Densidade encontrada:'+ densidade_ponderada])

#================================================================================================================
# [ Referência: SCRIPT_PYTHON_TAREFA_09 ]
# Tarefa do Fluxograma: [ Tarefa Analítica 9 ] 
# Título da tarefa: Identificar, quantitativamente, comunidades por meio de métricas decorrentes da concordância 
#                   duradoura e não circunstancial: descoberta de comunidades por meio da maximização da modularidade
# Descrição: Executa o algoritmo louvain até maximizar a modularidade a atribuir uma comunidade a cada vértice.
# O script ora executado traz a rede do banco de dados, executa o louvain para maximizar a modularidade e atribui
# uma comunidade a cada vértice. Ao final cada um dos vértices estará alocado em um grupo (comunidade ou cluster)
#================================================================================================================ 
def calcular_centralidade_ponte(G):
    # Calcular a centralidade de intermediação ponderada
    centralidade_intermediacao = nx.betweenness_centrality(G, weight='weight')
    
    # Calculando o coeficiente de ponte para cada nó e salvando os atributos nos nós do grafo
    for node in G.nodes:
        neighbors = list(G.neighbors(node))
        degree_weighted = sum(G[node][neighbor]['weight'] for neighbor in neighbors)  # Grau ponderado do nó
        
        # Coeficiente de ponte ponderado
        coef_ponte = 0
        for neighbor in neighbors:
            neighbor_degree_weighted = sum(G[neighbor][n]['weight'] for n in G.neighbors(neighbor))  # Grau ponderado do vizinho
            if neighbor_degree_weighted > 0:
                coef_ponte += 1 / neighbor_degree_weighted  # Somando a contribuição ponderada
        
        # Calculando a centralidade de ponte e salvando no nó
        centralidade_ponte = centralidade_intermediacao[node] * coef_ponte 
        G.nodes[node]['Centralidade_Intermediacao'] = centralidade_intermediacao[node]
        G.nodes[node]['coef_ponte'] = coef_ponte 
        G.nodes[node]['Centralidade_Ponte'] = centralidade_ponte
    
    # Atualiza a lista de nós com todos os atributos para garantir que 'Centralidade_Ponte' está incluída
    nodes_data = list(G.nodes(data=True))
    nodes_list = [{**attributes, 'ID': node} for node, attributes in nodes_data]
    nodes_df = pd.DataFrame(nodes_list)  # Recria o DataFrame com todos os atributos dos nós

  
    # Cálculo dos quartis e dos limites de outliers
    Q1 = nodes_df['Centralidade_Ponte'].quantile(0.25)
    Q2 = nodes_df['Centralidade_Ponte'].quantile(0.5)
    Q3 = nodes_df['Centralidade_Ponte'].quantile(0.75)
    IQR = Q3 - Q1

    limite_inferior_outlier = Q1 - 1.5 * IQR
    limite_superior_outlier = Q3 + 1.5 * IQR

    criar_caixa_ascii(['Quartis da Centralidade de Ponte dos vértices de rede - Deputados dialogantes', \
                      f"Limite superior do 1º quartil (Q1): {Q1}", \
                      f"Limite superior do 2º quartil (Q2): {Q2}", \
                      f"Limite superior do 3º quartil (Q3): {Q3}", \
                      f"Limite inferior para ser considerado outlier: {limite_inferior_outlier}", \
                      f"Limite superior para ser considerado outlier: {limite_superior_outlier}"],
                      cor='amarelo')

    # Adiciona as colunas 'Quartil' e 'Outlier' com valores "SS", "SI" ou "N"
    nodes_df['Quartil'] = pd.cut(nodes_df['Centralidade_Ponte'],
                                 bins=[-float('inf'), Q1, Q2, Q3, float('inf')],
                                 labels=['1', '2', '3', '4'])

    nodes_df['Outlier'] = nodes_df['Centralidade_Ponte'].apply(
        lambda x: 'SI' if x < limite_inferior_outlier else ('SS' if x > limite_superior_outlier else 'N')
    )

    # Contagem de ocorrências em cada quartil
    contagem_quartil = nodes_df['Quartil'].value_counts().sort_index()
    print("\nContagem de ocorrências em cada quartil:")
    for quartil, count in contagem_quartil.items():
        print(f"Quartil {quartil}: {count} ocorrências")

    # Contagem de outliers superiores e inferiores
    outliers_superiores = nodes_df[nodes_df['Outlier'] == 'SS'].shape[0]
    outliers_inferiores = nodes_df[nodes_df['Outlier'] == 'SI'].shape[0]

    print(f"\nQuantidade de outliers superiores (SS): {outliers_superiores}")
    print(f"Quantidade de outliers inferiores (SI): {outliers_inferiores}")

    # Exibe o DataFrame atualizado
    print("\nLista resumida de vértices, seus dados e suas  e métricas calculadas:")
    print(nodes_df)
    
    # Salva o DataFrame com os nós e seus atributos no arquivo Excel
    nodes_df.to_excel('REDE_VERTICES.xlsx', index=False)  # Salva os dados dos nós em um arquivo Excel no formato .xlsx

    # Obtenção das arestas com os dados
    edges_data = list(G.edges(data=True))  # Obtém a lista de arestas com seus atributos
    edges_list = []
    for u, v, attributes in edges_data:
        attributes['Origem'] = u  # Adiciona o nó de origem
        attributes['Destino'] = v  # Adiciona o nó de destino
        edges_list.append(attributes)  # Adiciona os atributos da aresta na lista


    # Tenta criar o DataFrame a partir dos atributos das arestas
    try:
        edges_df = pd.DataFrame(edges_list)
        # Salva o DataFrame com as arestas e seus atributos no arquivo Excel
        edges_df.to_excel('REDE_ARESTAS.xlsx', index=False)
    except Exception as e:
        print("Erro ao criar o DataFrame edges_df:", e)


#================================================================================================================
# [ Referência: SCRIPT_PYTHON_TAREFA_10 ]
# Tarefa do Fluxograma: [ Tarefa Operacional 10 ] 
# Título da tarefa: Salvar a rede em formatos GEFX, bem como em formato excel (um arquivo de vértices e um 
# arquivo de arestas).
# Descrição: Salva a rede como um arquivo .GEFX (Graph Exchange XML Format) que é uma linguagem para descrever 
# estruturas de redes complexas, seus dados associados e dinâmicas.
#================================================================================================================                     
def salvar_Rede(G):  
    # Nome do arquivo de saída com base no nome do banco e da tabela
    arquivo_gexf ="REDE_CONCORDANCIA.gexf" 
    # Exporta a rede para um arquivo GEXF
    nx.write_gexf(G, arquivo_gexf)
    criar_caixa_ascii(['Rede criada com sucesso',f'A Rede de concordâncias foi salva no arquivo {arquivo_gexf}','O arquivo pode ser aberto em qualquer software de análise de redes que suporte o formato GEFX',f"Quantidade de nós: {G.number_of_nodes()}",f"Quantidade de arestas: {G.number_of_edges()}"])
    #criar_caixa_ascii(['Salvar rede (vértices e arestas) em formato EXCEL',f'A Rede de concordâncias foi salva em dois arquivos do Excel: REDE-ARESTAS.XLS e REDE-VERTICES.XLS'])

def calcular_densidade_ponderada(G, modularidade=""):
    """
    Calcula a densidade ponderada de uma rede.

    Parâmetros:
    G - a rede (grafo) no formato NetworkX.
    modularidade - string representando o módulo. Se vazia, calcula para a rede toda.
                   Se numérica (ex.: "0", "1"), calcula apenas para o módulo indicado.

    Retorna:
    A densidade ponderada da rede ou do módulo específico.
    """
    
    if modularidade == "":  # Se modularidade for string vazia, calcula para a rede toda
        total_weight = G.size(weight='weight')  # Soma dos pesos das arestas
        num_nodes = G.number_of_nodes()
        max_possible_edges = num_nodes * (num_nodes - 1) / 2  # Número máximo de arestas possível (grafo não direcionado)
        densidade_ponderada = total_weight / max_possible_edges if max_possible_edges > 0 else 0
    else:
        # Se modularidade é numérica, filtra os nós que pertencem ao módulo especificado
        module_nodes = [n for n, data in G.nodes(data=True) if 'modularidade' in data and str(data['modularidade']) == modularidade]
        
        # Cria subgrafo contendo apenas os nós do módulo
        subgraph = G.subgraph(module_nodes)
        total_weight = subgraph.size(weight='weight')  # Soma dos pesos das arestas do subgrafo
        num_nodes = subgraph.number_of_nodes()
        max_possible_edges = num_nodes * (num_nodes - 1) / 2  # Número máximo de arestas do módulo
        densidade_ponderada = total_weight / max_possible_edges if max_possible_edges > 0 else 0
    
    return densidade_ponderada

def exibir_tabela_distribuicao(grafo, campo):
    """
    Exibe uma tabela com a quantidade de vértices para cada valor diferente do campo informado.

    Parâmetros:
    grafo (networkx.Graph): A rede a ser analisada.
    campo (str): O nome do campo dos vértices.
    """
    from collections import Counter

    # Obtém os valores do campo para cada nó
    valores_campo = [grafo.nodes[n].get(campo, None) for n in grafo.nodes]

    # Conta a quantidade de vértices para cada valor do campo
    distribuicao = Counter(valores_campo)

    # Define o cabeçalho
    cabecalho1 = f"Valor do {campo}"
    cabecalho2 = "Quantidade de vértices"

    # Determina as larguras das colunas
    largura_coluna1 = max(len(cabecalho1), max((len(str(v)) for v in distribuicao.keys()), default=0))
    largura_coluna2 = max(len(cabecalho2), max((len(str(c)) for c in distribuicao.values()), default=0))

    # Adiciona espaço extra para clareza visual
    largura_coluna1 += 2
    largura_coluna2 += 2

    # Construção das bordas
    linha_horizontal_topo = f"╔{'═' * largura_coluna1}╦{'═' * largura_coluna2}╗"
    linha_horizontal_meio = f"╠{'═' * largura_coluna1}╬{'═' * largura_coluna2}╣"
    linha_horizontal_rodape = f"╚{'═' * largura_coluna1}╩{'═' * largura_coluna2}╝"

    # Construção do cabeçalho
    cabecalho = (
        f"║ {cabecalho1:<{largura_coluna1 - 2}} ║ {cabecalho2:<{largura_coluna2 - 2}} ║"
    )

    # Construção das linhas da tabela
    linhas = [
        f"║ {str(valor):<{largura_coluna1 - 2}} ║ {quantidade:>{largura_coluna2 - 2}} ║"
        for valor, quantidade in distribuicao.items()
    ]

    # Exibição da tabela
    print(linha_horizontal_topo)
    print(cabecalho)
    print(linha_horizontal_meio)
    for linha in linhas:
        print(linha)
    print(linha_horizontal_rodape)

# Exemplo de uso:
# exibir_tabela_distribuicao(grafo, 'modularidade')


def comparar_percentuais(grafo, campo1, campo2):
    """
    Compara os percentuais entre dois campos dos vértices de uma rede e exibe os maiores percentuais,
    incluindo as quantidades absolutas.

    Parâmetros:
    grafo (networkx.Graph): A rede a ser analisada.
    campo1 (str): O nome do primeiro campo dos vértices (referência principal).
    campo2 (str): O nome do segundo campo dos vértices (para comparar com o campo1).
    """
    from collections import Counter

    # Extrai os valores dos dois campos para cada nó
    valores_campo1 = [grafo.nodes[n].get(campo1) for n in grafo.nodes]
    valores_campo2 = [grafo.nodes[n].get(campo2) for n in grafo.nodes]

    # Conta a frequência de cada valor único no campo1
    distribuicao_campo1 = Counter(valores_campo1)

    # Organiza os dados por campo1 e campo2
    comparacao = {}
    for valor_campo1 in distribuicao_campo1.keys():
        # Filtra os nós que possuem o valor_campo1
        valores_correspondentes_campo2 = [
            valores_campo2[i]
            for i, v in enumerate(valores_campo1)
            if v == valor_campo1
        ]

        # Conta as ocorrências no campo2 para o valor atual do campo1
        distribuicao_campo2 = Counter(valores_correspondentes_campo2)
        total_campo1 = distribuicao_campo1[valor_campo1]

        # Calcula os percentuais para cada valor do campo2
        percentuais = {
            valor_campo2: (contagem / total_campo1) * 100
            for valor_campo2, contagem in distribuicao_campo2.items()
        }

        # Determina o maior percentual
        maior_percentual = max(percentuais.items(), key=lambda x: x[1])
        valor_campo2, percentual = maior_percentual
        quantidade = distribuicao_campo2[valor_campo2]
        comparacao[valor_campo1] = (valor_campo2, percentual, quantidade, total_campo1)

    # Exibe o resultado
    print("Comparação dos maiores percentuais:")
    for valor_campo1, (valor_campo2, percentual, quantidade, total) in comparacao.items():
        print(
            f"- {percentual:.2f}% dos casos parlamentares com {campo1} partidária com valor '{valor_campo1}' "
            f"({quantidade} dos {total} totais) possuem {campo2} igual a '{valor_campo2}'"
        )

# Exemplo de uso:
# comparar_percentuais(grafo, "ideologia", "modularidade")



def plot_boxplot(graph, value_field, color_field, custom_colors=None):
    # Definir a taxa de jittering
    jitter_rate = 0.09

    # Número de vértices
    numero_vertices = graph.number_of_nodes()

    # Extrair os valores do campo especificado nos nós do grafo
    values = [data[value_field] for node, data in graph.nodes(data=True)]
    colors = [data[color_field] for node, data in graph.nodes(data=True)]

    # Criar uma lista de valores únicos para atribuir cores diferentes
    unique_colors = list(set(colors))

    # Se custom_colors for fornecido e tiver cores suficientes, use-as
    if custom_colors and len(custom_colors) >= len(unique_colors):
        color_palette = custom_colors[:len(unique_colors)]
    else:
        # Caso contrário, use a paleta "husl" e adicione cores extras se necessário
        color_palette = custom_colors if custom_colors else []
        remaining_colors = len(unique_colors) - len(color_palette)
        color_palette += sns.color_palette("husl", remaining_colors)

    # Inverter a ordem das cores na paleta
    color_palette.reverse()

    # Criar um dicionário para mapear cada valor único a uma cor
    color_mapping = {val: color_palette[i] for i, val in enumerate(unique_colors)}

    # Ajustar o tamanho da figura
    plt.figure(figsize=(6, 8))

    # Criar o boxplot
    sns.boxplot(y=values, color='lightgray', showfliers=False)

    # Plotar cada ponto com jittering e cores diferentes
    for i, val in enumerate(values):
        plt.scatter(x=np.random.uniform(-jitter_rate, jitter_rate), y=val, 
                    color=color_mapping[colors[i]], alpha=0.7)

    # Ajustar a escala do eixo y para logarítmica
    plt.yscale('log')

    # Adicionar a legenda
    legend_elements = []
    for val in unique_colors:
        count = colors.count(val)
        percent = (count / numero_vertices) * 100  # Cálculo do percentual
        legend_elements.append(
            plt.Line2D([0], [0], marker='o', color='w', 
                       markerfacecolor=color_mapping[val], 
                       markersize=10, 
                       label=f'Comunidade {val}: {count} ({percent:.2f}%)'))

    plt.legend(handles=legend_elements, title=color_field.capitalize())

    # Adicionar rótulo ao eixo y
    plt.ylabel(value_field.replace('_', ' ').capitalize() + ' (Escala Logarítmica)')

    # Mostrar o gráfico
    plt.show()

def plotar_grafico_donut(grafo, nome_atributo, cores_personalizadas=None):
    """
    Gera um gráfico do tipo "donut" a partir dos valores de um atributo dos vértices de uma rede NetworkX.
    
    Args:
        grafo (networkx.Graph): Rede do tipo NetworkX.
        nome_atributo (str): Nome do atributo dos vértices para análise.
        cores_personalizadas (list, opcional): Lista de cores a serem usadas no gráfico.
    
    Raises:
        ValueError: Se o atributo não for encontrado nos nós.
    """
    # Obtém os valores do atributo para todos os nós
    atributos = nx.get_node_attributes(grafo, nome_atributo)
    
    if not atributos:
        raise ValueError(f"O atributo '{nome_atributo}' não foi encontrado nos vértices da rede.")
    
    # Conta a ocorrência de cada categoria
    contagem = Counter(atributos.values())
    categorias = list(contagem.keys())
    valores = list(contagem.values())
    
    # Calcula os percentuais
    total = sum(valores)
    percentuais = [v / total * 100 for v in valores]
    
    # Monta os rótulos com categorias e percentuais
    rotulos = [f"{nome_atributo.capitalize()} {categoria} ({valor} ocorrências - {percentual:.1f}%)"
               for categoria, valor, percentual in zip(categorias, valores, percentuais)]
    
    # Configura as cores
    if cores_personalizadas:
        cores = cores_personalizadas[:len(categorias)]  # Usa cores fornecidas
        if len(categorias) > len(cores_personalizadas):  # Gera aleatórias, se necessário
            cores += [
                f"#{''.join(random.choices('0123456789ABCDEF', k=6))}"
                for _ in range(len(categorias) - len(cores_personalizadas))
            ]
    else:
        # Gera todas as cores aleatoriamente se nenhuma for fornecida
        cores = [
            f"#{''.join(random.choices('0123456789ABCDEF', k=6))}"
            for _ in range(len(categorias))
        ]
    
    # Configura o gráfico de pizza em formato "donut"
    fig, ax = plt.subplots()
    wedges, texts = ax.pie(valores, labels=rotulos, colors=cores, wedgeprops=dict(width=0.3, edgecolor='w'))
    
    # Adiciona um círculo no centro para o estilo "donut"
    centro_circulo = plt.Circle((0, 0), 0.70, color='white')
    ax.add_artist(centro_circulo)
    
    # Ajusta o título e exibe o gráfico
    ax.set_title(f"Distribuição do atributo '{nome_atributo}'", fontsize=14)
    plt.show()