#===========================================================================================================
# ATENÇÃO - ANTES DE EXECUTAR O SCRIPT CERTIFIQUE-SE DE QUE TODOS OS PRÉ-REQUISITOS EXIGIDOS FORAM ATENDIDOS
#===========================================================================================================

from funcoes import *

# Função principal
def main():
    lst_msg_abertura = [
    "UNIVERSIDADE FEDERAL DE SANTA CATARINA - PROGRAMA DE PÓS-GRADUAÇÃO EM CIÊNCIA DA INFORMAÇÃO",
    "O presente script tem o objetivo de criar a infraestrutura tecnológica relacionada a pesquisa escritada tese:",
    "CONEXÕES PARLAMENTARES: GRAFOS LEGISLATIVOS PARA IMPACTO DECISIVO EM PROJETOS DE LEI",
    "Discente: MANOEL CAMILO DE SOUSA NETTO",
    "Orientador: Dr ADILSON LUIZ PINTO"
    ]
    os.system('cls' if os.name == 'nt' else 'clear')
    criar_caixa_ascii(lst_msg_abertura,cor='verde')

    # Cria uma variável networkx em memória
    Rede_concordancia = nx.Graph()

    try:
        criar_caixa_ascii(["M E N S A G E M","Pressione [ENTER] para continuar a criar \
a infraestrutura de pesquisa ou [CTRL-C] para abortar..."])
        input('')
    except KeyboardInterrupt:
        criar_caixa_ascii(["M E N S A G E M","criação de infraestrutura cancelada..."])
        exit(0)
    start_time = time.time()  # Início da medição do tempo
    db_name = "bd_tese"
    connection = connect_mysql()
    
    if connection:     
        #================================================================================================================
        # [ Referência: SCRIPT_PYTHON_TAREFA_01 ]
        # Tarefa do Fluxograma: [ Tarefa Operacional 1 ] 
        # Título da tarefa: Criar Banco de Dados Relacional (MySQL)
        # Descrição: Cria um banco de dados no Software MySQL Server
        #================================================================================================================        
        try:
            criar_caixa_ascii(["[ Tarefa Operacional 1 ]", \
                            "Criar Banco de Dados Relacional (MySQL)"],cor='amarelo')
            create_database(connection, db_name)            
        except Exception as e:
            # Capturando qualquer tipo de exceção e exibindo a mensagem de erro
            strErro = f"Ocorreu um erro imprevisível: {e}"
            criar_caixa_ascii(["ERRO NA EXECUÇÃO DA TAREFA 1- CRIAÇÃO DO BANCO DE DADOS",strErro],cor='vermelho')   
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
        try:
            criar_caixa_ascii(["[ Tarefa Operacional 2 ]", \
                              "Realizar download das planilhas nos Dados Abertos da Câmara dos Deputados"],cor='amarelo')
            arquivos_baixados = baixar_arquivos_de_dados()
        except Exception as e:
            # Capturando qualquer tipo de exceção e exibindo a mensagem de erro
            strErro = f"Ocorreu um erro imprevisível: {e}"
            criar_caixa_ascii(["ERRO NA EXECUÇÃO DA TAREFA 2 - DOWNLOAD DOS ARQUIVOS",strErro],cor='vermelho')   
        #==============================================================================================================
            

        #================================================================================================================
        # [ Referência: SCRIPT_PYTHON_TAREFA_03 ]
        # Tarefa do Fluxograma: [ Tarefa Operacional 3 ] 
        # Título da tarefa: Importar planilhas para o Banco de Dados recém-criado. Cada arquivo "".xlsx" gera uma única tabela
        # Descrição: Insere os registros contidos nas planilhas baixadas em tabelas do banco de dados Relacional MySQL.
        #================================================================================================================  
        try:    
            criar_caixa_ascii(["[ Tarefa Operacional 3 ]", \
            "Importar planilhas para o Banco de Dados recém-criado. Cada arquivo "".xlsx"" gera uma única tabela"],cor='amarelo')
            cria_tabelas_e_insere_dados_dos_arquivos(connection, db_name, arquivos_baixados )
        except Exception as e:
            # Capturando qualquer tipo de exceção e exibindo a mensagem de erro
            strErro = f"Ocorreu um erro imprevisível: {e}"
            criar_caixa_ascii(["ERRO NA EXECUÇÃO DA TAREFA 3 - IMPORTAÇÃO DE PLANILHAS",strErro],cor='vermelho')


        #================================================================================================================
        # [ Referências: SCRIPT_SQL_TAREFA_04, SCRIPT_SQL_TAREFA_05,SCRIPT_SQL_TAREFA_06,SCRIPT_SQL_TAREFA_07]
        # Tarefa do Fluxograma: [ Tarefa Analítica 4 ],[ Tarefa Analítica 5 ], [ Tarefa Analítica 6 ], [ Tarefa Analítica 7 ]
        # Título das tarefas: 
        #                   [ Tarefa Analítica 4 ] - Excluir votações não relevantes, Referência:  SCRIPT_SQL_TAREFA_004
        #                   [ Tarefa Analítica 5 ] - Criar rede de Concordâncias,Referência:  SCRIPT_SQL_TAREFA_005
        #                   [ Tarefa Analítica 6 ] - Excluir as concordâncias não duradouras(circunstanciais), 
        #                                            Referência: SCRIPT_SQL_TAREFA_006
        #                   [ Tarefa Analítica 7 ] - Identificar, qualitativamente, comunidades legislativas ideológicas 
        #                                            por meio da Ideologia partidária conhecida, Referência: SCRIPT_SQL_TAREFA_007
        # Descrição: Executa as tarefas 4,5,6 e 7 diretamente no banco de dados por meio da execução de um script SQL
        #================================================================================================================   
        try:
            criar_caixa_ascii([
                "Execução das Tarefas 4,5,6 e 7 diretamente no banco de dados",
                "[ Tarefa Analítica 4 ] - Excluir votações não relevantes, Referência: SCRIPT_SQL_TAREFA_004",
                "[ Tarefa Analítica 5 ] - Criar rede de Concordâncias, Referência: SCRIPT_SQL_TAREFA_005",
                "[ Tarefa Analítica 6 ] - Excluir as concordâncias não duradouras (circunstanciais), Referência: SCRIPT_SQL_TAREFA_006",
                "[ Tarefa Analítica 7 ] - Identificar, qualitativamente, comunidades legislativas ideológicas, Referência: SCRIPT_SQL_TAREFA_007"
            ], cor='amarelo')
            execute_sql_scripts(connection, db_name)
        except Exception as e:
            strErro = f"Ocorreu um erro imprevisível: {e}"
            criar_caixa_ascii(["ERRO NA EXECUÇÃO DAS TAREFAS 4,5,6 ou 7", strErro], cor='vermelho')
     
        #================================================================================================================
        # [ Referência: SCRIPT_PYTHON_TAREFA_08 ]
        # Tarefa do Fluxograma: [ Tarefa Analítica 8 ] 
        # Título da tarefa: Identificar, quantitativamente, comunidades por meio de métricas decorrentes da concordância 
        #                   duradoura e não circunstancial: descoberta de comunidades por meio da maximização da modularidade
        # Descrição: Executa o algoritmo louvain até maximizar a modularidade a atribuir uma comunidade a cada vértice.
        # O script ora executado traz a rede do banco de dados, executa o louvain para maximizar a modularidade e atribui
        # uma comunidade a cada vértice. Ao final cada um dos vértices estará alocado em um grupo (comunidade ou cluster)
        #================================================================================================================   
        try:   
            criar_caixa_ascii(['Tarefa 8', 'Identificar, quantitativamente, comunidades por meio de métricas decorrentes da \
 concordância duradoura e não circunstancial'],cor='amarelo')
            obter_rede(connection, db_name, "TB_ARESTAS",Rede_concordancia)
            cria_comunidades_modularidade(Rede_concordancia)
        except Exception as e:      
            strErro = f"Ocorreu um erro imprevisível: {e}"
            strErro="ERRO"
            criar_caixa_ascii(["ERRO NA EXECUÇÃO DA TAREFA 8 - IDENTIFICAR COMUNIDADES QUANTITATIVAMENTE",strErro],cor='vermelho')  
        
        #================================================================================================================
        # [ Referência: SCRIPT_PYTHON_TAREFA_09 ]
        # Tarefa do Fluxograma: [ Tarefa Analítica 9 ] 
        # Título da tarefa: Estabelecer uma relação entre as comunidades detectadas por ideologia partidária e aquelas 
        # detectadas por métricas
        # Descrição: Estabelece uma relação percentual que informa quantos parlamentares de uma determinada comunidade calculada 
        # por métricas foram alocados dentro de uma comunidade ideológico-partidária
        #================================================================================================================   
        try: 
            criar_caixa_ascii(['Tarefa 9', ' Estabelecer uma relação entre as comunidades detectadas por ideologia partidária e aquelas \
 detectadas por métricas'], cor='amarelo')
            exibir_tabela_distribuicao(Rede_concordancia, "modularidade")
            exibir_tabela_distribuicao(Rede_concordancia, "Ideologia")
            comparar_percentuais(Rede_concordancia,"Ideologia","modularidade")
        except Exception as e: 
            strErro = f"Ocorreu um erro imprevisível: {e}"
            criar_caixa_ascii(["ERRO NA EXECUÇÃO DA TAREFA 9 - ESTABELECER RELAÇÃO ENTRE COMUNIDADES IDEOLÓGICAS \
E COMUNIDADES OBTIDAS POR MÉTRICAS",strErro],cor='vermelho')  

        #================================================================================================================
        # [ Referência: SCRIPT_PYTHON_TAREFA_10 ]
        # Tarefa do Fluxograma: [ Tarefa Analítica 10 ] 
        # Título da tarefa: Identificar, quantitativamente usando métricas adequadas, os parlamentares aliados que 
        # ativamente estabelecem pontes com opositores e que podem ser úteis para neles causarem mudanças de opinião
        # Descrição: Para cade vértice, calcula a centralidade de ponte como sendo intermediação x coef. de ponte
        # Ao final, cria um boxplot que exibe cada parlamentar localizado na distribuição de acordo com sua
        # centralidade de ponte. A cor de cada ponto indica a comunidade quantitativa de parlamentares.
        #================================================================================================================   
        try:            
            calcular_centralidade_ponte(Rede_concordancia)
            plot_boxplot(Rede_concordancia,'Centralidade_Ponte','Ideologia',['#f76d83','#6ea0f4','#556B2F'])
            plotar_grafico_donut(Rede_concordancia, "Ideologia", ['#f76d83','#6ea0f4','#556B2F'])
        except Exception as e:      
            strErro = f"Ocorreu um erro imprevisível: {e}"
            criar_caixa_ascii(["ERRO NA EXECUÇÃO DA TAREFA 10 - CÁLCULO DA CENTRALIDADE DE PONTE...",strErro],cor='vermelho')  

        #================================================================================================================
        # [ Referência: SCRIPT_PYTHON_TAREFA_11 ]
        # Tarefa do Fluxograma: [ Tarefa Operacional 11 ] 
        # Título da tarefa: Salvar a rede em formatos GEFX, bem como em formato excel (um arquivo de vértices e um 
        # arquivo de arestas).
        # Descrição: Salva a rede como um arquivo .GEFX (Graph Exchange XML Format) que é uma linguagem para descrever 
        # estruturas de redes complexas, seus dados associados e dinâmicas.
        # Os arquivos de vértices e arestas em formato Excel permitem que a rede seja estudada em outros softwares
        #================================================================================================================   
        try:   
            salvar_Rede(Rede_concordancia)
        except Exception as e:      
            strErro = f"Ocorreu um erro imprevisível: {e}"
            criar_caixa_ascii(["ERRO NA EXECUÇÃO DA TAREFA 10 -  SALVANDO REDE EM FORMAT O GEFX E EXCEL...",strErro],cor='vermelho')  

         
    else:
        print("Não foi possível conectar ao servidor MySQL.")

    connection.close()
    criar_caixa_ascii(["CONEXÃO FINALIZADA",'A conexão com o banco de dados foi finalizada'],cor='amarelo')  
    end_time = time.time()  # Fim da medição do tempo
    total_time = end_time - start_time
    total_time_min = round(total_time/60,2)
    hora_atual = datetime.now().strftime("%H:%M:%S")   
    criar_caixa_ascii(["FINALIZAÇÃO",f'Finalizado em {hora_atual}.','Toda infraestrutura tecnológica da \
tese foi criada.',f'tempo de execução {total_time_min:.2f} minutos.'],cor='verde')

if __name__ == "__main__":
    main()
