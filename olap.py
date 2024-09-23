import pandas as pd
import pyodbc

arquivoB3 = './COTAHIST_A2023.TXT'

separar_campos=[2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]

dados_B3 = pd.read_fwf(arquivoB3, widths=separar_campos, header=0)
dados_B3.columns = [
"tipo_registro",
"data_pregao",
"cod_bdi",
"cod_negociacao",
"tipo_mercado",
"nome_empresa",
"especificacao_papel",
"prazo_dias_merc_termo",
"moeda",
"preco_abertura",
"preco_maximo",
"preco_minimo",
"preco_medio",
"preco_ultimo_negocio",
"preco_melhor_oferta_compra",
"preco_melhor_oferta_venda",
"numero_negocios",
"quantidade_papeis_negociados",
"volume_total_negociado",
"preco_exercicio",
"ìndicador_correcao_precos",
"data_vencimento" ,
"fator_cotacao",
"preco_exercicio_pontos",
"codigo_isin",
"num_distribuicao_papel"
]
tirar_Ultima_linha = len(dados_B3['data_pregao'])
dados_B3 = dados_B3.drop(tirar_Ultima_linha-1)
# Tratar valores com virgula (dinheiro)

dinheiro = [
"preco_abertura",
"preco_maximo",
"preco_minimo",
"preco_medio",
"preco_ultimo_negocio",
"preco_melhor_oferta_compra",
"preco_melhor_oferta_venda",
"volume_total_negociado",
"preco_exercicio",
"preco_exercicio_pontos"
]

for coluna in dinheiro:
  dados_B3[coluna] = [i/100 for i in dados_B3[coluna]]

# Tratar datas  
dados_B3['data_pregao'] = pd.to_datetime(dados_B3['data_pregao'], format='%Y%m%d')
dados_B3['data_pregao'] = dados_B3['data_pregao'].dt.strftime('%d/%m/%Y')

# Configuração da conexão com o SQL Server
conn_str = (
    "DRIVER="  # Ajuste para o driver correto
    "SERVER="                  # Servidor rodando no Docker (localhost)
    "DATABASE=;"                        # Banco de dados (pode mudar se necessário)
    "UID=;"                                 # Usuário do SQL Server
    "PWD=;"                        # Senha do SQL Server
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Inserção dos dados na Dimensão_Ativo
for index, row in dados_B3.iterrows():
    cursor.execute('''
        INSERT INTO Dimensao_Ativo (cod_negociacao, nome_empresa, especificacao_papel, codigo_isin)
        VALUES (?, ?, ?, ?)
    ''', row['cod_negociacao'], row['nome_empresa'], row['especificacao_papel'], row['codigo_isin'])

# Inserção dos dados na Dimensão_Tempo
dados_B3['ano'] = dados_B3['data_pregao'].apply(lambda x: pd.to_datetime(x).year)
dados_B3['trimestre'] = dados_B3['data_pregao'].apply(lambda x: (pd.to_datetime(x).month - 1) // 3 + 1)

for index, row in dados_B3.iterrows():
    cursor.execute('''
        INSERT INTO Dimensao_Tempo (data_pregao, ano, trimestre)
        VALUES (?, ?, ?)
    ''', row['data_pregao'], row['ano'], row['trimestre'])

# Inserção dos dados na Dimensão_Mercado
for index, row in dados_B3.iterrows():
    cursor.execute('''
        INSERT INTO Dimensao_Mercado (cod_bdi, tipo_mercado)
        VALUES (?, ?)
    ''', row['cod_bdi'], row['tipo_mercado'])

# Inserção dos dados na Fato_Negociacao
for index, row in dados_B3.iterrows():
    cursor.execute('''
        INSERT INTO Fato_Negociacao (
            cod_negociacao, data_pregao, cod_bdi, 
            preco_abertura, preco_maximo, preco_minimo, preco_medio, preco_ultimo_negocio, 
            numero_negocios, quantidade_papeis_negociados, volume_total_negociado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', row['cod_negociacao'], row['data_pregao'], row['cod_bdi'], row['preco_abertura'], row['preco_maximo'],
       row['preco_minimo'], row['preco_medio'], row['preco_ultimo_negocio'], row['numero_negocios'], 
       row['quantidade_papeis_negociados'], row['volume_total_negociado'])

# Confirmar e fechar a conexão
conn.commit()
cursor.close()
conn.close()

