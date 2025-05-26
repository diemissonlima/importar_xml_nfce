import json
from time import sleep
import xmltodict
import os
import re
import pymysql

print("Lendo os XML's, aguarde por gentileza..")


conn = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='1234',
    database='smc_automacao'
)

pag_dict = {
        "01": "1 - Dinheiro",
        "02": "2 - Cheque",
        "03": "3 - Cartão de Crédito",
        "04": "4 - Cartão de Débito",
        "05": "5 - Crédito Loja",
        "10": "10 - Vale Alimentação",
        "11": "11 - Vale Refeição",
        "12": "12 - Vale Presente",
        "13": "13 - Vale Combustível",
        "15": "15 - Boleto Bancário",
        "16": "16 - Depósito Bancário",
        "17": "17 - PIX",
        "18": "18 - Transferência Bancária",
        "99": "99 - Outros"
    }

tipo_emissao_dict = {
    "1": "NORMAL",
    "9": "CONTINGENCIA"
}


def deletar_xml_invalido():
    pasta = "nfe"
    for arquivo in os.listdir(pasta):
        caminho_arquivo = os.path.join(pasta, arquivo)
        nome_splitado = arquivo.split('-')
        extensao = nome_splitado[1]
        if extensao == 'nfe.xml':
            os.remove(caminho_arquivo)


def editar_xml(nome_arquivo):
    with open(f'nfe/{nome_arquivo}', 'r', encoding='utf-8') as arquivo_xml:
        xml_str = arquivo_xml.read()
        xml_str = re.sub(r'<\?xml.*?\?>', '', xml_str).lstrip()
        data = xmltodict.parse(xml_str)
        novo_xml = xmltodict.unparse(data, pretty=True)

    with open(f'nfe/{nome_arquivo}', 'w', encoding='utf-8') as arquivo_corrigido:
        arquivo_corrigido.write(novo_xml)

    extrair_dados(nome_arquivo)


def extrair_dados(nome_arquivo):
        try:
            with open(f'nfe/{nome_arquivo}', 'rb') as arquivo_xml:
                dic_arquivo = xmltodict.parse(arquivo_xml)
        except:
            editar_xml(nome_arquivo)
            print(f'NFe {nome_arquivo[28:34]} Corrigida!')
            return

        if "NFe" in dic_arquivo:
            infos_nf = dic_arquivo["NFe"]["infNFe"]
        elif "procEventoNFe" in dic_arquivo:
            infos_nf = dic_arquivo["procEventoNFe"]["evento"]["infEvento"]
            atualizar_nfce_cancelada(infos_nf["chNFe"])
            return
        else:
            infos_nf = dic_arquivo["nfeProc"]["NFe"]["infNFe"]

        numero_nota = infos_nf["ide"]["nNF"]
        valor_nota = infos_nf["total"]["ICMSTot"]["vProd"]
        descontos_nota = infos_nf["total"]["ICMSTot"]["vDesc"]
        acrescimos_nota = infos_nf["total"]["ICMSTot"]["vOutro"]
        valor_total_nota = infos_nf["total"]["ICMSTot"]["vNF"]

        serie = "00" + infos_nf["ide"]["serie"]
        data = infos_nf["ide"]["dhEmi"][:10]
        hora = infos_nf["ide"]["dhEmi"][11:19]
        chave = nome_arquivo[:44]# dic_arquivo["nfeProc"]["protNFe"]["infProt"]["chNFe"]
        data_pasta = chave[4:6] + "20" + chave[2:4]
        caminho_xml = "\\\\ESCRITORIO\SMC_LIGHT\\Notas_Fiscais\\NFCe\\" + data_pasta + "\Autorizadas\\" + chave + "-nfce.xml"
        tipo_emissao = tipo_emissao_dict[str(infos_nf["ide"]["tpEmis"])]
        status_nfce = "AUTORIZADA"
        retorno_nfce = "100 Autorizado o uso da NF-e"
        tipo_pagamento = infos_nf["pag"]["detPag"]
        items_nf = infos_nf["det"]

        print(data_pasta)

        # inserir_nfce_mysql([numero_nota, serie, data, hora, valor_nota, descontos_nota, acrescimos_nota,
        #                     valor_total_nota, chave, caminho_xml, tipo_emissao, status_nfce, retorno_nfce])
        
        # inserir_venda_mysql([data, hora, valor_nota, descontos_nota, acrescimos_nota,
        #                      valor_total_nota, "FECHADA", numero_nota])
        
        # if type(tipo_pagamento) == dict:
        #     inserir_venda_pagamento_mysql([tipo_pagamento])
        # elif type(tipo_pagamento) == list:
        #     inserir_venda_pagamento_mysql(tipo_pagamento)
        
        # if type(items_nf) == dict:
        #     inserir_venda_item([items_nf])
        # elif type(items_nf) == list:
        #     inserir_venda_item(items_nf)


def inserir_nfce_mysql(dados):
    global ultima_venda
    if conn.open:
        try:
            with conn.cursor() as cursor:
                ultima_venda += 1
                query = ("INSERT INTO nfce (data_emissao, hora_emissao, data_transmissao, hora_transmissao, codigo_venda,"
                         "valor_nfce, acrescimos_nfce, descontos_nfce, total_nfce, valor_pago, lote, serie, chave, "
                         "caminho_xml, tipo_emissao, status_nfce, retorno_nfce, codigo)"
                         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

                data = dados[2], dados[3], dados[2], dados[3], ultima_venda, dados[4], dados[6], dados[5], dados[7], \
                    dados[7], dados[1], '1', dados[8], dados[9], dados[10], dados[11], dados[12], dados[0]

                cursor.execute(query, data)
                conn.commit()

        except pymysql.MySQLError as erro:
            print(f"Erro: {erro}")


def inserir_venda_mysql(dados):
    caixa = "00" + str(cod_caixa)
    if conn.open:
        try:
            with conn.cursor() as cursor:
                query = ("INSERT INTO venda (operador, caixa, data, hora, valor_venda, acrescimo, desconto, total_venda, "
                         "valor_pago, status, cod_nfce, idCaixa)"
                         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

                data = cod_operador, caixa, dados[0], dados[1], dados[2], dados[4], dados[3], dados[5], dados[5], \
                    dados[6], dados[7], id_caixa

                cursor.execute(query, data)
                conn.commit()

        except pymysql.MySQLError as erro:
            print(f"Erro: {erro}")


def inserir_venda_pagamento_mysql(dados):
    global codigo_venda_pagamento
    if conn.open:
        try:
            codigo_venda_pagamento += 1
            with conn.cursor() as cursor:
                for pag in dados:
                    query = ("INSERT INTO venda_pagamento (codigo_venda, cod_tipo_pagamento, tipo_pagamento, total_pago, valor_debitado)"
                             "VALUES (%s, %s, %s, %s, %s)")

                    data = codigo_venda_pagamento, int(pag['tPag']), pag_dict[pag['tPag']].upper(), pag['vPag'], pag['vPag']

                    cursor.execute(query, data)
                    conn.commit()

        except pymysql.MySQLError as error:
            print(f"Erro: {error}")


def inserir_venda_item(dados):
    global codigo_venda_item
    for item in dados:
        if item['@nItem'] == '1':
            codigo_venda_item += 1

        if conn.open:
            try:
                with conn.cursor() as cursor:
                    query = ("INSERT INTO venda_item (codigo_venda, codigo_item_venda, codigo_produto, descricao,"
                             "unidade, quantidade, preco, preco_total)"
                             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

                    data = codigo_venda_item, item['@nItem'], item['prod']['cProd'], item['prod']['xProd'],\
                        item['prod']['uCom'], item['prod']['qCom'], item['prod']['vUnCom'], item['prod']['vProd']

                    cursor.execute(query, data)
                    conn.commit()

            except pymysql.MySQLError as error:
                print(f"Erro: {error}")


def atualizar_nfce_cancelada(chave_nfce):
    numero = chave_nfce[28:34]
    data_pasta = chave_nfce[4:6] + "20" + chave_nfce[2:4]
    caminho = "\\\\ESCRITORIO\\SMC_LIGHT\\Notas_Fiscais\\NFCe\\" + data_pasta + "\\Canceladas\\" + chave_nfce + "-cancnfce.xml"
    status = "CANCELADA"
    retorno = "135 Evento registrado e vinculado a NF-e"

    with conn.cursor() as cursor:
        if conn.open:
            query = """
            UPDATE nfce
            SET caminho_xml = %s, status_nfce = %s, retorno_nfce = %s
            WHERE codigo = %s
        """

            cursor.execute(query, (caminho, status, retorno, numero))
            conn.commit()


ultima_venda = int(input("Digite o numero da ultima venda: "))
cod_operador = int(input("Digite o codigo do operador: "))
cod_caixa = int(input("Digite o numero caixa: "))
id_caixa = int(input("Digite o ID do caixa: "))

codigo_venda_pagamento = ultima_venda
codigo_venda_item = ultima_venda

deletar_xml_invalido()

lista_arquivos = os.listdir("nfe")

cont = 0
for arquivo in lista_arquivos:
    extrair_dados(arquivo)
    cont += 1

print('-=' * 20)
print('Leitura finalizada com sucesso!'.center(40))
print(f'Foram lidas {cont} NFs.'.center(40))
print('-=' * 20)
input('Pressione ENTER para encerrar.')
