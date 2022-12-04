from py2neo import Graph
import spacy
import os
import json


def read_text_file(file_path):
    f = open(file_path)

    data = json.load(f)
    
    for documento in data:
        tupla = ()
        if (documento['Entidades']):
            for each_item in documento['Entidades']:
                nome_entidade = each_item
                
                tipo_entidade = "ENTIDADE"
                
                tupla = ([documento['URL'][0]],
                         documento['Titulo'][0],
                         tipo_entidade,
                         nome_entidade)
                
                entidadesDict.append(tupla)
                
        if (documento['Autores']):
            for each_item in documento['Autores']:
                nome_entidade = each_item
                tipo_entidade = "AUTOR"
                tupla = ([documento['URL'][0]],documento['Titulo'][0],tipo_entidade, nome_entidade)
                entidadesDict.append(tupla)
                
        if (documento['DataPublicado']):
            for each_item in documento['DataPublicado']:
                nome_entidade = each_item
                tipo_entidade = "DATA"
                tupla = ([documento['URL'][0]],documento['Titulo'][0],tipo_entidade, nome_entidade)
                entidadesDict.append(tupla)
                
        if (documento['PalavrasChaves']):
            for each_item in documento['PalavrasChaves']:
                nome_entidade = each_item
                tipo_entidade = "PALAVRA_CHAVE"
                tupla = ([documento['URL'][0]],documento['Titulo'][0],tipo_entidade, nome_entidade)
                entidadesDict.append(tupla)
        

# CREATE


def createDocumento(tituloDocumento: str, linkDocumento: str):
    queryCreateFile = f"CREATE (doc: Documento {{titulo: '{tituloDocumento}', link: '{linkDocumento}'}})"
    grafo.run(queryCreateFile)


def createAutor(valorAutor: str):
    queryCreateType = f"CREATE (a: Autor {{valor: '{valorAutor}'}})"
    grafo.run(queryCreateType)


def createData(valorData: str):
    queryCreateType = f"CREATE (d: Data {{valor: '{valorData}'}})"
    grafo.run(queryCreateType)


def createPalavraChave(valorPalavraChave: str):
    queryCreateType = f"CREATE (pc: PalavraChave {{valor: '{valorPalavraChave}'}})"
    grafo.run(queryCreateType)


def createEntidade(valorEntidade: str, tipo: str):
    queryCreateEntity = f"CREATE (e: Entidade {{valor: '{valorEntidade}', tipo: '{tipo}'}})"
    grafo.run(queryCreateEntity)

# CHECK
def checkDocumento(linkDocumento: str):
    queryMatchFile = f"MATCH (doc: Documento) WHERE doc.link = '{linkDocumento}'  RETURN doc"
    listaFile = grafo.run(queryMatchFile).data()
    return listaFile


def checkAutor(nameAutor: str):
    queryMatchType = f"MATCH (a: Autor) WHERE a.valor = '{nameAutor}' RETURN a"
    listaType = grafo.run(queryMatchType).data()
    return listaType


def checkData(valorData: str):
    queryMatchType = f"MATCH (d: Data) WHERE d.valor = '{valorData}' RETURN d"
    listaType = grafo.run(queryMatchType).data()
    return listaType


def checkPalavraChave(valorPalavraChave: str):
    queryMatchType = f"MATCH (pc: PalavraChave) WHERE pc.valor = '{valorPalavraChave}' RETURN pc"
    listaType = grafo.run(queryMatchType).data()
    return listaType


def checkEntidade(valorEntidade: str, tipo: str):
    queryMatchType = f"MATCH (e: Entidade) WHERE e.valor = '{valorEntidade}' AND e.tipo = '{tipo}' RETURN e"
    listaType = grafo.run(queryMatchType).data()
    return listaType

# FIND


def findDocumento(linkDocumento: str):
    queryFindFile = f"MATCH (doc: Documento) WHERE doc.link = '{linkDocumento}' RETURN doc.link"
    jsonListFindFile = grafo.run(queryFindFile).data()
    dictFindFile = dict(jsonListFindFile[0])
    fileName = str(dictFindFile["doc.link"])
    return fileName


def findAutor(nameAutor: str):
    queryFindType = f"MATCH (a: Autor) WHERE a.valor = '{nameAutor}' RETURN a.valor"
    jsonListFindType = grafo.run(queryFindType).data()
    dictFindType = dict(jsonListFindType[0])
    typeName = str(dictFindType["a.valor"])
    return typeName


def findData(valorData: str):
    queryFindType = f"MATCH (d: Data) WHERE d.valor = '{valorData}' RETURN d.valor"
    jsonListFindType = grafo.run(queryFindType).data()
    dictFindType = dict(jsonListFindType[0])
    typeName = str(dictFindType["d.valor"])
    return typeName


def findPalavraChave(valorPalavraChave: str):
    queryFindType = f"MATCH (pc: PalavraChave) WHERE pc.valor = '{valorPalavraChave}' RETURN pc.valor"
    jsonListFindType = grafo.run(queryFindType).data()
    dictFindType = dict(jsonListFindType[0])
    typeName = str(dictFindType["pc.valor"])
    return typeName


def findEntidade(valorEntidade: str, tipo: str):
    queryMatchFile = f"MATCH (e: Entidade) WHERE e.valor = '{valorEntidade}' AND e.tipo = '{tipo}' RETURN e.valor, e.tipo"
    jsonListFindEntity = grafo.run(queryMatchFile).data()
    dictFindEntity = dict(jsonListFindEntity[0])
    entityValor = str(dictFindEntity["e.valor"])
    entityTipo = str(dictFindEntity["e.tipo"])
    return (entityValor + '#' + entityTipo )

# CREATE RELATION


def createDocumentoAutorRelationShip(linkDocumento: str, valorAutor: str):
    queryEntityFileRelationship = f"MATCH (a: Autor), (doc: Documento) " \
                                  f"WHERE  a.valor = '{valorAutor}' AND doc.link = " \
                                  f"'{linkDocumento}' " \
                                  f"CREATE (doc)-[r:escrito_por]->(a) " \
                                  f"RETURN type(r)"

    grafo.run(queryEntityFileRelationship)


def createDocumentoDataRelationShip(linkDocumento: str, valorData: str):
    queryEntityFileRelationship = f"MATCH (d: Data), (doc: Documento) " \
                                  f"WHERE  d.valor = '{valorData}' AND doc.link = " \
                                  f"'{linkDocumento}' " \
                                  f"CREATE (doc)-[r:publicado_em]->(d) " \
                                  f"RETURN type(r)"

    grafo.run(queryEntityFileRelationship)


def createDocumentoPalavraChaveRelationShip(linkDocumento: str, valorPalavraChave: str):
    queryEntityFileRelationship = f"MATCH (pc: PalavraChave), (doc: Documento) " \
                                  f"WHERE  pc.valor = '{valorPalavraChave}' AND doc.link = " \
                                  f"'{linkDocumento}' " \
                                  f"CREATE (doc)-[r:contem]->(pc) " \
                                  f"RETURN type(r)"

    grafo.run(queryEntityFileRelationship)


def createDocumentoEntidadeRelationShip(linkDocumento: str, valorEntidade: str, tipo:str):
    queryEntityFileRelationship = f"MATCH (e: Entidade), (doc: Documento) " \
                                  f"WHERE  e.valor = '{valorEntidade}' AND e.tipo = '{tipo}' AND doc.link = " \
                                  f"'{linkDocumento}' " \
                                  f"CREATE (doc)-[r:menciona]->(e) " \
                                  f"RETURN type(r)"

    grafo.run(queryEntityFileRelationship)


def createGraph(linkDocumento: str, tituloDocumento: str, tipoNo: str, valorNo: str):

    if (tipoNo == 'AUTOR'):
        listaDocumentos = checkDocumento(linkDocumento)
        listaAutor = checkAutor(valorNo)

        # Documento não existe
        if len(listaDocumentos) == 0:
            createDocumento(tituloDocumento, linkDocumento)
            recoveredDocumento = findDocumento(linkDocumento)
            if len(listaAutor) == 0:
                createAutor(valorNo)
                recoveredAutor = findAutor(valorNo)
                createDocumentoAutorRelationShip(recoveredDocumento, recoveredAutor)
            else:
                recoveredAutor = findAutor(valorNo)
                createDocumentoAutorRelationShip(recoveredDocumento, recoveredAutor)
        # Documento existe
        else:
            recoveredDocumento = findDocumento(linkDocumento)
            
            if len(listaAutor) == 0:
                createAutor(valorNo)
                recoveredAutor= findAutor(valorNo)
                createDocumentoAutorRelationShip(recoveredDocumento, recoveredAutor)
            else:
                recoveredAutor = findAutor(valorNo)
                createDocumentoAutorRelationShip(recoveredDocumento, recoveredAutor)
        
    elif (tipoNo == 'DATA'):
        listaDocumentos = checkDocumento(linkDocumento)
        listaData = checkData(valorNo)

        # Documento não existe
        if len(listaDocumentos) == 0:
            createDocumento(tituloDocumento, linkDocumento)
            recoveredDocumento = findDocumento(linkDocumento)
            if len(listaData) == 0:
                createData(valorNo)
                recoveredData = findData(valorNo)
                createDocumentoDataRelationShip(recoveredDocumento, recoveredData)
            else:
                recoveredData = findData(valorNo)
                createDocumentoDataRelationShip(recoveredDocumento, recoveredData)
        # Documento existe
        else:
            recoveredDocumento = findDocumento(linkDocumento)
            
            if len(listaData) == 0:
                createData(valorNo)
                recoveredData= findData(valorNo)
                createDocumentoDataRelationShip(recoveredDocumento, recoveredData)
            else:
                recoveredData = findData(valorNo)
                createDocumentoDataRelationShip(recoveredDocumento, recoveredData)
    elif (tipoNo == 'PALAVRA_CHAVE'):
        listaDocumentos = checkDocumento(linkDocumento)
        listaPalavraChave = checkPalavraChave(valorNo)

        # Documento não existe
        if len(listaDocumentos) == 0:
            createDocumento(tituloDocumento, linkDocumento)
            recoveredDocumento = findDocumento(linkDocumento)
            if len(listaPalavraChave) == 0:
                createPalavraChave(valorNo)
                recoveredPalavraChave = findPalavraChave(valorNo)
                createDocumentoPalavraChaveRelationShip(recoveredDocumento, recoveredPalavraChave)
            else:
                recoveredPalavraChave = findPalavraChave(valorNo)
                createDocumentoPalavraChaveRelationShip(recoveredDocumento, recoveredPalavraChave)
        # Documento existe
        else:
            recoveredDocumento = findDocumento(linkDocumento)
            
            if len(listaPalavraChave) == 0:
                createPalavraChave(valorNo)
                recoveredPalavraChave= findPalavraChave(valorNo)
                createDocumentoPalavraChaveRelationShip(recoveredDocumento, recoveredPalavraChave)
            else:
                recoveredPalavraChave = findPalavraChave(valorNo)
                createDocumentoPalavraChaveRelationShip(recoveredDocumento, recoveredPalavraChave)
        
    elif (tipoNo == 'ENTIDADE'):
        listaDocumentos = checkDocumento(linkDocumento)
        valorArray = valorNo.split('#')
        valor = valorArray[0]
        tipo = valorArray[1]
        listaEntidade = checkEntidade(valor, tipo)

        # Documento não existe
        if len(listaDocumentos) == 0:
            createDocumento(tituloDocumento, linkDocumento)
            recoveredDocumento = findDocumento(linkDocumento)
            if len(listaEntidade) == 0:
                createEntidade(valor,tipo)
                recoveredEntidade = findEntidade(valor,tipo)
                recoveredArray = recoveredEntidade.split('#')
                recoveredvalor = recoveredArray[0]
                recoveredtipo = valorArray[1]
                createDocumentoEntidadeRelationShip(recoveredDocumento, recoveredvalor, recoveredtipo)
            else:
                recoveredEntidade = findEntidade(valor,tipo)
                recoveredArray = recoveredEntidade.split('#')
                recoveredvalor = recoveredArray[0]
                recoveredtipo = valorArray[1]
                createDocumentoEntidadeRelationShip(recoveredDocumento, recoveredvalor, recoveredtipo)
        # Documento existe
        else:
            recoveredDocumento = findDocumento(linkDocumento)
            
            if len(listaEntidade) == 0:
                createEntidade(valor,tipo)
                recoveredEntidade = findEntidade(valor,tipo)
                recoveredArray = recoveredEntidade.split('#')
                recoveredvalor = recoveredArray[0]
                recoveredtipo = valorArray[1]
                createDocumentoEntidadeRelationShip(recoveredDocumento, recoveredvalor, recoveredtipo)
            else:
                recoveredEntidade = findEntidade(valor,tipo)
                recoveredArray = recoveredEntidade.split('#')
                recoveredvalor = recoveredArray[0]
                recoveredtipo = valorArray[1]
                createDocumentoEntidadeRelationShip(recoveredDocumento, recoveredvalor, recoveredtipo)
                
uri = "neo4j+s://b193c73e.databases.neo4j.io"
user = "neo4j"
password = "aa-IdLAN4zZ6uMUOGj3paxfz2GvtqH93FUeZHUK_GCM"

# windows-1252
unicode = 'windows-1252'

try:
    grafo = Graph(uri, auth=(user, password))
    print('SUCCESS: Connected to the Neo4j Database.')
except Exception as e:
    print('ERROR: Could not connect to the Neo4j Database. See console for details')
    raise SystemExit(e)

nlp = spacy.load("pt_core_news_sm")
file_path = r'C:\Repositorios\TCC\named-entity-extraction\lista_documentos_com_entidades.json' #Fazer a parte 3 e 4

entidadesDict = []

read_text_file(file_path)

try:
    contador = 1
    for tupla in entidadesDict:
        print(f"ID: {contador} Total: {len(entidadesDict)}")
        contador = contador + 1
        print(f"Link = {tupla[0]}")
        print(f"Titulo = {tupla[1]}")
        print(f"Tipo = {tupla[2]}")
        print(f"Valor = {tupla[3]}")
        print("\n")

        createGraph(tupla[0][0], tupla[1], tupla[2], tupla[3])
except Exception as e: 
    print("UM ERRO OCORREU")
    print(e)
