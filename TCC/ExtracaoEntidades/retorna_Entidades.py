import json
import pandas as pd
import spacy
import pt_core_news_sm
import os
import uuid

lista_entidades = []
nlp = pt_core_news_sm.load()
filename = r'C:\Repositorios\TCC\ExtracaoEntidades\lista_metadados_sem_entidades.json'

with open(filename,'r+',encoding='windows-1252') as file_lista_documentos:
  lista_documentos = json.load(file_lista_documentos)

  #print(lista_documentos[0]["Resumo"])
  pos = 0
  for documento in lista_documentos:
    #print(documento["Resumo"])
    lista_entidades.clear()
    resumo = documento["Resumo"][0]
    info = nlp(resumo)
    for entidade in info.ents:
      #print(documento["Titulo"][0] + " - " + entidade.text + "#" + entidade.label_)
      string_entidade_label = entidade.text + "#" + entidade.label_
      lista_entidades.append(string_entidade_label)
    documento["Entidades"] = lista_entidades.copy()
    pos = pos + 1

tempfile = os.path.join(os.path.dirname(filename), 'lista_documentos_com_entidades')
with open(tempfile, 'w',encoding='windows-1252') as file_lista_documentos:
    json.dump(lista_documentos, file_lista_documentos, indent=4, ensure_ascii=False)
