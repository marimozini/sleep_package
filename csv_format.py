import pandas as pd

#df = pd.read_excel("BIN/1379-38_left wrist_046456_2018-08-03 10-23-38.csv")
#file_path = "BIN/1379-38_left wrist_046456_2018-08-03 10-23-38.csv"
#key = "Time Zone"


def split_100_csv (file_path, key):
    # Lê manualmente as primeiras 100 linhas
    with open(file_path, 'r', encoding='utf-8') as file:
        primeiras_100_linhas = [next(file) for _ in range(100)]

    measurement_frequency = None
    dados_extracao = {}
    result = None

    # Itera sobre cada linha e divide a partir da vírgula
    for linha in primeiras_100_linhas:
        partes = linha.split(",", 1)  # Divide a linha em duas partes (antes e depois da vírgula)
        
        if len(partes) > 1:  # Verifica se há algo após a vírgula
            chave = partes[0].strip()  # Pega o texto antes da vírgula (opcional)
            valor = partes[1].strip()  # Pega o texto depois da vírgula
            dados_extracao[chave] = valor
            #print(f"{chave}: {valor}")

    
    for chave, valor in dados_extracao.items():
        #print(f"{chave}: {valor}")

        
        if chave == key and key == "Measurement Frequency":
             measurement_frequency = ''.join([char for char in valor if char.isdigit()])

        elif chave == key:
             result = valor 
        
    return result if result is not None else measurement_frequency  



