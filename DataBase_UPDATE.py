import io
import os
import time
from ftplib import FTP, error_perm
import psycopg2
import schedule
from dotenv import load_dotenv

load_dotenv()


# --------------------------------------------------------------------------------------------------------------------
# FUNÇÃO PARA CONEXÃO DO FTP
def conexao_FTP():
    ftp = FTP(os.environ.get('HOST_FTP'))
    ftp.login(user=os.environ.get('USER_FTP'), passwd=os.environ.get('PASSWD_FTP'), acct='')
    ftp.encoding = "utf-8"

    ftp.cwd('/')

    return ftp


# --------------------------------------------------------------------------------------------------------------------
# FUNÇÃO PARA TRANSFERÊNCIA DOS DADOS
def download_FTP(ftp, filename):
    r = io.BytesIO()
    ftp.retrbinary(f'RETR {filename}', r.write)

    dados = r.getvalue().decode()

    dados = dados.split(',')
    dados = [i.replace(' ', '') for i in dados]

    if len(filename) == 32:

        hora_arq = filename[24:26] + ':' + filename[26:28] + ':00'  # Para frequencia de _15m_ ou _24h_
        # hora_arq = filename[24:26] + ':' + filename[26:28] + ':00'  # Para frequencia de _1h_
        # hora_arq = hora_minuto[0:2] + ':' + hora_minuto[2:] + ':00'

    else:
        hora_arq = filename[26:28] + ':' + filename[28:30] + ':00'  # Para frequencia de _15m_ ou _24h_

    data_hora = dados[2] + ' ' + hora_arq

    dados.pop(3)
    dados.pop(2)

    dados.insert(0, data_hora)

    r.close()

    dados = tuple(dados)

    return dados


# --------------------------------------------------------------------------------------------------------------------
# FUNÇÃO PARA CONEXÃO COM O BANCO DE DADOS
def conexao_db():
    # USUÁRIO POSTGRES - ACESSA TODOS OS BANCOS
    con = psycopg2.connect(host=os.environ.get('HOST_DATABASE'),
                           database=os.environ.get('NAME_DATABASE'),
                           user=os.environ.get('USER_DATABASE'),
                           password=os.environ.get('PASSWD_DATABASE')
                           )
    return con


# --------------------------------------------------------------------------------------------------------------------
# FUNÇÃO PARA CONSULTAS NO BANCO DE DADOS
def consulta_dados(sql_SELECT):
    # CONEXÃO AO DB E CRIAÇÃO DO CURSOR
    con = conexao_db()
    cur = con.cursor()

    # Executar o comando sql
    cur.execute(sql_SELECT)

    recset = cur.fetchall()
    registros = []

    for rec in recset:
        registros.append(rec)

    con.close()

    return registros


# --------------------------------------------------------------------------------------------------------------------
# FUNÇÃO PARA INSERIR DADOS NO BANCO
def update_data(sql_UPDATE):
    # CONEXÃO AO DB E CRIAÇÃO DO CURSOR
    con = conexao_db()
    cur = con.cursor()

    try:

        cur.execute(sql_UPDATE)
        con.commit()

    except (Exception, psycopg2.DatabaseError) as error:

        print("Error: %s" % error)
        con.rollback()
        cur.close()

        return None

    cur.close()


# =================================================================================================================
# =================================================================================================================
# FUNÇÃO PRINCIPAL PARA RODAR O CÓDIGO
def job_update():
    print('...start run...\n')

    # CONEXÃO COM O FTP
    ftp = conexao_FTP()

    pastas = ['EMA-Cepdec', 'EMA-Parceiros']

    for pasta in pastas:

        ftp.cwd(f'/{pasta}')

        # LOOP PARA PERCORRER TODAS AS PASTAS DAS EMAs
        for EMA in sorted(ftp.nlst()):

            # CONSULTA AO BANCO DE DADOS
            sql = f'SELECT * FROM {EMA}_15m ' \
                  f'ORDER BY data_hora_utc ASC'

            # A CONSULTA RETORNA UMA LISTA DE TUPLAS COM OS DADOS
            registros_db = consulta_dados(sql)

            lista_arq_branco = {}

            # LOOP PARA FORMAÇÃO DA LISTA DOS NOMES DOS ARQUIVOS QUE ESTÃO EM BRANCO
            for i in registros_db:

                if any(i[j] == '' for j in range(0, len(i))):

                    chave = i[0]
                    frequencia = i[2]
                    data = i[0][:10]
                    hora = i[0][11:16]
                    nome_arquivo = EMA + '_' + frequencia + '_' + data[:4] + data[5:7] + data[8:10] + '_' + hora[:2] \
                                   + hora[3:5] + '.txt'

                    lista_arq_branco[chave] = nome_arquivo

                else:

                    pass

            keys = list(lista_arq_branco.keys())
            filenames = list(lista_arq_branco.values())

            for filename, key in zip(filenames, keys):

                try:

                    # ENTRAR NO DIRETÓRIO DA ESTAÇÃO
                    ftp.cwd(f'/{pasta}/{EMA}')

                    # DOWNLOAD DOS DADOS NO FTP_CEPDEC
                    dados = download_FTP(ftp, filename)

                    # COMANDO SQL PARA UPDATE
                    sql = f"UPDATE {EMA}_15m " \
                          f"SET " \
                          f"temp_int = '{dados[3]}', " \
                          f"pressao_inst = '{dados[4]}', " \
                          f"pressao_med = '{dados[5]}', " \
                          f"pressao_max = '{dados[6]}', " \
                          f"pressao_min = '{dados[7]}', " \
                          f"temp_inst = '{dados[8]}', " \
                          f"temp_med = '{dados[9]}', " \
                          f"temp_max = '{dados[10]}', " \
                          f"temp_min = '{dados[11]}', " \
                          f"umid_rel_inst = '{dados[12]}', " \
                          f"umid_rel_med = '{dados[13]}', " \
                          f"umid_rel_max = '{dados[14]}', " \
                          f"umid_rel_min = '{dados[15]}', " \
                          f"rad_solar_glob_inst_LPPYRRA02 = '{dados[16]}', " \
                          f"rad_solar_glob_med_LPPYRRA02 = '{dados[17]}', " \
                          f"rad_solar_glob_max_LPPYRRA02 = '{dados[18]}', " \
                          f"rad_solar_glob_min_LPPYRRA02 = '{dados[19]}', " \
                          f"rad_solar_glob_inst_LPNET14 = '{dados[20]}', " \
                          f"rad_solar_glob_med_LPNET14 = '{dados[21]}', " \
                          f"rad_solar_glob_max_LPNET14 = '{dados[22]}', " \
                          f"rad_solar_glob_min_LPNET14 = '{dados[23]}', " \
                          f"rad_solar_reflet_inst = '{dados[24]}', " \
                          f"rad_solar_reflet_med = '{dados[25]}', " \
                          f"rad_solar_reflet_max = '{dados[26]}', " \
                          f"rad_solar_reflet_min = '{dados[27]}', " \
                          f"rad_iv_inst = '{dados[28]}', " \
                          f"rad_iv_med = '{dados[29]}', " \
                          f"rad_iv_max = '{dados[30]}', " \
                          f"rad_iv_min = '{dados[31]}', " \
                          f"rad_spf_inst = '{dados[32]}', " \
                          f"rad_spf_med = '{dados[33]}', " \
                          f"rad_spf_max = '{dados[34]}', " \
                          f"rad_spf_min = '{dados[35]}', " \
                          f"rad_sol_liq_inst = '{dados[36]}', " \
                          f"rad_sol_liq_med = '{dados[37]}', " \
                          f"rad_sol_liq_max = '{dados[38]}', " \
                          f"rad_sol_liq_min = '{dados[39]}', " \
                          f"temp_inst_NTC = '{dados[40]}', " \
                          f"temp_med_NTC = '{dados[41]}', " \
                          f"temp_max_NTC = '{dados[42]}', " \
                          f"temp_min_NTC = '{dados[43]}', " \
                          f"dir_vent_med = '{dados[44]}', " \
                          f"dir_vent_max = '{dados[45]}', " \
                          f"dir_vent_min = '{dados[46]}', " \
                          f"vel_vent_med = '{dados[47]}', " \
                          f"vel_vent_max = '{dados[48]}', " \
                          f"vel_vent_min = '{dados[49]}', " \
                          f"temp_solo_inst = '{dados[50]}', " \
                          f"temp_solo_med = '{dados[51]}', " \
                          f"temp_solo_max = '{dados[52]}', " \
                          f"temp_solo_min = '{dados[53]}', " \
                          f"umid_solo_inst = '{dados[54]}', " \
                          f"umid_solo_med = '{dados[55]}', " \
                          f"umid_solo_max = '{dados[56]}', " \
                          f"umid_solo_min = '{dados[57]}', " \
                          f"prec = '{dados[58]}' " \
                          f"WHERE data_hora_utc = '{key}';"

                    update_data(sql)

                    print(f'{EMA}: {filename}')
                    print('=== Atualização realizada com sucesso ===\n')

                except error_perm as e:

                    print(f'Arquivo não encontrado!')
                    print(f'ERRO: {e}\n')

                except IndexError as e:

                    print(f'Arquivo vazio: {e}\n')

        print('====================================================================================')

    return None


# =================================================================================================================
# =================================================================================================================
# CHAMAMENTO DA FUNÇÃO PRINCIPAL - AGENDAMENTO PARA RODAR O CÓDIGO
schedule.every().day.at("00:12").do(job_update)
schedule.every().day.at("03:12").do(job_update)
schedule.every().day.at("06:12").do(job_update)
schedule.every().day.at("09:12").do(job_update)
schedule.every().day.at("12:12").do(job_update)
schedule.every().day.at("15:12").do(job_update)
schedule.every().day.at("18:12").do(job_update)
schedule.every().day.at("21:12").do(job_update)

while True:
    schedule.run_pending()

    time.sleep(1)
