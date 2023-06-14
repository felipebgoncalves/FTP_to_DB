import io
import os
from ftplib import FTP, error_perm
import psycopg2
import time
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
def inserir_dados(sql_INSERT):
    # CONEXÃO AO DB E CRIAÇÃO DO CURSOR
    con = conexao_db()
    cur = con.cursor()

    try:

        cur.execute(sql_INSERT)
        con.commit()

    except (Exception, psycopg2.DatabaseError) as error:

        print("Error: %s" % error)
        con.rollback()
        cur.close()

        return 1

    cur.close()


# =================================================================================================================
# =================================================================================================================
# FUNÇÃO PRINCIPAL PARA RODAR O CÓDIGO
def job_historic():
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

            dict_db = {}

            # LOOP PARA FORMAÇÃO DA LISTA DOS NOMES DOS ARQUIVOS QUE ESTÃO EM BRANCO
            for i in registros_db:
                chave = i[0]
                frequencia = i[2]
                data = i[0][:10]
                hora = i[0][11:16]
                nome_arquivo = EMA + '_' + frequencia + '_' + data[:4] + data[5:7] + data[8:10] + '_' \
                               + hora[:2] + hora[3:5] + '.txt'

                dict_db[chave] = nome_arquivo

            # keys = list(lista_db.keys())
            lista_db = set(dict_db.values())

            # LINHA DE COMANDO PARA TROCAR O PATH
            ftp.cwd(f'/{pasta}/{EMA}')
            print(f'\n{EMA}:')

            lista_ftp = ftp.nlst('*_15m_2023*.txt')
            lista_ftp = set(lista_ftp)

            lista_filenames = lista_ftp.difference(lista_db)
            lista_filenames = sorted(list(lista_filenames))

            for filename in lista_filenames:

                try:

                    dados = download_FTP(ftp, filename)

                    sql = f'INSERT INTO {EMA}_15m (' \
                          f'data_hora_UTC, ' \
                          f'id_ema, ' \
                          f'frequencia_ema, ' \
                          f'temp_int, ' \
                          f'pressao_inst, ' \
                          f'pressao_med, ' \
                          f'pressao_max, ' \
                          f'pressao_min, ' \
                          f'temp_inst, ' \
                          f'temp_med, ' \
                          f'temp_max, ' \
                          f'temp_min, ' \
                          f'umid_rel_inst, ' \
                          f'umid_rel_med, ' \
                          f'umid_rel_max, ' \
                          f'umid_rel_min, ' \
                          f'rad_solar_glob_inst_LPPYRRA02, ' \
                          f'rad_solar_glob_med_LPPYRRA02, ' \
                          f'rad_solar_glob_max_LPPYRRA02, ' \
                          f'rad_solar_glob_min_LPPYRRA02, ' \
                          f'rad_solar_glob_inst_LPNET14, ' \
                          f'rad_solar_glob_med_LPNET14, ' \
                          f'rad_solar_glob_max_LPNET14, ' \
                          f'rad_solar_glob_min_LPNET14, ' \
                          f'rad_solar_reflet_inst, ' \
                          f'rad_solar_reflet_med, ' \
                          f'rad_solar_reflet_max, ' \
                          f'rad_solar_reflet_min, ' \
                          f'rad_iv_inst, ' \
                          f'rad_iv_med, ' \
                          f'rad_iv_max, ' \
                          f'rad_iv_min, ' \
                          f'rad_spf_inst, ' \
                          f'rad_spf_med, ' \
                          f'rad_spf_max, ' \
                          f'rad_spf_min, ' \
                          f'rad_sol_liq_inst, ' \
                          f'rad_sol_liq_med, ' \
                          f'rad_sol_liq_max, ' \
                          f'rad_sol_liq_min, ' \
                          f'temp_inst_NTC, ' \
                          f'temp_med_NTC, ' \
                          f'temp_max_NTC, ' \
                          f'temp_min_NTC, ' \
                          f'dir_vent_med, ' \
                          f'dir_vent_max, ' \
                          f'dir_vent_min, ' \
                          f'vel_vent_med, ' \
                          f'vel_vent_max, ' \
                          f'vel_vent_min, ' \
                          f'temp_solo_inst, ' \
                          f'temp_solo_med, ' \
                          f'temp_solo_max, ' \
                          f'temp_solo_min, ' \
                          f'umid_solo_inst, ' \
                          f'umid_solo_med, ' \
                          f'umid_solo_max, ' \
                          f'umid_solo_min, ' \
                          f'prec) ' \
                          f'VALUES {dados}'

                    inserir_dados(sql)

                    print(filename)
                    print(f'Download e Inserção dos Dados Concluido!')
                    print()

                except error_perm as e:

                    print(f'Arquivo não encontrado!')
                    print(f'ERRO: {e}')

                    if len(filename) == 32:

                        data_hora = filename[15:19] + '-' + filename[19:21] + '-' + filename[21:23] + ' ' + \
                                    filename[24:26] + ':' + filename[26:28] + ':00'

                    else:
                        data_hora = filename[17:21] + '-' + filename[21:23] + '-' + filename[23:25] + ' ' + \
                                    filename[26:28] + ':' + filename[28:30] + ':00'

                    dados = [data_hora, EMA, '15m']

                    for i in range(0, 56):
                        dados.append('')

                    dados = tuple(dados)

                    sql = f'INSERT INTO {EMA}_15m (' \
                          f'data_hora_UTC, ' \
                          f'id_ema, ' \
                          f'frequencia_ema, ' \
                          f'temp_int, ' \
                          f'pressao_inst, ' \
                          f'pressao_med, ' \
                          f'pressao_max, ' \
                          f'pressao_min, ' \
                          f'temp_inst, ' \
                          f'temp_med, ' \
                          f'temp_max, ' \
                          f'temp_min, ' \
                          f'umid_rel_inst, ' \
                          f'umid_rel_med, ' \
                          f'umid_rel_max, ' \
                          f'umid_rel_min, ' \
                          f'rad_solar_glob_inst_LPPYRRA02, ' \
                          f'rad_solar_glob_med_LPPYRRA02, ' \
                          f'rad_solar_glob_max_LPPYRRA02, ' \
                          f'rad_solar_glob_min_LPPYRRA02, ' \
                          f'rad_solar_glob_inst_LPNET14, ' \
                          f'rad_solar_glob_med_LPNET14, ' \
                          f'rad_solar_glob_max_LPNET14, ' \
                          f'rad_solar_glob_min_LPNET14, ' \
                          f'rad_solar_reflet_inst, ' \
                          f'rad_solar_reflet_med, ' \
                          f'rad_solar_reflet_max, ' \
                          f'rad_solar_reflet_min, ' \
                          f'rad_iv_inst, ' \
                          f'rad_iv_med, ' \
                          f'rad_iv_max, ' \
                          f'rad_iv_min, ' \
                          f'rad_spf_inst, ' \
                          f'rad_spf_med, ' \
                          f'rad_spf_max, ' \
                          f'rad_spf_min, ' \
                          f'rad_sol_liq_inst, ' \
                          f'rad_sol_liq_med, ' \
                          f'rad_sol_liq_max, ' \
                          f'rad_sol_liq_min, ' \
                          f'temp_inst_NTC, ' \
                          f'temp_med_NTC, ' \
                          f'temp_max_NTC, ' \
                          f'temp_min_NTC, ' \
                          f'dir_vent_med, ' \
                          f'dir_vent_max, ' \
                          f'dir_vent_min, ' \
                          f'vel_vent_med, ' \
                          f'vel_vent_max, ' \
                          f'vel_vent_min, ' \
                          f'temp_solo_inst, ' \
                          f'temp_solo_med, ' \
                          f'temp_solo_max, ' \
                          f'temp_solo_min, ' \
                          f'umid_solo_inst, ' \
                          f'umid_solo_med, ' \
                          f'umid_solo_max, ' \
                          f'umid_solo_min, ' \
                          f'prec) ' \
                          f'VALUES {dados}'

                    inserir_dados(sql)

                except IndexError as e:

                    print(f'Arquivo vazio: {e}')

                    if len(filename) == 32:

                        data_hora = filename[15:19] + '-' + filename[19:21] + '-' + filename[21:23] + ' ' + \
                                    filename[24:26] + ':' + filename[26:28] + ':00'

                    else:
                        data_hora = filename[17:21] + '-' + filename[21:23] + '-' + filename[23:25] + ' ' + \
                                    filename[26:28] + ':' + filename[28:30] + ':00'

                    dados = [data_hora, EMA, '15m']

                    for i in range(0, 56):
                        dados.append('')

                    dados = tuple(dados)

                    sql = f'INSERT INTO {EMA}_15m (' \
                          f'data_hora_UTC, ' \
                          f'id_ema, ' \
                          f'frequencia_ema, ' \
                          f'temp_int, ' \
                          f'pressao_inst, ' \
                          f'pressao_med, ' \
                          f'pressao_max, ' \
                          f'pressao_min, ' \
                          f'temp_inst, ' \
                          f'temp_med, ' \
                          f'temp_max, ' \
                          f'temp_min, ' \
                          f'umid_rel_inst, ' \
                          f'umid_rel_med, ' \
                          f'umid_rel_max, ' \
                          f'umid_rel_min, ' \
                          f'rad_solar_glob_inst_LPPYRRA02, ' \
                          f'rad_solar_glob_med_LPPYRRA02, ' \
                          f'rad_solar_glob_max_LPPYRRA02, ' \
                          f'rad_solar_glob_min_LPPYRRA02, ' \
                          f'rad_solar_glob_inst_LPNET14, ' \
                          f'rad_solar_glob_med_LPNET14, ' \
                          f'rad_solar_glob_max_LPNET14, ' \
                          f'rad_solar_glob_min_LPNET14, ' \
                          f'rad_solar_reflet_inst, ' \
                          f'rad_solar_reflet_med, ' \
                          f'rad_solar_reflet_max, ' \
                          f'rad_solar_reflet_min, ' \
                          f'rad_iv_inst, ' \
                          f'rad_iv_med, ' \
                          f'rad_iv_max, ' \
                          f'rad_iv_min, ' \
                          f'rad_spf_inst, ' \
                          f'rad_spf_med, ' \
                          f'rad_spf_max, ' \
                          f'rad_spf_min, ' \
                          f'rad_sol_liq_inst, ' \
                          f'rad_sol_liq_med, ' \
                          f'rad_sol_liq_max, ' \
                          f'rad_sol_liq_min, ' \
                          f'temp_inst_NTC, ' \
                          f'temp_med_NTC, ' \
                          f'temp_max_NTC, ' \
                          f'temp_min_NTC, ' \
                          f'dir_vent_med, ' \
                          f'dir_vent_max, ' \
                          f'dir_vent_min, ' \
                          f'vel_vent_med, ' \
                          f'vel_vent_max, ' \
                          f'vel_vent_min, ' \
                          f'temp_solo_inst, ' \
                          f'temp_solo_med, ' \
                          f'temp_solo_max, ' \
                          f'temp_solo_min, ' \
                          f'umid_solo_inst, ' \
                          f'umid_solo_med, ' \
                          f'umid_solo_max, ' \
                          f'umid_solo_min, ' \
                          f'prec) ' \
                          f'VALUES {dados}'

                    inserir_dados(sql)

        print('====================================================================================')

    return None


# =================================================================================================================
# CHAMAMENTO DA FUNÇÃO PRINCIPAL
start = time.time()

job_historic()

stop = time.time()

tempo = round(stop - start, 2)

print(f'\nTempo de execução: {tempo} segundos')
