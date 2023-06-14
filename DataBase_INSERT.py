import io
import os
import time
from datetime import datetime
from ftplib import FTP, error_perm  # FTP protocol client
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
def download_FTP(ftp, filename, hora_minuto):
    r = io.BytesIO()
    ftp.retrbinary(f'RETR {filename}', r.write)

    dados = r.getvalue().decode()

    dados = dados.split(',')
    dados = [i.replace(' ', '') for i in dados]

    hora_arq = hora_minuto[0:2] + ':' + hora_minuto[2:] + ':00'

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


# ===================================================================================================================
# FUNÇÃO PRINCIPAL PARA RODAR O CÓDIGO
def job():

    hora_minuto = datetime.utcnow().strftime('%H%M')

    print('...start run...\n')

    time.sleep(300)

    # CONEXÃO COM O FTP
    ftp = conexao_FTP()

    pastas = ['EMA-Cepdec', 'EMA-Parceiros']

    for pasta in pastas:

        ftp.cwd(f'/{pasta}')

        # LOOP PARA PERCORRER TODAS AS PASTAS DAS EMAs
        for EMA in sorted(ftp.nlst()):

            try:
                # LINHA DE COMANDO PARA TROCAR O PATH
                ftp.cwd(f'/{pasta}/{EMA}')
                print(f'\n{EMA}:')

                # CONSTRUÇÃO DO NOME DO ARQUIVO PARA REALIZAÇÃO DO DOWNLOAD
                filename = EMA + '_15m_' + datetime.utcnow().strftime('%Y%m%d') + '_' + hora_minuto + '.txt'
                # filename = EMA + '_1h_' + datetime.utcnow().strftime('%Y%m%d') + '_' + hora_minuto + '.txt'
                # filename = EMA + '_24h_' + datetime.utcnow().strftime('%Y%m%d') + '_' + hora_minuto + '.txt'

                print(filename)

                dados = download_FTP(ftp, filename, hora_minuto)

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

                print(f'Download e Inserção dos Dados Concluido!')

            except error_perm as e:

                print(f'Arquivo não encontrado!')
                print(f'ERRO: {e}')

                dados = [datetime.utcnow().strftime('%Y-%m-%d') + ' ' + hora_minuto[0:2] + ':' + hora_minuto[2:] +
                         ':00', EMA, '15m']

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

                dados = [datetime.utcnow().strftime('%Y-%m-%d') + ' ' + hora_minuto[0:2] + ':' + hora_minuto[2:] +
                         ':00', EMA, '15m']

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
# =================================================================================================================
# AGENDAMENTO PARA RODAR O CÓDIGO

schedule.every().hour.at(":00").do(job)
schedule.every().hour.at(":15").do(job)
schedule.every().hour.at(":30").do(job)
schedule.every().hour.at(":45").do(job)

while True:
    schedule.run_pending()

    time.sleep(1)
