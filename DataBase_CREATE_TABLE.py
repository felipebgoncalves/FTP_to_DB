import os
from ftplib import FTP  # FTP protocol client
import psycopg2
from dotenv import load_dotenv

load_dotenv()


# FUNÇÃO PARA CONEXÃO DO FTP
def conexao_FTP():
    ftp = FTP(os.environ.get('HOST_FTP'))
    ftp.login(user=os.environ.get('USER_FTP'), passwd=os.environ.get('PASSWD_FTP'), acct='')
    ftp.encoding = "utf-8"

    ftp.cwd('/EMA-Cepdec/')

    return ftp


# FUNÇÃO PARA CONEXÃO COM O BANCO DE DADOS
def conexao_db():
    # USUÁRIO POSTGRES - ACESSA TODOS OS BANCOS
    con = psycopg2.connect(host=os.environ.get('HOST_DATABASE'),
                           database=os.environ.get('NAME_DATABASE'),
                           user=os.environ.get('USER_DATABASE'),
                           password=os.environ.get('PASSWD_DATABASE')
                           )
    return con


# FUNÇÃO PARA CRIAÇÃO DE TABELAS NO BANCO DE DADOS
def criar_tabela(sql_CREATE):
    # CONEXÃO AO DB E CRIAÇÃO DO CURSOR
    con = conexao_db()
    cur = con.cursor()

    # EXECUÇÃO DO COMANDO SQL
    cur.execute(sql_CREATE)

    # GRAVAR
    con.commit()

    # FECHAR CONEXÃO COM O DB
    cur.close()
    con.close()

    ftp = conexao_FTP()

    for EMA in sorted(ftp.nlst()):

        sql = f'CREATE TABLE {EMA}_15m (' \
              f'data_hora_UTC varchar(25) primary key,' \
              f'id_ema varchar(15),' \
              f'frequencia_ema varchar(5),' \
              f'temp_int varchar(15),' \
              f'pressao_inst varchar(15),' \
              f'pressao_med varchar(15),' \
              f'pressao_max varchar(15),' \
              f'pressao_min varchar(15),' \
              f'temp_inst varchar(15),' \
              f'temp_med varchar(15),' \
              f'temp_max varchar(15),' \
              f'temp_min varchar(15),' \
              f'umid_rel_inst varchar(15),' \
              f'umid_rel_med varchar(15),' \
              f'umid_rel_max varchar(15),' \
              f'umid_rel_min varchar(15),' \
              f'rad_solar_glob_inst_LPPYRRA02 varchar(15),' \
              f'rad_solar_glob_med_LPPYRRA02 varchar(15),' \
              f'rad_solar_glob_max_LPPYRRA02 varchar(15),' \
              f'rad_solar_glob_min_LPPYRRA02 varchar(15),' \
              f'rad_solar_glob_inst_LPNET14 varchar(15),' \
              f'rad_solar_glob_med_LPNET14 varchar(15),' \
              f'rad_solar_glob_max_LPNET14 varchar(15),' \
              f'rad_solar_glob_min_LPNET14 varchar(15),' \
              f'rad_solar_reflet_inst varchar(15),' \
              f'rad_solar_reflet_med varchar(15),' \
              f'rad_solar_reflet_max varchar(15),' \
              f'rad_solar_reflet_min varchar(15),' \
              f'rad_iv_inst varchar(15),' \
              f'rad_iv_med varchar(15),' \
              f'rad_iv_max varchar(15),' \
              f'rad_iv_min varchar(15),' \
              f'rad_spf_inst varchar(15),' \
              f'rad_spf_med varchar(15),' \
              f'rad_spf_max varchar(15),' \
              f'rad_spf_min varchar(15),' \
              f'rad_sol_liq_inst varchar(15),' \
              f'rad_sol_liq_med varchar(15),' \
              f'rad_sol_liq_max varchar(15),' \
              f'rad_sol_liq_min varchar(15),' \
              f'temp_inst_NTC varchar(15),' \
              f'temp_med_NTC varchar(15),' \
              f'temp_max_NTC varchar(15),' \
              f'temp_min_NTC varchar(15),' \
              f'dir_vent_med varchar(15),' \
              f'dir_vent_max varchar(15),' \
              f'dir_vent_min varchar(15),' \
              f'vel_vent_med varchar(15),' \
              f'vel_vent_max varchar(15),' \
              f'vel_vent_min varchar(15),' \
              f'temp_solo_inst varchar(15),' \
              f'temp_solo_med varchar(15),' \
              f'temp_solo_max varchar(15),' \
              f'temp_solo_min varchar(15),' \
              f'umid_solo_inst varchar(15),' \
              f'umid_solo_med varchar(15),' \
              f'umid_solo_max varchar(15),' \
              f'umid_solo_min varchar(15),' \
              f'prec varchar(15)' \
              f')'


criar_tabela(sql)
