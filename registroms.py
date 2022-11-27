import pandas
import pyodbc
import configparser
import logging
from datetime import date, datetime
from PyQt5 import uic, QtWidgets
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import math

app = QtWidgets.QApplication([])
tela = uic.loadUi("tela.ui")


def data_atual():
    """Retorna data atual"""
    return str(date.today().strftime("%d%m%y"))

def date_time():
    """Retorna data e hora atual"""
    return str(datetime.today().strftime("%d/%m/%Y  %H:%M:%S"))

# Cria log
log_format = '%(asctime)'
logging.basicConfig(filename='infarmaBal'+data_atual() +
                    '.log', filemode='a', level=logging.INFO)
logger = logging.getLogger('root')

logging.info(date_time()+" PROGRAMA INICIADO")

# Leitura do arquivo ini
try:
    cfg = configparser.ConfigParser()
    cfg.read('Infarma.ini')
    cod_loja = cfg.getint('SERVIDOR', 'Loja')
    hostName = cfg.get('SERVIDOR', 'HostName')
    dataBase = cfg.get('SERVIDOR', 'Database')
    driverOdbc = '{SQL Server}'
    tela.labelDataCon.setText(f'{hostName} / {dataBase} - LOJA: {cod_loja}')
    logging.info(date_time()+" LEITURA DO ARQUIVO INI REALIZADA")
except Exception as e:
    logging.info(date_time()+" LEITURA DO ARQUIVO INI ERRO")
    logging.warning(date_time()+' '+str(e))

# Leitura da planilha
try:
    df = pandas.read_excel('abcfarma.xlsx')
    logging.info(date_time()+" LEITURA DA PLANILHA REALIZADA")
except Exception as e:
    logging.info(date_time()+" LEITURA DA PLANILHA ERRO")
    logging.warning(date_time()+' '+str(e))

# realiza conexão com o banco
try:
    conn = pyodbc.connect(
        f'DRIVER={driverOdbc};SERVER={hostName};DATABASE={dataBase};UID='';PWD='';')
    cursor = conn.cursor()
    logging.info(date_time()+" CONEXAO DB REALIZADA")
except Exception as e:
    logging.info(date_time()+" CONEXAO DB ERRO")
    logging.warning(date_time()+' '+str(e))
finally:
    logging.info(date_time()+" CONEXAO DB FINALIZADA")
    conn.close()


# Funções DB
cnxn = (
    f'DRIVER={driverOdbc};SERVER={hostName};DATABASE={dataBase};UID='';PWD='';')
produtos = []
produtos_abc = []


def connect_db():
    """Connect DB"""
    try:
        conn = pyodbc.connect(
            f'DRIVER={driverOdbc};SERVER={hostName};DATABASE={dataBase};UID='';PWD='';')
        logging.info(date_time()+" CONNECT DB REALIZADA")
        return conn
    except Exception as e:
        logging.info(date_time()+" CONNECT DB ERRO")
        logging.warning(date_time()+' '+str(e))

def gerar_excel():
    """Export data to excel"""
    try:
        cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": cnxn})
        engine = create_engine(cnxn_url)

        query = ("""SELECT * FROM Produtos_PlanilhaABC""")
        atual = pandas.read_sql_query(query, engine)
        atual.to_excel("Produtos_PlanilhaABC.xlsx",
                       sheet_name='atual', index=False)

        logging.info(date_time()+" GERAR EXCEL Produtos_PlanilhaABC REALIZADA")

        query = ("""SELECT * FROM Produtos_Teste""")
        plabc = pandas.read_sql_query(query, engine)
        plabc.to_excel("Produtos_Teste.xlsx", sheet_name='plabc', index=False)

        logging.info(date_time()+" GERAR EXCEL Produtos_Teste REALIZADA")

        tela.labelInfoExcel.setText('Excel Gerado')

    except Exception as e:
        logging.info(date_time()+" GERAR EXCEL ERRO")
        logging.warning(date_time()+' '+str(e))

    finally:
        logging.info(date_time()+" GERAR EXCEL FINALIZADA")

# DM

def update_ms():
    """UPDATE PRODU SET NUM_REGMS"""
    # NUM_REGMS
    try:
        conn = connect_db()
        cursor = conn.cursor()
        sql = ("""
        SELECT T.Cod_EAN
        FROM PRODU T INNER JOIN Produtos_PlanilhaABC P
        ON T.Cod_EAN = P.Cod_EAN
        WHERE (T.NUM_REGMS IS NULL OR T.NUM_REGMS!= P.NUM_REGMS)
        AND
        LEN(P.NUM_REGMS )=13
        """)

        cursor.execute(sql)
        result = cursor.fetchall()
        produtos_ms = []
        for i in result:
            if i[0] != None:
                produtos_ms.append(int(i[0]))
            else:
                pass

        logging.info(date_time()+" PRODUTOS COM MS DIVERGENTE:" +
                     str(len(produtos_ms)))

        for ean in produtos_ms:
            sql = (f"""UPDATE PR SET PR.NUM_REGMS=PA.NUM_REGMS ,PR.Cod_AbcFar=PA.Cod_AbcFar
            FROM PRODU PR INNER JOIN Produtos_PlanilhaABC PA 
            ON PR.Cod_EAN= PA.Cod_EAN
            WHERE PR.Cod_EAN = '{ean}'
            AND 
			(
			PR.NUM_REGMS IS NULL OR
			PR.NUM_REGMS<>PA.NUM_REGMS OR
			PR.Cod_AbcFar IS NULL OR
			PR.Cod_AbcFar != PA.Cod_AbcFar
            )
            """)
            cursor.execute(sql)
            cursor.commit()
            print(sql)

        print("Concluido")
        logging.info(date_time()+" UPDATE NUM_REGMS CONCLUIDO")
        tela.labelInfoMs.setText('Update NUM_REGMS realizado')

    except Exception as e:
        logging.info(date_time()+" UPDATE NUM_REGMS ERRO")
        logging.warning(date_time()+' '+str(e))
        tela.labelInfoMs.setText('Erro Update NUM_REGMS')

    finally:
        logging.info(date_time()+" UPDATE NUM_REGMS FINALIZADA")
        cursor.close()
        conn.close()

def update_ncm():
    """UPDATE PRODU SET Cod_Ncm"""
    # NCM
    try:
        conn = connect_db()
        cursor = conn.cursor()

        sql = ("""
        SELECT T.Cod_EAN
        FROM PRODU T INNER JOIN Produtos_PlanilhaABC P
        ON T.Cod_EAN = P.Cod_EAN
        WHERE (T.Cod_Ncm IS NULL OR T.Cod_Ncm!= P.Cod_Ncm)
		AND ( 
		P.Cod_Ncm != ''
		AND P.Cod_Ncm IS NOT NULL
		AND LEN(P.Cod_Ncm)=8
		)
        """)

        cursor.execute(sql)
        result = cursor.fetchall()
        produtos_ncm = []

        for i in result:
            if i[0] != None:
                produtos_ncm.append(int(i[0]))
            else:
                pass

        logging.info(date_time()+" PRODUTOS COM NCM DIVERGENTE:" +
                     str(len(produtos_ncm)))

        for ean in produtos_ncm:
            sql = (f"""
            UPDATE PR SET PR.Cod_Ncm=PA.Cod_Ncm 
            FROM PRODU PR INNER JOIN Produtos_PlanilhaABC PA 
            ON PR.Cod_EAN= PA.Cod_EAN
            WHERE PR.Cod_EAN = '{ean}'
            AND PA.Cod_Ncm IS NOT NULL
            AND PA.Cod_Ncm !=''
            AND LEN(PA.Cod_Ncm)=8
            AND
			(
			PR.Cod_Ncm IS NULL OR
			PR.Cod_Ncm<>PA.Cod_Ncm 
            )
            """)

            cursor.execute(sql)
            cursor.commit()

        logging.info(date_time()+" UPDATE NCM CONCLUIDO")
        tela.labelInfoNcm.setText('Update NCM realizado')

    except Exception as e:
        logging.info(date_time()+" UPDATE NCM ERRO")
        logging.warning(date_time()+' '+str(e))
        tela.labelInfoNcm.setText('Erro Update NCM')

    finally:
        logging.info(date_time()+" UPDATE NCM FINALIZADA")
        cursor.close()
        conn.close()

def update_cest():
    """UPDATE PRODU SET Cod_CEST"""
    # CEST
    try:
        conn = connect_db()
        cursor = conn.cursor()

        sql = ("""
        SELECT T.Cod_EAN
        FROM PRODU T INNER JOIN Produtos_PlanilhaABC P
        ON T.Cod_EAN = P.Cod_EAN
        WHERE (T.Cod_CEST IS NULL OR T.Cod_CEST!= P.Cod_CEST)
		AND 
        (P.Cod_CEST != '' AND P.Cod_CEST IS NOT NULL AND LEN(P.Cod_CEST)=7)
        """)

        cursor.execute(sql)
        result = cursor.fetchall()
        produtos_cest = []

        for i in result:
            if i[0] != None:
                produtos_cest.append(int(i[0]))
            else:
                pass

        logging.info(date_time()+" PRODUTOS COM CEST DIVERGENTE:" +
                     str(len(produtos_cest)))

        for ean in produtos_cest:

            sql = (f"""
            UPDATE PR SET PR.Cod_CEST=PA.Cod_CEST 
            FROM PRODU PR INNER JOIN Produtos_PlanilhaABC PA 
            ON PR.Cod_EAN= PA.Cod_EAN
            WHERE PR.Cod_EAN = '{ean}'
            A.Cod_CEST IS NOT NULL AND 
            PA.Cod_CEST !='' AND 
            LEN(PA.Cod_CEST)=7
            AND
            (
			PR.Cod_CEST IS NULL OR
			PR.Cod_CEST<>PA.Cod_CEST 
            )
            
            """)

            cursor.execute(sql)
            cursor.commit()

        logging.info(date_time()+" UPDATE CEST CONCLUIDO")
        tela.labelInfoCest.setText('Update CEST realizado')

    except Exception as e:
        logging.info(date_time()+" UPDATE CEST ERRO")
        logging.warning(date_time()+' '+str(e))
        tela.labelInfoCest.setText('Erro Update CEST')

    finally:
        logging.info(date_time()+" UPDATE CEST FINALIZADA")
        cursor.close()
        conn.close()

def update_ctrlista():
    """UPDATE PRODU SET Ctr_Lista"""
    # Ctr_Lista
    try:
        conn = connect_db()
        cursor = conn.cursor()

        sql = ("""
        SELECT T.Cod_EAN
        FROM PRODU T 
		INNER JOIN Produtos_PlanilhaABC P
        ON T.Cod_EAN = P.Cod_EAN
        WHERE (T.Ctr_Lista IS NULL OR 
		T.Ctr_Lista!= 
		CASE 
		WHEN P.Ctr_Lista ='LISTA POSITIVA' THEN 'P'
		WHEN P.Ctr_Lista ='LISTA NEGATIVA' THEN 'N'
		WHEN P.Ctr_Lista ='LISTA NEUTRA' THEN 'X'
		WHEN P.Ctr_Lista ='OUTROS' THEN 'O'
		END
		)
        """)

        cursor.execute(sql)
        result = cursor.fetchall()
        produtos_ctrlista = []

        for i in result:
            if i[0] != None:
                produtos_ctrlista.append(int(i[0]))
            else:
                pass

        logging.info(
            date_time()+" PRODUTOS COM CTR LISTA DIVERGENTE:"+str(len(produtos_ctrlista)))
        for ean in produtos_ctrlista:
            sql = (f"""
            --LISTA P
            UPDATE PR SET PR.Ctr_Lista='P'
            FROM PRODU PR INNER JOIN Produtos_PlanilhaABC PA 
            ON PR.Cod_EAN= PA.Cod_EAN
            WHERE PR.Cod_EAN = '{ean}'
            PA.Ctr_Lista = 'LISTA POSITIVA'
            AND 
             (
			PR.Ctr_Lista IS NULL OR
			PR.Ctr_Lista<>PA.Ctr_Lista
            )
            
            


            --LISTA N
            UPDATE PR SET PR.Ctr_Lista='N'
            FROM PRODU PR INNER JOIN Produtos_PlanilhaABC PA 
            ON PR.Cod_EAN= PA.Cod_EAN
            WHERE PR.Cod_EAN = '{ean}'
            PA.Ctr_Lista = 'LISTA NEGATIVA'
            AND 
             (
			PR.Ctr_Lista IS NULL OR
			PR.Ctr_Lista<>PA.Ctr_Lista
            )
            


            --LISTA X
            UPDATE PR SET PR.Ctr_Lista='X'
            FROM PRODU PR INNER JOIN Produtos_PlanilhaABC PA 
            ON PR.Cod_EAN= PA.Cod_EAN
            WHERE PR.Cod_EAN = '{ean}'
            PA.Ctr_Lista = 'LISTA NEUTRA'
            AND 
             (
			PR.Ctr_Lista IS NULL OR
			PR.Ctr_Lista<>PA.Ctr_Lista
            )
            


            --LISTA O
            UPDATE PR SET PR.Ctr_Lista='O'
            FROM PRODU PR INNER JOIN Produtos_PlanilhaABC PA 
            ON PR.Cod_EAN= PA.Cod_EAN
            WHERE PR.Cod_EAN = '{ean}'
            PA.Ctr_Lista = 'OUTROS'
            AND 
             (
			PR.Ctr_Lista IS NULL OR
			PR.Ctr_Lista<>PA.Ctr_Lista
            )
            
            """)

        cursor.execute(sql)
        cursor.commit()

        logging.info(date_time()+" UPDATE Ctr_Lista CONCLUIDO")
        tela.labelInfoLista.setText('Update Ctr_Lista realizado')

    except Exception as e:
        logging.info(date_time()+" UPDATE Ctr_Lista ERRO")
        logging.warning(date_time()+' '+str(e))
        tela.labelInfoLista.setText('Erro Update Ctr_Lista')

    finally:
        logging.info(date_time()+" UPDATE Ctr_Lista FINALIZADA")
        cursor.close()
        conn.close()

# DDL


def create_tables():
    """Create tables Produtos_Teste ,Produtos_PlanilhaABC and insert into Produtos_Teste"""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        sql = ("""
            IF NOT EXISTS 
            (SELECT * FROM sysobjects WHERE NAME='Produtos_Teste' AND xtype='U')
            CREATE TABLE Produtos_Teste 
            (
	        [Cod_Produt] [int] NOT NULL,
			[Des_Produt] [varchar](40) NULL,
			[Cod_EAN] [varchar](14) NULL,
			[NUM_REGMS] [varchar](20) NULL,
			[Cod_Ncm] [varchar](12) NULL,
			[Ctr_Lista] [varchar](1) NULL,
			[Cod_AbcFar] [int] NULL,
			[Cod_CEST] [varchar](7) NULL,
			[Cod_PriAtv] [int] NULL,
	        )
            """)

        cursor.execute(sql)
        # cursor.commit()
        logging.info(date_time()+' CREATE TABLE Produtos_Teste')

        sql = (f"""INSERT INTO Produtos_Teste
              SELECT Cod_Produt ,Des_Produt,Cod_EAN,NUM_REGMS, Cod_Ncm, Ctr_Lista,Cod_AbcFar,Cod_CEST,Cod_PriAtv FROM PRODU""")

        cursor.execute(sql)
        cursor.commit()
        logging.info(date_time()+' INSERT TABLE Produtos_Teste')

        sql = ("""
            IF NOT EXISTS 
            (SELECT * FROM sysobjects WHERE NAME='Produtos_PlanilhaABC' AND xtype='U')
            CREATE TABLE Produtos_PlanilhaABC 
            (
			[Cod_EAN] [varchar](14) NULL,
			[NUM_REGMS] [varchar](20) NULL,
			[Cod_Ncm] [varchar](12) NULL,
			[Ctr_Lista] [varchar](14) NULL,
			[Cod_AbcFar] [int] NULL,
			[Cod_CEST] [varchar](7) NULL,
			[Des_PriAtv] [varchar](250) NULL,
	        )
            """)

        cursor.execute(sql)
        cursor.commit()
        logging.info(
            date_time()+" CREATE TABLE REALIZADA Produtos_PlanilhaABC")
        tela.labelInfoCreate.setText('Tabelas Criadas')

    except Exception as e:
        logging.info(date_time()+" CREATE TABLE ERRO")
        logging.warning(date_time()+' '+str(e))
        tela.labelInfoCreate.setText('Erro ao Criar Tabelas')
    finally:
        logging.info(date_time()+" CREATE TABLE FINALIZADA")
        cursor.close()
        conn.close()

def drop_tables():
    """DROP TABLES Produtos_Teste AND Produtos_PlanilhaABC"""
    try:
        conn = connect_db()
        cursor = conn.cursor()
        sql = """
            IF EXISTS 
            (SELECT * FROM sysobjects WHERE NAME='Produtos_Teste' AND xtype='U')
            DROP TABLE IF EXISTS [dbo].[Produtos_Teste]
            """
        cursor.execute(sql)
        logging.info(date_time()+' DROP TABLE 1 REALIZADO')
        sql = """
        IF EXISTS 
        (SELECT * FROM sysobjects WHERE NAME='Produtos_PlanilhaABC' AND xtype='U')
        DROP TABLE IF EXISTS [dbo].[Produtos_PlanilhaABC]
        """
        cursor.execute(sql)
        logging.info(date_time()+' DROP TABLE 2 REALIZADO')
        cursor.commit()

        tela.labelInfoDrop.setText('Drop realizado')

    except Exception as e:
        logging.info(date_time()+' DROP TABBLE ERRO')
        logging.warning(date_time()+' '+str(e))
        tela.labelInfoDrop.setText('Erro Drop')
    finally:
        logging.info(date_time()+" DROP FINALIZADA")

def insert_ncm():
    try:
        conn = connect_db()
        cursor = conn.cursor()
        lista_ncm = []
        sql = ("""
        SELECT DISTINCT(P.Cod_Ncm)  FROM Produtos_PlanilhaABC P LEFT JOIN TBNCM N
        ON P.Cod_Ncm = N.Cod_Ncm
        WHERE (P.Cod_Ncm IS NOT NULL AND P.Cod_Ncm != '' AND LEN(P.Cod_Ncm)=8)
        AND N.Cod_Ncm IS NULL
        """)
        cursor.execute(sql)
        result = cursor.fetchall()

        for ncm in result:
            lista_ncm.append(ncm[0])

        logging.info(date_time()+" NCMS A SEREM CADASTRADOS:"+str(len(lista_ncm)))

        for ncm in lista_ncm:
            notexists = (
                f"IF NOT EXISTS (SELECT 1 FROM TBNCM WHERE COD_NCM ='{ncm}') ")
            values = (
                f"'{ncm}','Outros','73','40' ,'98','8','73','4' ,'98' ,'8' ,NULL,'26.75' ,'31.50')")
            insert = """
                    INSERT INTO [dbo].[TBNCM]
                    ([Cod_Ncm],[Des_Ncm],[Cst_PisEntTri],[Cst_PisSaiTri],[Cst_PisEntNaoTri],[Cst_PisSaiNaoTri],[Cst_CofEntTri],[Cst_CofSaiTri],[Cst_CofEntNaoTri],[Cst_CofSaiNaoTri],[Cod_SeqNat],[Alq_IbptNac],[Alq_IbptImp])
                    VALUES
                    ("""
            sql = notexists + insert + values
            cursor.execute(sql)
            cursor.commit()
        
        logging.info(date_time()+" INSERT NCM REALIZADA")
        tela.labelInfoInsertNcm.setText('Insert NCM Realizado')


    except Exception as e:
        logging.info(date_time()+" INSERT NCM ERRO")
        logging.warning(date_time()+' '+str(e))
        tela.labelInfoInsertNcm.setText('Erro Insert NCM')
    finally:
        logging.info(date_time()+" INSERT NCM FINALIZADA")
        cursor.close()
        conn.close()

def insert_prod_pla():
    """Insert into Produtos_PlanilhaABC"""
    try:
        produtos_cad = 0
        produtos_not_cad = 0
        count = 0
        conn = connect_db()
        cursor = conn.cursor()

        # Realiza select dos EANs e adiciona em uma lista para adicionar apenas produtos cadastrados no banco
        sql = """SELECT Cod_EAN FROM Produtos_Teste"""
        cursor.execute(sql)
        result = cursor.fetchall()

        for i in result:
            if i[0] != None:
                produtos.append(int(i[0]))
                count += 1
            else:
                pass

        logging.info(date_time()+" RESULT EAN:"+str(count))

        for i, ean in enumerate(df['EAN']):

            num_regms = df.loc[i, 'Registro_ANVISA']
            cod_ncm = df.loc[i, 'NCM']
            ctr_lista = df.loc[i, 'Descricao_Lista']
            cod_abcfar = df.loc[i, 'ID_Produto']
            cod_cest = df.loc[i, 'CEST']
            des_priatv = df.loc[i, 'Composicao']

            if ean in produtos:
                # VALIDA NCM
                if math.isnan(cod_ncm):
                    cod_ncm = ''
                else:
                    cod_ncm = int(cod_ncm)

                # VALIDA CEST
                if math.isnan(cod_cest):
                    cod_cest = ''
                else:
                    cod_cest = int(cod_cest)

                notexists = (
                    f"IF NOT EXISTS (SELECT 1 FROM Produtos_PlanilhaABC WHERE Cod_EAN ='{ean}') ")
                values = (
                    f"'{ean}','{num_regms}','{cod_ncm}','{ctr_lista}','{cod_abcfar}','{cod_cest}','{des_priatv}')")
                insert = """INSERT INTO Produtos_PlanilhaABC ([Cod_EAN],[NUM_REGMS],[Cod_Ncm],[Ctr_Lista],[Cod_AbcFar],[Cod_CEST],[Des_PriAtv])
                            VALUES ("""

                sql = notexists + insert + values
                produtos_abc.append(ean)

                cursor.execute(sql)
                cursor.commit()
                produtos_cad += 1

            else:
                produtos_not_cad += 1

        logging.info(date_time()+" INSERT TABLE REALIZADA")

        logging.info(date_time()+" CADASTRADOS:"+str(produtos_cad))
        logging.info(date_time()+" NAO CADASTRADOS:"+str(produtos_not_cad))
        tela.labelInfoInsert.setText('Insert Realizado')

    except Exception as e:
        logging.info(date_time()+" INSERT TABLE ERRO")
        logging.warning(date_time()+' '+str(e))
        tela.labelInfoInsert.setText('Erro Insert')

    finally:
        logging.info(date_time()+" INSERT TABLE FINALIZADA")
        cursor.close()
        conn.close()


# Assign functions to buttons
tela.create.clicked.connect(create_tables)
tela.insert.clicked.connect(insert_prod_pla)
tela.insertNcm.clicked.connect(insert_ncm)

tela.btn_update_ncm.clicked.connect(update_ncm)
tela.btn_update_ms.clicked.connect(update_ms)
tela.btn_update_cest.clicked.connect(update_cest)
tela.btn_update_lista.clicked.connect(update_ctrlista)

tela.excel.clicked.connect(gerar_excel)
tela.drop.clicked.connect(drop_tables)

# Start screen
tela.show()
app.exec()
