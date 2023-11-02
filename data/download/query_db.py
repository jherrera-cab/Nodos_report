import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from datetime import datetime
from print_test import print_test
from data.procesed.save_df_query import save_query
import os


    
def read_SQL(cartera=None, date_variables=None, month_report=None, faltantes=None):
    def credentialGet():
        
        # Asignación de variables para la conexion de la base SQL
        user = os.environ.get("user_server")
        if user in os.environ:
            user = os.environ["user_server"]

        password = os.environ.get("password-server")
        if password in os.environ:
            password = os.environ["password-server"]

        server = "SYNBOG-DESK-148"
        database = "Estrategia"

        email_password = os.environ.get("email_password")
        if email_password in os.environ:
            email_password = os.environ["email_password"]

        return (user, password, server, database, email_password)

    def connectSQL(user, password, server, database):
        engine = create_engine(
            f"mssql+pyodbc://{server}/{database}?UID={user};PWD={password}&driver=ODBC+Driver+17+for+SQL+Server"
        )
        connection = engine.connect()
        return (engine, connection)

    #df_goals
    def read_db_0(cartera):
            x=1
            query_goals = db.text(
                """
                SELECT 
                    [id]
                    ,[nombre]
                    ,[coordinador_nacional]
                    ,[correo_nacional]
                    ,[director]
                    ,[correo_director]
                    ,[meta_gestion]
                    ,[meta_contacto]
                    ,[meta_promesas]
                    ,[peso_gestion]
                    ,[peso_contacto]
                    ,[peso_promesas]
                FROM [Estrategia].[dbo].[Metas]
                where nombre = :entidad
                                    """
            )
            query_goals = query_goals.bindparams(entidad=cartera)
            result = connection.execute(query_goals)
            df_goals = pd.DataFrame(result)
            save_query(df=df_goals, name=f'df_goals-{cartera}', folder=folder)
    
    #df_gestion_month           
    def read_db_1(cartera):
        
        date_start_formated, date_end_formated = date_variables['date_init_month_n3'].strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d")
        query = db.text(
            """ SET DATEFIRST 1;
                SELECT		
                    A.ACCOUNT_NUMBER,
                    A.ID_GESTION,
                    A.ENTIDAD_ID,
                    A.IDENTIFICACION,
                    A.HISTORY_DATE,
                    A.HISTORY_DATE_END,
                    CAST(HISTORY_DATE AS DATE) AS FECHA,
                    DATEPART(MONTH, HISTORY_DATE) AS MES,
                    DATEPART(ISO_WEEK, HISTORY_DATE) AS SEMANA,
                    DATEPART(WEEKDAY, HISTORY_DATE) - 1 AS DIA_SEMANA,
                    ID_ACCION,
                    ID_EFECTO,
                    C.JERARQUIA,
                    C.TIEMPO_GESTION AS META_TIEMPO_GESTION,
                    C.TIPO_CONTACTO,
                    C.AGENDA,
                    DATEDIFF(SECOND,A.HISTORY_DATE, HISTORY_DATE_END) AS AHT,
                    CASE
                        WHEN DATEDIFF(SECOND,A.HISTORY_DATE, HISTORY_DATE_END) - Tiempo_Gestion > 0 THEN 
                            DATEDIFF(SECOND,A.HISTORY_DATE, HISTORY_DATE_END) - Tiempo_Gestion
                    END AS ACW,
                    ID_CONTACTO,
                    A.OBSERVACION,
                    MONEY1,
                    MONEY2,
                    TEXT6,
                    TEXT8,
                    TEXT6 AS TELEPHONE,
                    B.NOMBRE,
                    B.COORDINADORA,
                    B.SUCURSAL,
                    B.FECHA_INGRESO,
                    
                    concat(B.NOMBRE, MONTH(A.HISTORY_DATE)) AS llave
                FROM		[dbo].[Gestiones] AS A
                INNER JOIN	[dbo].[Nomina]	  AS B ON A.GESTOR_ID = B.[Usuario Sinfin 1]
                INNER JOIN	[dbo].[Tipo_Contacto] AS C ON A.ID_EFECTO = C.EFECTO
                WHERE		CAST(HISTORY_DATE AS DATE) BETWEEN :date_start AND :date_end and B.ESTADO_CONTRATO = 'ACTIVO' AND B.CARGO='GESTOR' AND ENTIDAD_ID= :entidad
                ORDER BY	HISTORY_DATE DESC
                        """
        )
        query = query.bindparams(date_start=date_start_formated, date_end=date_end_formated, entidad=cartera)
        result = connection.execute(query)
        df_gestion_month = pd.DataFrame(result)
        save_query(df=df_gestion_month, name=f'df_gestion_month-{cartera}', folder=folder)
    
    #df_promises    
    def read_db_2(cartera):
# region LECTURA PROMESAS MES EN CURSO 
        date_start_formated, date_end_formated = date_variables['date_init_month_n3'].strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d")
        query_promises = db.text(
            """ SET DATEFIRST 1;
                SELECT		
                    A.ACCOUNT_NUMBER,
                    A.ID_GESTION,
                    A.ENTIDAD_ID,
                    A.IDENTIFICACION,
                    A.HISTORY_DATE,
                    A.GESTOR_ID,
                    A.PROMISE_NEXT_DATE,
                    A.PROMISE_NEXT_AMOUNT,
                    CAST(HISTORY_DATE AS DATE) AS FECHA,
                    DATEPART(MONTH, HISTORY_DATE) AS MES,
                    DATEPART(ISO_WEEK, HISTORY_DATE) AS SEMANA,
                    DATEPART(WEEKDAY, HISTORY_DATE) - 1 AS DIA_SEMANA,
                    A.PROMISE_STATE,
                    A.PROMISE_TYPE,
                    B.NOMBRE,
                    B.COORDINADORA,
                    B.SUCURSAL,
                    B.FECHA_INGRESO,
                    concat(B.NOMBRE, MONTH(A.HISTORY_DATE)) AS llave
                FROM		[dbo].[Promesas]  AS A
                INNER JOIN	[dbo].[Nomina]	  AS B ON A.GESTOR_ID = B.[Usuario Sinfin 1]
                WHERE		CAST(HISTORY_DATE AS DATE) BETWEEN :date_start AND :date_end and B.ESTADO_CONTRATO = 'ACTIVO' AND B.CARGO='GESTOR' AND ENTIDAD_ID= :entidad
                ORDER BY	HISTORY_DATE DESC 

                        """
        )
        query_promises = query_promises.bindparams(date_start=date_start_formated, date_end=date_end_formated, entidad=cartera)
        result = connection.execute(query_promises)
        df_promises = pd.DataFrame(result)
        save_query(df=df_promises, name=f'df_promises-{cartera}', folder=folder)
# endregion

    #df_efect
    def read_db_3(cartera):
# region LECTURA MEJOR ULTIMO EFECTO
        query_efect = db.text(
            """
            SELECT *
            FROM [Estrategia].[dbo].[Ultimo_Mejor_Efecto]
            WHERE   ENTIDAD_ID = :entidad
                                """
        )
        query_efect = query_efect.bindparams(entidad=entidad_sfects)
        result      = connection.execute(query_efect)
        df_efect    = pd.DataFrame(result)
        save_query(df=df_efect, name=f'df_efect-{cartera}', folder=folder)
# endregion
    
    #df_seguimientos_promesas
    def read_db_4(cartera):
# region LECTURA SEGUIMIENTO DE PROMESAS
            
        query_seguimientos_promesas = db.text(
            """
            SELECT 
                    [ENTIDAD_ID]
                    ,[ACCOUNT_NUMBER]
                    ,[HISTORY_DATE]
                    ,[GESTOR_ID]
                    ,[PROMISE_NEXT_AMOUNT]
                    ,[PROMISE_NEXT_DATE]
                    ,[PROMISE_STATE]
                    ,[Q_SEGUIMIENTOS]
                    ,[Q_SEGUIMIENTOS_SEMANA]
                    ,[Q_SEGUIMIENTOS_MES]
            FROM [Estrategia].[dbo].[Seguimiento_Promesas]
            WHERE ENTIDAD_ID =:entidad
                                """
        )
        query_seguimientos_promesas = query_seguimientos_promesas.bindparams(entidad=entidad_sfects)
        result      = connection.execute(query_seguimientos_promesas)
        df_seguimientos_promesas    = pd.DataFrame(result)
        save_query(df=df_seguimientos_promesas, name=f'df_seguimientos_promesas-{cartera}', folder=folder)

# endregion
    
    #df_master_aux
    def read_db_5(cartera):
        # region LECTURA MESTRO AUX 
        query_aux_logueo= db.text(
            """
            SELECT	
                    CAST(FECHA AS date) AS DATE,
                    CONCAT(DATEPART(HOUR, FECHA), ':', RIGHT('0' + CAST(DATEPART(MINUTE, FECHA) AS VARCHAR), 2)) AS HOUR,
                    MONTH(FECHA) AS MONTH,
                    DATEPART(ISO_WEEK, FECHA) AS NUM_WEEK,
                    DATEPART(DW, FECHA) AS NUM_DAY,
                    COORDINADORA,
                    Nombre AS NOMBRE,
                    ESTADOANTERIOR as AUX,
                    SUM(TIEMPO) / 60000 AS TIME
            FROM	Maestro_aux as a
            inner join [dbo].[Nomina] as b on A.gestor_id = b.[Usuario Sinfin 1]
            WHERE	ENTIDAD_ID= :entidad AND TIEMPO IS NOT NULL AND MONTH(FECHA)= :month_filter
            GROUP BY ESTADOANTERIOR, FECHA, nombre, COORDINADORA
            ORDER BY  FECHA ASC, nombre DESC
            """
        )   

        query_aux_logueo=query_aux_logueo.bindparams(entidad=cartera, month_filter=month_report)
        result =connection.execute(query_aux_logueo)
        df_master_aux=pd.DataFrame(result)
        save_query(df=df_master_aux, name=f'df_master_aux-{cartera}', folder=folder)
        
  
        # endregion
    
    
    if cartera == 'NATURA2':
        entidad_sfects = 'NATURA'
    else:
        entidad_sfects = cartera
    
    folder='procesed\df'   
    date_start  =   date_variables['date_init_month']
    date_end    =   date_variables['date_finish_month']
    user, password, server, database, email_password = credentialGet()
    engine, connection = connectSQL(user, password, server, database)
 
    
    functions={
        0   :   read_db_0,
        1   :   read_db_1,
        2   :   read_db_2,
        3   :   read_db_3,
        4   :   read_db_4,
        5   :   read_db_5,
    }
    
    
    for i in faltantes:
        functions[i](cartera)
        
    
