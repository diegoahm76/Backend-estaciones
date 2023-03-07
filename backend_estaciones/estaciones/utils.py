import pyodbc  # importar el módulo pyodbc para conectarse a la base de datos SQL Server
import psycopg2  # importar el módulo psycopg2 para conectarse a la base de datos PostgreSQL
import os
from estaciones.utlls_send import send_email, send_sms

# Conexión a SQL Server
def connect_to_sql_server():
    return pyodbc.connect(os.environ['MSSQL_DB_INFO'])

# Conexión a PostgreSQL
def conn_postgresq():
    return psycopg2.connect(host=os.environ['BIA_ESTACIONES_HOST'], port=os.environ['BIA_ESTACIONES_PORT'],
                            database=os.environ['BIA_ESTACIONES_NAME'], user=os.environ['BIA_ESTACIONES_USER'],
                            password=os.environ['BIA_ESTACIONES_PASSWORD'])

def envio_alertas(data):

    conn_postgresql = conn_postgresq()
    cursor = conn_postgresql.cursor()

    query_parametros= 'SELECT "T902frecuenciaSolicitudDatos", "T902temperaturaAmbienteMax", "T902temperaturaAmbienteMin", "T902humedadAmbienteMax", "T902humedadAmbienteMin", "T902presionBarometricaMax", "T902presionBarometricaMin", "T902velocidadVientoMax", "T902velocidadVientoMin", "T902direccionVientoMax", "T902direccionVientoMin", "T902precipitacionMax", "T902precipitacionMin", "T902luminosidadMax", "T902luminosidadMin", "T902nivelAguaMax", "T902nivelAguaMin", "T902velocidadAguaMax", "T902velocidadAguaMin", "T902Id_Estacion" FROM "T902ParametrosReferencia";'
    cursor.execute(query_parametros)  # Ejecutar una consulta SQL a la tabla  T902ParametrosReferencia
    resultado_parametrps= cursor.fetchall()

    query_personas = 'SELECT "T904IdPersonaEstaciones", "T904primerNombre", "T904PrimerApellido", "T904emailNotificacion", "T904nroCelularNotificacion", "T905Id_Estacion" FROM "T904PersonasEstaciones" JOIN "T905PersonasEstaciones_Estacion" ON "T905PersonasEstaciones_Estacion"."T905Id_PersonaEstaciones" = "T904PersonasEstaciones"."T904IdPersonaEstaciones";'
    cursor.execute(query_personas) # Ejecutar una consulta SQL a la tabla  T904PersonasEstaciones
    resultado_personas = cursor.fetchall()

    query_conf_alarma = 'SELECT "T906nombreVariableAlarma", "T906mensajeAlarmaMaximo", "T906mensajeAlarmaMinimo", "T906mensajeNoAlarma", "T906frecuenciaAlarma" FROM "T906ConfiguracionAlertasPersonas";'
    cursor.execute(query_conf_alarma)  # Ejecutar una consulta SQL a la tabla  T906ConfiguracionAlertasPersonas
    resultado_conf_alarma = cursor.fetchall()

    for registro in data:
        estacion = registro[10]
        parametro_estacion = [parametros for parametros in resultado_parametrps if parametros[19] == estacion]
        personas = [persona for persona in resultado_personas if persona[5] == estacion]

        # VALIDAR SI GENERAR ALERTA TEMPERATURA
        conf_alarma_tmp = [alarma for alarma in resultado_conf_alarma if alarma[0] == 'TMP']
        if registro[1] < parametro_estacion[0][2]:
            mensaje_min = conf_alarma_tmp[0][2]
            # estructura HTML para el mensaje
            mensaje_html = f"""
                <html>
                    <head></head>
                    <body>
                        <h1>Alerta de temperatura</h1>
                        <p>La temperatura superó el límite mínimo: {mensaje_min}</p>
                    </body>
                </html>
            """
            Asunto=  'Alarma temperatura'
            
            for persona in personas:
                # send_sms(persona[4],mensaje_min)
                print("PERSONA: ", persona)
                print("MSG: ", mensaje_min)
                data = {'template': mensaje_html, 'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)
                

        elif registro[1] > parametro_estacion[0][1]:
            mensaje_max = conf_alarma_tmp[0][1]
            for persona in personas:
                #print("PERSONA: ", persona)
                #print("MSG: ", mensaje_max)
                send_sms(persona[4],mensaje_max)

                data = {'template': mensaje_max, 'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass
        

    return "Envio exitoso"

def get_data_from_sql_server_estaciones():

    try:
       # Conectarse a la base de datos SQL Server
        conn_sql_server = connect_to_sql_server()
        cursor = conn_sql_server.cursor()  # Crear un cursor para realizar consultas
        # Ejecutar una consulta SQL
        cursor.execute(
            'SELECT T001fechaMod, T001nombre , T001coord1, T001coord2 FROM T001Estaciones WHERE T001transferido = 0')
        # Recuperar todos los resultados de la consulta
        datos_estacion = cursor.fetchall()
        for row in datos_estacion:  # Recorrer cada fila de los resultados
            # Actualizar una fila de la tabla
            cursor.execute(
                'UPDATE T001Estaciones SET T001transferido = 1 WHERE T001fechaMod = ? AND T001nombre = ? AND T001coord1 =? AND T001coord2=?', row)
        conn_sql_server.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_sql_server.close()  # Cerrar la conexión
        return datos_estacion
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al obtener los datos de estaciones: {e}")
        return None


def insert_data_into_postgresql_estaciones(datos_estacion):

    try:
        # Conectarse a la base de datos PostgreSQL
        conn_postgresql = conn_postgresq()
        cursor = conn_postgresql.cursor()  # Crear un cursor para realizar consultas
        cursor.executemany('INSERT INTO "T900Estaciones" ("T900fechaModificacion", "T900nombreEstacion", "T900latitud", "T900longitud") VALUES (%s,%s,%s,%s)',
                           datos_estacion)  # Insertar varias filas en la tabla
        conn_postgresql.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_postgresql.close()  # Cerrar la conexión
        print("ENTRA INSERT DATA FROM ESTACIONES")
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al insertar los datos de estaciones: {e}")


def get_data_from_sql_server_datos():

    try:
        # Conectarse a la base de datos SQL Server
        conn_sql_server = connect_to_sql_server()
        cursor = conn_sql_server.cursor()  # Crear un cursor para realizar consultas
        cursor.execute('SELECT TOP 1000 T002fecha, T002temperaturaAmbiente , T002humedadAmbiente, T002presionBarometrica, T002velocidadViento, T002direccionViento,T002precipitacion,T002luminocidad,T002nivelAgua,T002velocidadAgua,OBJECTID FROM T002Datos WHERE T002transferido = 0')  # Ejecutar una consulta SQL
        data = cursor.fetchall()  # Recuperar todos los resultados de la consulta
        for row in data:  # Recorrer cada fila de los resultados
            cursor.execute('UPDATE T002Datos SET T002transferido = 1 WHERE T002fecha =? AND T002temperaturaAmbiente =? AND T002humedadAmbiente =? AND T002presionBarometrica =? AND T002velocidadViento =? AND T002direccionViento=? AND T002precipitacion =? AND T002luminocidad =? AND T002nivelAgua =? AND T002velocidadAgua=? AND OBJECTID=?', row)  # Actualizar una fila de la tabla
        conn_sql_server.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_sql_server.close()  # Cerrar la conexión
        print("ENTRA GET DATA FROM DATOS")
        return data
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al obtener los datos de datos: {e}")
        return None


def insert_data_into_postgresql_datos(data):

    try:
        # Conectarse a la base de datos PostgreSQL
        conn_postgresql = conn_postgresq()
        cursor = conn_postgresql.cursor()  # Crear un cursor para realizar consultas
        cursor.executemany('INSERT INTO "T901Datos" ("T901fechaRegistro", "T901temperaturaAmbiente", "T901humedadAmbiente", "T901presionBarometrica", "T901velocidadViento", "T901direccionViento", "T901precipitacion", "T901luminosidad", "T901nivelAgua", "T901velocidadAgua", "T901Id_Estacion") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', data)  # Insertar varias filas en la tabla
        conn_postgresql.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_postgresql.close()  # Cerrar la conexión
        print("ENTRA INSERT DATA FROM DATOS")
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al insertar los datos de datos: {e}")


def get_data_from_sql_server_parametros():
    try:
        # Conectarse a la base de datos SQL Server
        conn_sql_server = connect_to_sql_server()
        cursor = conn_sql_server.cursor()  # Crear un cursor para realizar consultas
        cursor.execute('SELECT T003fechaMod, T003frecuencia , T003temperaturaAmbienteMax, T003temperaturaAmbienteMin, T003humedadAmbienteMax, T003humedadAmbienteMin,T003presionBarometricaMax,T003presionBarometricaMin,T003velocidadVientoMax,T003velocidadVientoMin,T003direccionVientoMax, T003direccionVientoMin,T003precipitacionMax,T003precipitacionMin,T003luminocidadMax,T003luminocidadMin,T003nivelAguaMax,T003nivelAguaMin,T003velocidadAguaMax,T003velocidadAguaMin,OBJECTID FROM T003Configuraciones WHERE T003transferido = 0')  # Ejecutar una consulta SQL
        # Recuperar todos los resultados de la consulta
        data_parametros = cursor.fetchall()
        for row in data_parametros:  # Recorrer cada fila de los resultados
            cursor.execute('UPDATE T003Configuraciones  SET T003transferido = 1 WHERE T003fechaMod =? AND T003frecuencia =? AND T003temperaturaAmbienteMax =? AND T003temperaturaAmbienteMin =? AND T003humedadAmbienteMax =? AND T003humedadAmbienteMin=? AND T003presionBarometricaMax =? AND T003presionBarometricaMin =? AND T003velocidadVientoMax =? AND T003velocidadVientoMin=? AND T003direccionVientoMax=? AND T003direccionVientoMin=? AND T003precipitacionMax=? AND T003precipitacionMin=? AND T003luminocidadMax=? AND T003luminocidadMin=? AND T003nivelAguaMax=? AND T003nivelAguaMin=? AND T003velocidadAguaMax=? AND T003velocidadAguaMin=? AND OBJECTID=? ', row)  # Actualizar una fila de la tabla
        conn_sql_server.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_sql_server.close()  # Cerrar la conexión
        print("ENTRA GET DATA FROM PARAMETROS")
        return data_parametros
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al obtener los datos de parametros: {e}")
        return None


def insert_data_into_postgresql_parametros(data_parametros):
    try:
        # Conectarse a la base de datos PostgreSQL
        conn_postgresql = conn_postgresq()
        cursor = conn_postgresql.cursor()  # Crear un cursor para realizar consultas
        cursor.executemany('INSERT INTO "T902ParametrosReferencia" ("T902fechaModificacion", "T902frecuenciaSolicitudDatos", "T902temperaturaAmbienteMax", "T902temperaturaAmbienteMin", "T902humedadAmbienteMax", "T902humedadAmbienteMin", "T902presionBarometricaMax", "T902presionBarometricaMin", "T902velocidadVientoMax", "T902velocidadVientoMin", "T902direccionVientoMax","T902direccionVientoMin" ,"T902precipitacionMax", "T902precipitacionMin", "T902luminosidadMax", "T902luminosidadMin", "T902nivelAguaMax", "T902nivelAguaMin", "T902velocidadAguaMax", "T902velocidadAguaMin", "T902Id_Estacion") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', data_parametros)  # Insertar varias filas en la tabla
        conn_postgresql.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_postgresql.close()  # Cerrar la conexión
        print("ENTRA INSERT DATA FROM PARAMETROS")
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al insertar los datos de parametros: {e}")


def get_data_from_sql_server_alertas():
    try:
        # Conectarse a la base de datos SQL Server
        conn_sql_server = connect_to_sql_server()
        cursor = conn_sql_server.cursor()  # Crear un cursor para realizar consultas
        cursor.execute("""SELECT T004descripcion, T004fecha,
    CASE 
        WHEN T004descripcion LIKE '%Conversor 1 RS485%' THEN 'Conversor 1 RS485'
        WHEN T004descripcion LIKE '%sensor meteorológico%' THEN 'sensor meteorológico'
        WHEN T004descripcion LIKE '%sensor radar%' THEN 'sensor radar'
        WHEN T004descripcion LIKE '%Voltaje%' THEN 'Voltaje'
        WHEN T004descripcion LIKE '%ángulo%' THEN 'ángulo'
        WHEN T004descripcion LIKE '%Paneles%' THEN 'Paneles'
        WHEN T004descripcion LIKE '%Baterías%' THEN 'Baterías'
        WHEN T004descripcion LIKE '%Conversor 0 RS485%' THEN 'Conversor 0 RS485'
        WHEN T004descripcion LIKE '%temperatura%' THEN 'temperatura' 
        WHEN T004descripcion LIKE '%humedad%' THEN 'humedad' 
        WHEN T004descripcion LIKE '%presión%' THEN 'presión' 
        WHEN T004descripcion LIKE '%velocidad del viento%' THEN 'velocidad del viento' 
        WHEN T004descripcion LIKE '%dirección del viento%' THEN 'dirección del viento' 
        WHEN T004descripcion LIKE '%precipitación%' THEN 'precipitación' 
        WHEN T004descripcion LIKE '%nivel del agua%' THEN 'nivel del agua' 
        WHEN T004descripcion LIKE '%velocidad del agua%' THEN 'velocidad del agua'
    END AS T004palabra ,
    OBJECTID
FROM T004Alertas WHERE T004transferido=0""")

   # Ejecutar una consulta SQL
        # Recuperar todos los resultados de la consulta
        data_parametros = cursor.fetchall()
        for row in data_parametros:  # Recorrer cada fila de los resultados
            cursor.execute('UPDATE T004Alertas SET T004transferido = 1 '
                           'WHERE T004descripcion = ? '
                           'AND T004fecha = ? AND '
                           '(CASE '
                           'WHEN T004descripcion LIKE \'%Conversor 1 RS485%\' THEN \'Conversor 1 RS485\' '
                           'WHEN T004descripcion LIKE \'%sensor meteorológico%\' THEN \'sensor meteorológico\''
                           'WHEN T004descripcion LIKE \'%sensor radar%\' THEN \'sensor radar\' '
                           'WHEN T004descripcion LIKE \'%Voltaje%\' THEN \'Voltaje\' '
                           'WHEN T004descripcion LIKE \'%ángulo%\' THEN \'ángulo\' '
                           'WHEN T004descripcion LIKE \'%Paneles%\' THEN \'Paneles\' '
                           'WHEN T004descripcion LIKE \'%Baterías%\' THEN \'Baterías\' '
                           'WHEN T004descripcion LIKE \'%Conversor 0 RS485%\' THEN \'Conversor 0 RS485\' '
                           'WHEN T004descripcion LIKE \'%temperatura%\' THEN \'temperatura\' '
                           'WHEN T004descripcion LIKE \'%humedad%\' THEN \'humedad\' '
                           'WHEN T004descripcion LIKE \'%presión%\' THEN \'presión\' '
                           'WHEN T004descripcion LIKE \'%velocidad del viento%\' THEN \'velocidad del viento\''
                           'WHEN T004descripcion LIKE \'%dirección del viento%\' THEN \'dirección del viento\''
                           'WHEN T004descripcion LIKE \'%precipitación%\' THEN \'precipitación\' '
                           'WHEN T004descripcion LIKE \'%nivel del agua%\' THEN \'nivel del agua\' '
                           'WHEN T004descripcion LIKE \'%velocidad del agua%\' THEN \'velocidad del agua\' '
                           'END) = ? AND OBJECTID = ?', row)  # Actualizar una fila de la tabla
    # Actualizar una fila de la tabla
        conn_sql_server.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_sql_server.close()  # Cerrar la conexión
        print("ENTRA GET DATA FROM ALERTAS")
        return data_parametros
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al obtener los datos de alertas: {e}")
        return None


def insert_data_into_postgresql_alertas(data_alertas):
    try:
        # Conectarse a la base de datos PostgreSQL
        conn_postgresql = conn_postgresq()
        cursor = conn_postgresql.cursor()  # Crear un cursor para realizar consultas
        cursor.executemany('INSERT INTO "T903AlertasEquipoEstacion" ("T903descripcion", "T903fechaGeneracion", "T903nombreVariable", "T903Id_Estacion") '
                           'VALUES (%s, %s, %s, %s)', data_alertas)  # Insertar varias filas en la tabla
        conn_postgresql.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_postgresql.close()  # Cerrar la conexión
        print("ENTRA INSERT DATA FROM ALERTAS")
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al insertar los datos de alertas: {e}")


def transfer_data():
    try:
        datos_estacion = get_data_from_sql_server_estaciones()  # Obtener datos de SQL Server
        data = get_data_from_sql_server_datos()  # Obtener datos de SQL Server
        # Obtener datos de SQL Server
        data_parametros = get_data_from_sql_server_parametros()
        data_alertas = get_data_from_sql_server_alertas()  # Obtener datos de SQL Server
        
        if datos_estacion:  # Si hay datos
            insert_data_into_postgresql_estaciones(
                datos_estacion)  # Ins
        if data:  # Si hay datos
            insert_data_into_postgresql_datos(
                data)  # Insertar datos en PostgreSQLertar datos en PostgreSQL

        if data_parametros:  # Si hay datos
            insert_data_into_postgresql_parametros(
                data_parametros)  # Insertar datos en PostgreSQL
        if data_alertas:  # Si hay datos
            insert_data_into_postgresql_alertas(
                data_alertas)  # Insertar datos en PostgreSQL

    except Exception as e:
        print(f"Ha ocurrido un error: {e}")


# PRUEBA

def get_data_from_postgresql():

       # Conectarse a la base de datos SQL Server
        conn_sql_server = conn_postgresq()
        cursor = conn_sql_server.cursor()  # Crear un cursor para realizar consultas
        # Ejecutar una consulta SQL
        cursor.execute(
            'SELECT "T901fechaRegistro", "T901temperaturaAmbiente", "T901humedadAmbiente", "T901presionBarometrica", "T901velocidadViento", "T901direccionViento", "T901precipitacion", "T901luminosidad", "T901nivelAgua", "T901velocidadAgua", "T901Id_Estacion" FROM public."T901Datos";')
        # Recuperar todos los resultados de la consulta
        datos_data = cursor.fetchall()
        envio_alertas(datos_data)
        return True
        print(f"Ha ocurrido un error al obtener los datos de estaciones: {e}")
        return False