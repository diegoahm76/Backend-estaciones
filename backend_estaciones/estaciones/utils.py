import pyodbc  # importar el módulo pyodbc para conectarse a la base de datos SQL Server
import psycopg2  # importar el módulo psycopg2 para conectarse a la base de datos PostgreSQL
import os

# Conexión a SQL Server
def connect_to_sql_server():
    return pyodbc.connect(os.environ['MSSQL_DB_INFO'])

# Conexión a PostgreSQL
def conn_postgresq():
    return psycopg2.connect(host=os.environ['BIA_ESTACIONES_HOST'], port=os.environ['BIA_ESTACIONES_PORT'],
                            database=os.environ['BIA_ESTACIONES_NAME'], user=os.environ['BIA_ESTACIONES_USER'],
                            password=os.environ['BIA_ESTACIONES_PASSWORD'])

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