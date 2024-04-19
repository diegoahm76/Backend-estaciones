import schedule
import time
import pymssql  # importar el módulo pyodbc para conectarse a la base de datos SQL Server
import psycopg2  # importar el módulo psycopg2 para conectarse a la base de datos PostgreSQL
import os
from estaciones.utlls_send import send_email, send_sms
from estaciones.cron import test_cronjob
import datetime
# CARGAR VARIABLES DE ENTORNO
from dotenv import load_dotenv
load_dotenv()
# Conexión a SQL Server


def connect_to_sql_server():
    try:
        connect = pymssql.connect(server=os.environ['MSSQL_DB_SERVER'],
                                user=os.environ['MSSQL_DB_USER'],
                                password=os.environ['MSSQL_DB_PASSWORD'],
                                database=os.environ['MSSQL_DB_DATABASE'])
        print(connect)
        # cursor = connect.cursor()  # Crear un cursor para realizar consultas
        # # Ejecutar una consulta SQL
        # cursor.execute(
        #     'SELECT T001fechaMod, T001nombre , T001coord1, T001coord2 FROM T001Estaciones WHERE T001transferido = 1')
        # # Recuperar todos los resultados de la consulta
        # datos_estacion = cursor.fetchall()
        # print("DATOS_ESTACION: ", datos_estacion)
        return connect
    except Exception as e:
        print(f"Error al conectar a la base de datos SQL Server: {e}")
        return None

# Conexión a PostgreSQL

def conn_postgresq():
    try:
        connection = psycopg2.connect(host=os.environ['BIA_ESTACIONES_HOST'], port=os.environ['BIA_ESTACIONES_PORT'],
                                database=os.environ['BIA_ESTACIONES_NAME'], user=os.environ['BIA_ESTACIONES_USER'],
                                password=os.environ['BIA_ESTACIONES_PASSWORD'])
        print(connection)
        return connection
    except Exception as e:
            print(f"Error al conectar a la base de datos PostgreSQL: {e}")
            return None


def insert_data_into_postgresql_historial(data_historial):
    print("Entro a la conexion")
    try:
        # Conectarse a la base de datos PostgreSQL
        conn_postgresql = conn_postgresq()
        cursor = conn_postgresql.cursor()
        print("Conexión establecida con éxito.")
        # Insertar varias filas en la tabla
        cursor.execute('INSERT INTO "T907HistorialAlarmasEnviadas_PorEstacion" ("T907fechaHoraEnvio", "T907mensajeEnviado", "T907dirEmailEnviado", "T907nroCelularEnviado", "T907Id_Estacion", "T907Id_PersonaEstaciones") VALUES (%s,%s,%s,%s,%s,%s)', data_historial)
        conn_postgresql.commit()  # Confirmar los cambios en la base de datos
        cursor.close()  # Cerrar el cursor
        conn_postgresql.close()  # Cerrar la conexión
        print("Paso Auditoria")
        pass
    except Exception as e:
        print(f"Ha ocurrido un error al insertar los datos de estaciones: {e}")


def envio_alertas(data):

    conn_postgresql = conn_postgresq()
    print("Conexion exitosa")
    cursor = conn_postgresql.cursor()
    query_estacion = 'SELECT "T900IdEstacion", "T900nombreEstacion" FROM "T900Estaciones";'
    cursor.execute(query_estacion)
    resultado_estacion = cursor.fetchall()

    query_parametros = 'SELECT "T902frecuenciaSolicitudDatos", "T902temperaturaAmbienteMax", "T902temperaturaAmbienteMin", "T902humedadAmbienteMax", "T902humedadAmbienteMin", "T902presionBarometricaMax", "T902presionBarometricaMin", "T902velocidadVientoMax", "T902velocidadVientoMin", "T902direccionVientoMax", "T902direccionVientoMin", "T902precipitacionMax", "T902precipitacionMin", "T902luminosidadMax", "T902luminosidadMin", "T902nivelAguaMax", "T902nivelAguaMin", "T902velocidadAguaMax", "T902velocidadAguaMin", "T902Id_Estacion" FROM "T902ParametrosReferencia";'
    # Ejecutar una consulta SQL a la tabla  T902ParametrosReferencia
    cursor.execute(query_parametros)
    resultado_parametrps = cursor.fetchall()

    query_personas = 'SELECT "T904IdPersonaEstaciones", "T904primerNombre", "T904PrimerApellido", "T904emailNotificacion", "T904nroCelularNotificacion", "T905Id_Estacion" FROM "T904PersonasEstaciones" JOIN "T905PersonasEstaciones_Estacion" ON "T905PersonasEstaciones_Estacion"."T905Id_PersonaEstaciones" = "T904PersonasEstaciones"."T904IdPersonaEstaciones";'
    # Ejecutar una consulta SQL a la tabla  T904PersonasEstaciones
    cursor.execute(query_personas)
    resultado_personas = cursor.fetchall()

    query_conf_alarma = 'SELECT "T906nombreVariableAlarma", "T906mensajeAlarmaMaximo", "T906mensajeAlarmaMinimo", "T906mensajeNoAlarma", "T906frecuenciaAlarma" FROM "T906ConfiguracionAlertasPersonas";'
    # Ejecutar una consulta SQL a la tabla  T906ConfiguracionAlertasPersonas
    cursor.execute(query_conf_alarma)
    resultado_conf_alarma = cursor.fetchall()

    for registro in data:
        estacion = registro[10]
        parametro_estacion = [
            parametros for parametros in resultado_parametrps if parametros[19] == estacion]
        personas = [
            persona for persona in resultado_personas if persona[5] == estacion]
        estacion_result = [
            estaciones for estaciones in resultado_estacion if estaciones[0] == estacion]
        for estacion in estacion_result:
            nombre_estacion = estacion[1]
            id_estacion = estacion[0]
            print(nombre_estacion)
        # VALIDAR SI GENERAR ALERTA TEMPERATURA
        conf_alarma_tmp = [
            alarma for alarma in resultado_conf_alarma if alarma[0] == 'TMP']
        if registro[1] < parametro_estacion[0][2]:

            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[1]} °C '
            # estructura HTML para el mensaje
            mensaje_html = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de temperatura </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                   </body>
               </html>
            """
            Asunto = 'Alarma temperatura'

            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_min  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta de temperatura \n La estacion {nombre_estacion} emitio la siguiente alerta: {mensaje_min}')
                print("PERSONA: ", persona)
                print("MSG: ", mensaje_min)
                data = {'template': f'{persona[1]} {persona[2]} {mensaje_html}',
                        'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)

        elif registro[1] > parametro_estacion[0][1]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[1]} °C'
            mensaje_html_max = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de temperatura </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                   </body>
               </html>
            """
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_max  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta de temperatura \n La estacion {nombre_estacion} emitio la siguiente alerta:{mensaje_max}')

                data = {'template': mensaje_html_max,
                        'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass

        # VALIDAR SI GENERAR ALERTA HUMEDAD
        conf_alarma_tmp = [
            alarma for alarma in resultado_conf_alarma if alarma[0] == 'HUR']
        if registro[2] < parametro_estacion[0][4]:
            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[2]} %RH '
            # estructura HTML para el mensaje
            mensaje_html = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de humedad </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                   </body>
               </html>
            """
            Asunto = 'Alarma!!'

            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_min  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta de Humedad \n La estacion {nombre_estacion} emitio la siguiente alerta:\n{mensaje_min}')
                data = {'template': mensaje_html,
                        'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)

        elif registro[2] > parametro_estacion[0][3]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[2]} %RH'
            mensaje_html_max = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de Humedad </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                   </body>
               </html>
            """
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_max  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta de Humedad \n La estacion {nombre_estacion} emitio la siguiente alerta:\n {mensaje_max}')
                data = {'template': mensaje_html_max,
                        'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass

         # VALIDAR SI GENERAR ALERTA PRESION BAROMETRICA
        conf_alarma_tmp = [
            alarma for alarma in resultado_conf_alarma if alarma[0] == 'PRB']
        if registro[3] < parametro_estacion[0][6]:
            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[3]}hPa '
            # estructura HTML para el mensaje
            mensaje_html = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de presión barometrica </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                   </body>
               </html>
            """
            Asunto = 'Alarma!!'

            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_min  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta de presion hPa \n La estacion {nombre_estacion} emitio la siguiente alerta: \n{mensaje_min}')
                # print("PERSONA: ", persona)
                # print("MSG: ", mensaje_min)
                data = {'template': mensaje_html,
                        'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)

        elif registro[3] > parametro_estacion[0][5]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[2]} hPa'
            mensaje_html_max = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de presión barometrica </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                   </body>
               </html>
            """
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_max  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta de presion barometrica \n La estacion {nombre_estacion} emitio la siguiente alerta:\n {mensaje_max}')
                data = {'template': mensaje_html_max,
                        'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass

        #  VALIDAR SI GENERAR ALERTA VELOCIDAD DEL VIENTO
        conf_alarma_tmp = [
            alarma for alarma in resultado_conf_alarma if alarma[0] == 'VDV']
        if registro[4] < parametro_estacion[0][8]:
            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[4]} m/s '
           # estructura HTML para el mensaje
            mensaje_html = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta Velocidad del viento </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                   </body>
               </html>
            """
            Asunto = 'Alarma!!'

            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_min  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta Velocidad del viento \n La estacion {nombre_estacion} emitio una alerta:\n{mensaje_min}')
                data = {'template': mensaje_html,
                        'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)

        elif registro[4] > parametro_estacion[0][7]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[4]} m/s'
            mensaje_html_max = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta Velocidad del viento </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                   </body>
               </html>
            """
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_max  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta Velocidad del viento \n La estacion {nombre_estacion} emitio la siguiente alerta:\n {mensaje_max}')
                data = {'template': mensaje_html_max,
                        'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass

        #  VALIDAR SI GENERAR ALERTA DIRECCION DEL VIENTO
        conf_alarma_tmp = [
            alarma for alarma in resultado_conf_alarma if alarma[0] == 'DDV']
        if registro[5] < parametro_estacion[0][10]:
            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[5]} ° '
            # estructura HTML para el mensaje
            mensaje_html = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de direeción del viento </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                   </body>
               </html>
            """
            Asunto = 'Alarma!!'

            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3] # tipo de dato: string
                telefono = persona[4] # tipo de dato: string
                id_persona = int(persona[0]) # tipo de dato: integer
                mensaje = mensaje_min # tipo de dato: string
                estacion = int(id_estacion) # tipo de dato: integer

                data_historial = [fecha_actual, mensaje, correo, telefono, estacion, id_persona]
               
                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta direeción viento \n La estacion {nombre_estacion} envio una alerta:\n{mensaje_min}')
                # print("PERSONA: ", persona)
                # print("MSG: ", mensaje_min)
                data = {'template': mensaje_html,
                        'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)

        elif registro[5] > parametro_estacion[0][9]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[5]} °'
            mensaje_html_max = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de direeción del viento </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                   </body>
               </html>
            """
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3] # tipo de dato: string
                telefono = persona[4] # tipo de dato: string
                id_persona = int(persona[0]) # tipo de dato: integer
                mensaje = mensaje_max # tipo de dato: string
                estacion = int(id_estacion) # tipo de dato: integer

                data_historial = [fecha_actual, mensaje, correo, telefono, estacion, id_persona]
               
                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta direeción del viento \n La estacion {nombre_estacion} emitio la siguiente alerta:\n {mensaje_max}')
                data = {'template': mensaje_max,
                        'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass

         # VALIDAR SI GENERAR ALERTA Precipitacion
        conf_alarma_tmp = [alarma for alarma in resultado_conf_alarma if alarma[0] == 'PCT']
        if registro[6] < parametro_estacion[0][12]:
            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[6]} mm'
            # estructura HTML para el mensaje
            mensaje_html = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de Precipitación </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                   </body>
               </html>
            """
            Asunto=  'Alarma!!'

            for persona in personas:
                send_sms(persona[4],f'{persona[1]} {persona[2]}\n Alerta de Precipitación \n La estacion {nombre_estacion} emitio la siguiente alerta:\n{mensaje_min}')
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3] # tipo de dato: string
                telefono = persona[4] # tipo de dato: string
                id_persona = int(persona[0]) # tipo de dato: integer
                mensaje = mensaje_min # tipo de dato: string
                estacion = int(id_estacion) # tipo de dato: integer

                data_historial = [fecha_actual, mensaje, correo, telefono, estacion, id_persona]
               
                insert_data_into_postgresql_historial(data_historial)
                data = {'template': mensaje_html, 'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)

        elif registro[6] > parametro_estacion[0][11]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[6]} mm'
            mensaje_html_max = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de Precipitación </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                   </body>
               </html>
            """
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3] # tipo de dato: string
                telefono = persona[4] # tipo de dato: string
                id_persona = int(persona[0]) # tipo de dato: integer
                mensaje = mensaje_max # tipo de dato: string
                estacion = int(id_estacion) # tipo de dato: integer

                data_historial = [fecha_actual, mensaje, correo, telefono, estacion, id_persona]
               
                insert_data_into_postgresql_historial(data_historial)
                send_sms(persona[4], f'{persona[1]} {persona[2]}\n Alerta de Precipitación \n La estacion {nombre_estacion} emitio la siguiente alerta:\n {mensaje_max}')
                data = {'template': mensaje_html_max, 'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass

        #  VALIDAR SI GENERAR ALERTA LUMINOSIDAD
        conf_alarma_tmp = [alarma for alarma in resultado_conf_alarma if alarma[0] == 'LMN']
        if registro[7] < parametro_estacion[0][14]:
            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[7]} '
            #estructura HTML para el mensaje
            mensaje_html = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de Luminosidad </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                   </body>
               </html>
            """
            Asunto=  'Alarma!!'

            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3] # tipo de dato: string
                telefono = persona[4] # tipo de dato: string
                id_persona = int(persona[0]) # tipo de dato: integer
                mensaje = mensaje_min # tipo de dato: string
                estacion = int(id_estacion) # tipo de dato: integer

                data_historial = [fecha_actual, mensaje, correo, telefono, estacion, id_persona]
               
                insert_data_into_postgresql_historial(data_historial)
                send_sms(persona[4],f'{persona[1]} {persona[2]}\n Alerta de Luminosidad \n La estacion {nombre_estacion} emitio la siguiente alerta:\n{mensaje_min}')
                data = {'template': mensaje_html, 'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)

        elif registro[7] > parametro_estacion[0][13]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[7]} '
            mensaje_html_max = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de Luminosidad </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                   </body>
               </html>
            """
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3] # tipo de dato: string
                telefono = persona[4] # tipo de dato: string
                id_persona = int(persona[0]) # tipo de dato: integer
                mensaje = mensaje_max # tipo de dato: string
                estacion = int(id_estacion) # tipo de dato: integer

                data_historial = [fecha_actual, mensaje, correo, telefono, estacion, id_persona]
               
                insert_data_into_postgresql_historial(data_historial)
                send_sms(persona[4], f'{persona[1]} {persona[2]}\n Alerta de Luminosidad \n La estacion {nombre_estacion} emitio la siguiente alerta:\n {mensaje_max}')
                data = {'template': mensaje_html_max, 'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass

        # VALIDAR SI GENERAR ALERTA vELOCIDAD DEL AGUA
        conf_alarma_tmp = [alarma for alarma in resultado_conf_alarma if alarma[0] == 'VDA']
        if registro[8] < parametro_estacion[0][16]:
            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[8]} m/s'
            #estructura HTML para el mensaje
            mensaje_html = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta de velocidad del agua </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                   </body>
               </html>
            """
            Asunto=  'Alarma!!'
            print("Registro",registro[8])

            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3] # tipo de dato: string
                telefono = persona[4] # tipo de dato: string
                id_persona = int(persona[0]) # tipo de dato: integer
                mensaje = mensaje_min # tipo de dato: string
                estacion = int(id_estacion) # tipo de dato: integer

                data_historial = [fecha_actual, mensaje, correo, telefono, estacion, id_persona]
               
                insert_data_into_postgresql_historial(data_historial)
                send_sms(persona[4],f'{persona[1]} {persona[2]}\n Alerta velocidad agua\n La estacion {nombre_estacion} envio una alerta:\n{mensaje_min}')
                data = {'template': mensaje_html, 'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)

        elif registro[8] > parametro_estacion[0][15]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[8]} m/s'
            mensaje_html_max = f"""
                 <html>
                   <head></head>
                    <body>
                        <h3>Alerta velocidad del agua </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                   </body>
               </html>
            """
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3] # tipo de dato: string
                telefono = persona[4] # tipo de dato: string
                id_persona = int(persona[0]) # tipo de dato: integer
                mensaje = mensaje_max # tipo de dato: string
                estacion = int(id_estacion) # tipo de dato: integer

                data_historial = [fecha_actual, mensaje, correo, telefono, estacion, id_persona]
               
                insert_data_into_postgresql_historial(data_historial)
                send_sms(persona[4], f'{persona[1]} {persona[2]}\n Alerta velocidad del agua \n La estacion {nombre_estacion} emitio la siguiente alerta:\n {mensaje_max}')
                data = {'template': mensaje_html_max, 'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass

        # VALIDAR SI GENERAR ALERTA NIVEL DEL AGUA
        conf_alarma_tmp = [
            alarma for alarma in resultado_conf_alarma if alarma[0] == 'NDA']
    if len(conf_alarma_tmp) > 0:
        if registro[9] < parametro_estacion[0][18]:

            mensaje_min = f'{conf_alarma_tmp[0][2]} {registro[9]} m' if conf_alarma_tmp[0][2] else ''
            print("Mensaje min", mensaje_min)
            # estructura HTML para el mensaje
            mensaje_html = f"""
                <html>
                <head></head>
                    <body>
                        <h3>Alerta de nivel de agua </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_min} </p>
                </body>
            </html>
            """
            Asunto = 'Alarma!!'
            print("Paso Html min")

            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_min  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                sms = f'{persona[1] or ""} {persona[2] or ""}\n Alerta nivel de agua \n La estacion {nombre_estacion or ""} emitio una alerta:\n{mensaje_min or ""}'
                send_sms(persona[4], sms)
                print("envio sms min")

                # print("PERSONA: ", persona)
                # print("MSG: ", mensaje_min)
                data = {'template': mensaje_html,
                        'email_subject': Asunto, 'to_email': persona[3]}
                send_email(data)
                print("envio email min")

        elif registro[9] > parametro_estacion[0][17]:
            mensaje_max = f'{conf_alarma_tmp[0][1]} {registro[9]} m' if conf_alarma_tmp[0][1] else ''
            print("Mensaje max sms", mensaje_max)
            mensaje_html_max = f"""
                <html>
                <head></head>
                    <body>
                        <h3>Alerta de nivel de agua </h3>
                        <P>La estacion {nombre_estacion} emitio la siguiente alerta: </p>
                        <p> {mensaje_max} </p>
                </body>
            </html>
            """
            print("Paso Html max")
            for persona in personas:
                now = datetime.datetime.now()
                fecha_actual = now.strftime("%Y-%m-%d %H:%M:%S")
                correo = persona[3]  # tipo de dato: string
                telefono = persona[4]  # tipo de dato: string
                id_persona = int(persona[0])  # tipo de dato: integer
                mensaje = mensaje_max  # tipo de dato: string
                estacion = int(id_estacion)  # tipo de dato: integer

                data_historial = [fecha_actual, mensaje,
                                  correo, telefono, estacion, id_persona]

                insert_data_into_postgresql_historial(data_historial)
                send_sms(
                    persona[4], f'{persona[1]} {persona[2]}\n Alerta nivel de agua \n La estacion {nombre_estacion} emitio la siguiente alerta:\n {mensaje_max}')
                print("envio sms min")
                data = {'template': mensaje_html_max,
                        'email_subject': 'Alarma', 'to_email': persona[3]}
                send_email(data)
                print("envio email max")
        else:
            # mensaje_no = conf_alarma_tmp[0][3]
            # for persona in personas:
            #     send_sms(persona[4],mensaje_no)

            #     data = {'template': mensaje_no, 'email_subject': 'Alarma', 'to_email': persona[3]}
            #     send_email(data)
            pass
    else:
        # En caso de que no haya alarmas, no hacemos nada
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
        cursor.execute('INSERT INTO "T900Estaciones" ("T900fechaModificacion", "T900nombreEstacion", "T900latitud", "T900longitud") VALUES (%s,%s,%s,%s)',
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
        print("Conectado a SQL Server")
        cursor = conn_sql_server.cursor()  # Crear un cursor para realizar consultas
        cursor.execute('SELECT TOP 500 T002fecha, T002temperaturaAmbiente , T002humedadAmbiente, T002presionBarometrica, T002velocidadViento, T002direccionViento,T002precipitacion,T002luminocidad,T002nivelAgua,T002velocidadAgua,OBJECTID FROM T002Datos WHERE T002transferido = 0')  # Ejecutar una consulta SQL
        data = cursor.fetchall()  # Recuperar todos los resultados de la consulta
        # Convertir data a una cadena de texto para evitar la concatenación con un valor nulo
        print("Datos obtenidos:", str(data))
        # envio_alertas(data)
        print("Alertas enviadas")
        for row in data:  # Recorrer cada fila de los resultados
            cursor.execute('UPDATE T002Datos SET T002transferido = 1 WHERE T002fecha = %s AND T002temperaturaAmbiente = %s AND T002humedadAmbiente = %s AND T002presionBarometrica = %s AND T002velocidadViento = %s AND T002direccionViento = %s AND T002precipitacion = %s AND T002luminocidad = %s AND T002nivelAgua = %s AND T002velocidadAgua = %s AND OBJECTID = %s',
                           (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))
        conn_sql_server.commit()  # Confirmar los cambios en la base de datos
        print("Datos actualizados")
        cursor.close()  # Cerrar el cursor
        conn_sql_server.close()  # Cerrar la conexión
        print("Conexión cerrada")
        return data
    except Exception as e:
        print(f"Ha ocurrido un error al obtener los datos de datos: {e}")
        return None


def insert_data_into_postgresql_datos(data):

    try:
        # Conectarse a la base de datos PostgreSQL
        print("entro a postgres")
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
        # Ejecutar una consulta SQL
        cursor.execute("""SELECT TOP 100 T004descripcion, T004fecha,
    CASE 
        WHEN T004descripcion LIKE '%Conversor 1 RS485%' THEN 'Conversor 1 RS485'
        WHEN T004descripcion LIKE '%Conversor RS485%' THEN 'Conversor RS485'
        WHEN T004descripcion LIKE '%sensor meteorologico%' THEN 'sensor meteorologico'
        WHEN T004descripcion LIKE '%sensor radar%' THEN 'sensor radar'
        WHEN T004descripcion LIKE '%luminosidad%' THEN 'luminosidad'
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
        WHEN T004descripcion LIKE '%precipitacion%' THEN 'precipitacion' 
        WHEN T004descripcion LIKE '%nivel del agua%' THEN 'nivel del agua' 
        WHEN T004descripcion LIKE '%velocidad del agua%' THEN 'velocidad del agua'
    END AS T004palabra ,
    OBJECTID
FROM T004Alertas WHERE T004transferido=0""")
   # Ejecutar una consulta SQL
        # Recuperar todos los resultados de la consulta
        data_parametros = cursor.fetchall()
        print("data parametros ", data_parametros)
        for row in data_parametros:
            cursor.execute("""UPDATE T004Alertas SET T004transferido = 1
                WHERE T004descripcion = %s
                AND T004fecha = %s AND OBJECTID = %s""", (row[0], row[1], row[3]))

        conn_sql_server.commit()
        cursor.close()
        conn_sql_server.close()
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


def get_data_from_postgresql():

    # Conectarse a la base de datos SQL Server
    conn_sql_server = connect_to_sql_server()
    print("entro a mssql")
    cursor = conn_sql_server.cursor()  # Crear un cursor para realizar consultas
    cursor.execute('SELECT TOP 1000 T002fecha, T002temperaturaAmbiente , T002humedadAmbiente, T002presionBarometrica, T002velocidadViento, T002direccionViento,T002precipitacion,T002luminocidad,T002nivelAgua,T002velocidadAgua,OBJECTID FROM T002Datos WHERE T002transferido = 0')
    # Recuperar todos los resultados de la consulta
    datos_data = cursor.fetchall()
    envio_alertas(datos_data)
    return True
    print(f"Ha ocurrido un error al obtener los datos de estaciones: {e}")
    return False


def enviar_alertas():
    try:
        # Conectarse a la base de datos SQL Server
        conn_sql_server = connect_to_sql_server()
        print("Conectado a SQL Server")
        cursor = conn_sql_server.cursor()  # Crear un cursor para realizar consultas
        cursor.execute('SELECT TOP 200 T002fecha, T002temperaturaAmbiente , T002humedadAmbiente, T002presionBarometrica, T002velocidadViento, T002direccionViento,T002precipitacion,T002luminocidad,T002nivelAgua,T002velocidadAgua,OBJECTID FROM T002Datos WHERE T002transferido = 0')  # Ejecutar una consulta SQL
        data = cursor.fetchall()  # Recuperar todos los resultados de la consulta
        # Convertir data a una cadena de texto para evitar la concatenación con un valor nulo
        print("Datos obtenidos:", str(data))
        envio_alertas(data)
        conn_sql_server.commit()  # Confirmar los cambios en la base de datos
        print("Datos actualizados")
        cursor.close()  # Cerrar el cursor
        conn_sql_server.close()  # Cerrar la conexión
        print("Conexión cerrada")
        return data
    except Exception as e:
        print(f"Ha ocurrido un error al obtener los datos de datos: {e}")
        return None


def enviar_aleryas():
    prueba = get_data_from_postgresql()
    print("Entro a la alerta")
    if prueba:
        return prueba


def transfer_data_datos():
    try:
        print("Entro a la migracion datos")
        # datos_estacion = get_data_from_sql_server_estaciones()  # Obtener datos de SQL Server
        data = get_data_from_sql_server_datos()  # Obtener datos de SQL Server
        # Obtener datos de SQL Server
        # data_parametros = get_data_from_sql_server_parametros()

        # if datos_estacion:  # Si hay datos
        #     insert_data_into_postgresql_estaciones(
        #         datos_estacion)  # Ins

        if data:  # Si hay datos
            # Insertar datos en PostgreSQLertar datos en PostgreSQL
            insert_data_into_postgresql_datos(data)

        # if data_parametros:  # Si hay datos
        #     insert_data_into_postgresql_parametros(
        #         data_parametros)  # Insertar datos en PostgreSQL

    except Exception as e:
        print(f"Ha ocurrido un error: {e}")


def transfer_data_alertas():
    try:
        print("Entro a la migracion alertas")

        data_alertas = get_data_from_sql_server_alertas()  # Obtener datos de SQL Server

        if data_alertas:  # Si hay datos
            insert_data_into_postgresql_alertas(
                data_alertas)  # Insertar datos en PostgreSQL

    except Exception as e:
        print(f"Ha ocurrido un error: {e}")

# connect_to_sql_server()

schedule.every(5).minutes.do(transfer_data_datos)
schedule.every(5).minutes.do(transfer_data_alertas)
# schedule.every(5).minutes.do(enviar_alertas)

while True:  # Ciclo principal del programa
    schedule.run_pending()  # Ejecutar tareas pendientes en el horario programado
    # Dormir el programa durante un segundo para evitar un uso excesivo de CPU
    time.sleep(5)
