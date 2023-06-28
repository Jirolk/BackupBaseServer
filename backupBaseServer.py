
from dotenv import load_dotenv
import os
import psycopg2
import mysql.connector
import subprocess
import gzip
from datetime import datetime, timedelta
import logging
import shutil
import importlib
from byte_to_megabytes import *
importlib.reload(mysql.connector)
import platform


# obtenemos la fecha actual
fecha = datetime.today().strftime("%Y_%m_%d")


class ConexionPostgreSQL:
    def __init__(self):
        load_dotenv()
        self.ruta = os.getenv("BACKUP_DIR")
        self.dia = int(os.getenv('DIA'))
        self.conexion = None
        #Log de las backup hechas
        logging.basicConfig(filename='log_Backup.txt', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    def conectarPG(self):
        host = os.getenv("pDATABASE_HOST")
        dbname=os.getenv("pDATABASE_NAME")
        user = os.getenv("pDATABASE_USER")
        password = os.getenv("pDATABASE_PASSWORD")
        port = os.getenv("pDATABASE_PORT")
        print("Postgres conexión:...")
        try:
            self.conexion = psycopg2.connect(
                f"host={host} dbname={dbname} user={user} password={password} port={port}")
            print('Conexión exitosa')
            return True
        except (Exception, psycopg2.Error)  as error:
            logging.error(f"Fallo con la conexión: {error}")
            print(f'Falla con la conexión: {error}')
        return False   

    def listar_bd(self):
        # Crear un cursor para ejecutar consultas
        cur = self.conexion.cursor()
        # Ejecutar una consulta SQL para obtener los nombres de todas las bases de datos
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        # Obtener los resultados de la consulta y la guardamos para poder recorrer luego
        db_list = [row[0] for row in cur.fetchall()]
        db_list = list(filter(lambda x: x.endswith("metropolis"), db_list))
        # rows = cur.fetchall()
        # # Imprimir los nombres de las bases de datos
        print("Bases de datos disponibles:", len(db_list))
        # for row in rows:
        #     print("- ", row[0])
        # Cerrar el cursor
        cur.close()
        return db_list

    def respaldar(self, db_list):
        # Establecer la variable de entorno PGPASSWORD con la contraseña
        os.environ['PGPASSWORD'] = os.getenv('DATABASE_PASSWORD')
        
        ##########################################
        
        # Determinar el sistema operativo actual
        operating_system = platform.system()

        # Obtener la ruta al ejecutable pg_dump según el sistema operativo
        if operating_system == "Windows":
            pg_dump_executable = "pg_dump.exe"  # Ajusta esto según la ubicación de tu ejecutable pg_dump en Windows
        else:
            pg_dump_executable = "pg_dump"

        # Obtener la ruta completa al ejecutable pg_dump
        pg_dump_path = shutil.which(pg_dump_executable)

        if pg_dump_path is None:
            print(f"El ejecutable '{pg_dump_executable}' no se encontró en la ruta")
        

        ##########################################

        carpeta = os.path.join(self.ruta,"postgresSql", fecha)
        os.makedirs(carpeta, exist_ok=True)
        password = os.getenv('pDATABASE_PASSWORD')
       
        peso_bytes=0 #Inicializamos para medir si realizó la backup
        peso_megabytes=0
        print("Empecemos... a respaldar BD postgres\n")

        for db in db_list:
            # archivo = carpeta+"/"+ f"{db}.sql.gz"
            archivo = os.path.join(carpeta, f"{db}.sql.gz")
            #comando para el respaldo de la BD postgres
            cmd = [
                #'pg_dump',
                pg_dump_path,
                '-h', os.getenv('pDATABASE_HOST'),
                '-U', os.getenv('pDATABASE_USER'),
                # '-W', os.getenv('DATABASE_PASSWORD'),
                '-F', 'p',
                '-w', db,
                '|', 'gzip', '>', archivo
            ]

            try:
                with subprocess.Popen(" ".join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
                    stdout, stderr = proc.communicate()
                    if stderr:
                        logging.error(f"Error al ejecutar pg_dump: {stderr.decode('utf-8')}")
                        print("Error al ejecutar pg_dump:", stderr.decode('utf-8'))
            except subprocess.CalledProcessError as e:
                logging.error(f"Ocurrió un error en subprocess: {e}")
                print("Ocurrió un error en subprocess:", e)


            # Verificar el tamaño del archivo de respaldo   
            if os.path.exists(archivo):
                peso_bytes += os.path.getsize(archivo)
                peso_megabytes = bytes_to_megabytes(peso_bytes)  
        
        print(f"Tamaño procesado: {peso_bytes} bytes | {peso_megabytes} Mb")
        if peso_megabytes>0.01:
            print(f"\nBackup de la BD completado con exito en fecha: {datetime.now()}")
            print(f"La Backup tiene un tamaño de {peso_megabytes:.2f} MB.")        
            logging.info(f"Postgres:Backup de la BD completado con exito. Cant:{len(db_list)}")
        else:
            logging.error("Postgres: No se pudo generar el archivo de respaldo")
            print("No se pudo generar el archivo de respaldo.")

       
        # Eliminar la variable de entorno PGPASSWORD después de su uso
        del os.environ['PGPASSWORD']


    def eliminarViejos(self):
        
        fecha_limite= datetime.now() - timedelta(days=self.dia)
        for nombreCarpeta in os.listdir(os.path.join(self.ruta, 'postgresSql')):
            
            if nombreCarpeta.startswith('20'):
                fechaCarpeta = datetime.strptime(nombreCarpeta, '%Y_%m_%d')               
            if fechaCarpeta < fecha_limite:
                try:
                    shutil.rmtree(os.path.join(self.ruta,"postgresSql", nombreCarpeta))
                    print('Se ha eliminado la carpeta:', nombreCarpeta)
                    logging.warning(f"'Se ha eliminado la carpeta:', {nombreCarpeta}")
                except OSError as e:
                    print(f'Error: {e} - La carpeta no se ha eliminado')
                    logging.error(f"{e} - La carpeta no se ha eliminado:', {nombreCarpeta}")
            else:
                print(f"La carpeta {nombreCarpeta} no fue borrado")
            
    def __del__(self):

        if self.conexion:
            self.conexion.close()
        
class ConexionMySQL():
    def __init__(self):
        load_dotenv()
        self.ruta = os.getenv("BACKUP_DIR")
        self.dia = int(os.getenv('DIA'))
        self.con= None
        #Log de las backup hechas
        logging.basicConfig(filename='log_Backup.txt', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
        
    
        host = os.getenv("DATABASE_HOST")
        user = os.getenv("DATABASE_USER")
        password = os.getenv("DATABASE_PASSWORD")    
        print("MySQl conexión:...")
        try:
            self.con = mysql.connector.connect(
                host=host,
                user=user,
                password=password)
            print("Conexión exitosa")
            # return True
        except Exception as e:
            logging.error(f"Fallo con la conexión: {e}")
            print(f"Falla la conexión: {e}")
        # return False

    def listar_bd(self):
        try:
            cur = self.con.cursor()
            # Ejecutar una consulta SQL
            cur.execute("show databases")
            
            # Obtener los resultados de la consulta
            db_list = [row[0] for row in cur.fetchall()]
            # Filtrar archivos que terminan con "_FE"
            archivos_FE = list(filter(lambda x: x.endswith("_fe"), db_list))
            cur.close()
            return archivos_FE
        except Exception as e:
            print(f"Error al listar bases de datos: {e}")


    def respaldar(self, archivos_FE):
        peso_bytes=0 #Inicializamos para medir si realizó la backup
        peso_megabytes =0
        print("Empecemos... a respaldar BD mysql\n")

        for db in archivos_FE:
            carpeta=os.path.join(self.ruta,"mysql", fecha)
            os.makedirs(carpeta, exist_ok = True)
            # archivo = carpeta+"/"+ f"{db}.sql.gz"
            archivo=os.path.join(carpeta, f"{db}.sql.gz")
            password = os.getenv("DATABASE_PASSWORD")
            # comando para el respaldo de la BD Mysql
            cmd=[
                'mysqldump',
                '-h', os.getenv('DATABASE_HOST'),
                '-u', os.getenv('DATABASE_USER'),
                '--password='+ os.getenv('DATABASE_PASSWORD'),
                db,
                '|', 'gzip', '>', archivo
                ]

            with subprocess.Popen(" ".join(cmd), stdin=subprocess.PIPE, shell=True) as proc:
                proc.communicate(input=password.encode())

            # Verificar el tamaño del archivo de respaldo
            if os.path.exists(archivo):
                peso_bytes += os.path.getsize(archivo)
                peso_megabytes = bytes_to_megabytes(peso_bytes) 
        
    
        print(f"Tamaño procesado: {peso_bytes} bytes | {peso_megabytes} Mb")
        if peso_megabytes>0.01:
            print(f"\nBackup de la BD completado con exito en fecha: {datetime.now()}")
            print(f"La Backup tiene un tamaño de {peso_megabytes:.2f} MB.")        
            logging.info(f"Mysql:Backup de la BD completado con exito. Cant:{len(archivos_FE)}")
        else:         
            logging.error(f"Mysql: No se pudo generar el archivo de respaldo")
            print("No se pudo generar el archivo de respaldo.")
                        

    def eliminarViejos(self):
        
        fecha_limite= datetime.now() - timedelta(days=self.dia)
        for nombreCarpeta in os.listdir(os.path.join(self.ruta, 'mysql')):
            
            if nombreCarpeta.startswith('20'):
                fechaCarpeta = datetime.strptime(nombreCarpeta, '%Y_%m_%d')               
            if fechaCarpeta < fecha_limite:
                try:
                    shutil.rmtree(os.path.join(self.ruta,"mysql", nombreCarpeta))
                    print('Se ha eliminado la carpeta:', nombreCarpeta)
                    logging.warning(f"'Se ha eliminado la carpeta:', {nombreCarpeta}")
                except OSError as e:
                    print(f'Error: {e} - La carpeta no se ha eliminado')
                    logging.error(f"{e} - La carpeta no se ha eliminado:', {nombreCarpeta}")
            else:
                print(f"La carpeta {nombreCarpeta} no fue borrado")

            
    def __del__(self):

        if self.con:
            self.con.close()


# Crear una instancia de la clase y llamar al método para listar las bases de datos
postgresql = ConexionPostgreSQL()
if postgresql.conectarPG():
    bd = postgresql.listar_bd()
    postgresql.respaldar(bd)
    postgresql.eliminarViejos()
else:
    print("No se pudo conectar a la base de datos PostgreSQL")

try:
    mysql = ConexionMySQL()
    bd = mysql.listar_bd()
    mysql.respaldar(bd)
    mysql.eliminarViejos()
except Exception as e:
    print(f"Ocurrió un error al conectarse a la base de datos: {e}")

print("fin...")