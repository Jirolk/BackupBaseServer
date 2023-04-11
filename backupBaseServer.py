
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
importlib.reload(mysql.connector)


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
               
        try:
            self.conexion = psycopg2.connect(
                f"host={host} dbname={dbname} user={user} password={password} port={port}")
            print('Conexión exitosa')
            
        except (Exception, psycopg2.Error)  as error:
            print(f'Falla con la conexión: {error}')
            

    def listar_bd(self):
        # Crear un cursor para ejecutar consultas
        cur = self.conexion.cursor()
        # Ejecutar una consulta SQL para obtener los nombres de todas las bases de datos
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        # Obtener los resultados de la consulta y la guardamos para poder recorrer luego
        db_list = [row[0] for row in cur.fetchall()]
        # rows = cur.fetchall()
        # # Imprimir los nombres de las bases de datos
        print("Bases de datos disponibles:", len(db_list))
        # for row in rows:
        #     print("- ", row[0])
        # Cerrar el cursor
        cur.close()
        return db_list

    def respaldar(self, db_list):
        carpeta = os.path.join(self.ruta,"postgresSql", fecha)
        os.makedirs(carpeta, exist_ok=True)
        password = os.getenv('pDATABASE_PASSWORD')
        print("Empecemos... a respaldar\n")
        for db in db_list:
            # archivo = carpeta+"/"+ f"{db}.sql.gz"
            archivo = os.path.join(carpeta, f"{db}.sql.gz")
            #comando para el respaldo de la BD postgres
            cmd = [
                'pg_dump',
                '-h', os.getenv('pDATABASE_HOST'),
                '-U', os.getenv('pDATABASE_USER'),
                # '-W', os.getenv('DATABASE_PASSWORD'),
                '-F', 'p',
                '-w', db,
                '|', 'gzip', '>', archivo
            ]

            with subprocess.Popen(" ".join(cmd), stdin=subprocess.PIPE, shell=True) as proc:
                proc.stdin.write(password.encode())
                proc.communicate()
            
        print(f"\nBackup de la BD completado con exito en fecha: {datetime.now()}")
        
        logging.info("Postgres:Backup de la BD completado con exito.")

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
                print("Ningún archivo fue borrado")


            
    def __del__(self):

        if self.conexion:
            self.conexion.close()
        
class ConexionMySQL():
    def __init__(self):
        load_dotenv()
        self.ruta = os.getenv("BACKUP_DIR")
        self.dia = int(os.getenv('DIA'))
        # self.con= None
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
        except Exception as e:
            print(f"Falla la conexión: {e}")
    
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
                '--password=', os.getenv('DATABASE_PASSWORD'),
                db,
                '|', 'gzip', '>', archivo
                ]

            with subprocess.Popen(" ".join(cmd), stdin=subprocess.PIPE, shell=True) as proc:
                        proc.stdin.write(password.encode())
                        proc.communicate()
        
        logging.info(f"MySQL:Backup de la BD completado con exito. Cant:{len(archivos_FE)}")
            
        print(f"\nBackup de la BD MySQL completado con exito en fecha: {datetime.now()}")
                

    def eliminarViejos(self):
        
        fecha_limite= datetime.now() - timedelta(days=self.dia)
        for nombreCarpeta in os.listdir(os.path.join(self.ruta, 'Mysql')):
            
            if nombreCarpeta.startswith('20'):
                fechaCarpeta = datetime.strptime(nombreCarpeta, '%Y_%m_%d')               
            if fechaCarpeta < fecha_limite:
                try:
                    shutil.rmtree(os.path.join(self.ruta,"Mysql", nombreCarpeta))
                    print('Se ha eliminado la carpeta:', nombreCarpeta)
                    logging.warning(f"'Se ha eliminado la carpeta:', {nombreCarpeta}")
                except OSError as e:
                    print(f'Error: {e} - La carpeta no se ha eliminado')
                    logging.error(f"{e} - La carpeta no se ha eliminado:', {nombreCarpeta}")
            else:
                print("Ningún archivo fue borrado")

            
    def __del__(self):

        if self.con:
            self.con.close()
        



# Crear una instancia de la clase y llamar al método para listar las bases de datos
postgresql = ConexionPostgreSQL()
postgresql.conectarPG()
bd = postgresql.listar_bd()
postgresql.respaldar(bd)
#eliminamos los arhivos Viejos
postgresql.eliminarViejos()

mysql= ConexionMySQL()
bd= mysql.listar_bd()
mysql.respaldar(bd)
mysql.eliminarViejos()

