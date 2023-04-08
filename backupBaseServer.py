from dotenv import load_dotenv
import os
import psycopg2
import subprocess
import gzip
from datetime import datetime, timedelta
import logging
import shutil


# obtenemos la fecha actual
fecha = datetime.today().strftime("%Y_%m_%d")


class ConexionPostgreSQL:
    def __init__(self):
        load_dotenv()
        self.ruta = os.getenv("BACKUP_DIR")
        self.dia = int(os.getenv('DIA'))
        #registro de las backup hechas
        logging.basicConfig(filename='log_Backup.txt', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    def conectarPG(self):
        host = os.getenv("DATABASE_HOST")
        user = os.getenv("DATABASE_USER")
        password = os.getenv("DATABASE_PASSWORD")
        port = os.getenv("DATABASE_PORT")
        self.conexion = psycopg2.connect(
            f"host={host} dbname=postgres user={user} password={password} port={port}")
        print('Conexión exitosa')
   
    def listar_bd(self):
        # Crear un cursor para ejecutar consultas
        cur = self.conexion.cursor()
        # Ejecutar una consulta SQL para obtener los nombres de todas las bases de datos
        cur.execute(
            "SELECT datname FROM pg_database WHERE datistemplate = false;")
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

    def repaldar(self, db_list):
        carpeta = os.path.join(self.ruta, fecha)
        os.makedirs(carpeta, exist_ok=True)
        password = os.getenv('DATABASE_PASSWORD')
        print("Empecemos... a respaldar\n")
        for db in db_list:
            # archivo = carpeta+"/"+ f"{db}.sql.gz"
            archivo = os.path.join(carpeta, f"{db}.sql.gz")
            #comando para el respaldo de la BD postgres
            cmd = [
                'pg_dump',
                '-h', os.getenv('DATABASE_HOST'),
                '-U', os.getenv('DATABASE_USER'),
                # '-W', os.getenv('DATABASE_PASSWORD'),
                '-F', 'p',
                '-w', db,
                '|', 'gzip', '>', archivo
            ]

            with subprocess.Popen(" ".join(cmd), stdin=subprocess.PIPE, shell=True) as proc:
                proc.stdin.write(password.encode())
                proc.communicate()
            
        print(f"\nBackup de la BD completado con exito en fecha: {datetime.now()}")
        
        logging.info("Backup de la BD completado con exito.")

    def elimiarViejos(self):
        
        fecha_limite= datetime.now() - timedelta(days=self.dia)
        for nombreCarpeta in os.listdir(self.ruta):
            if nombreCarpeta.startswith('20'):
                fechaCarpeta = datetime.strptime(nombreCarpeta, '%Y_%m_%d')               
            if fechaCarpeta < fecha_limite:
                try:
                    shutil.rmtree(os.path.join(self.ruta, nombreCarpeta))
                    print('Se ha eliminado la carpeta:', nombreCarpeta)
                    logging.warning(f"'Se ha eliminado la carpeta:', {nombreCarpeta}")
                except OSError as e:
                    print(f'Error: {e} - La carpeta no se ha eliminado')
                    logging.error(msg)(f"{e} - La carpeta no se ha eliminado:', {nombreCarpeta}")


            
    def __del__(self):
        self.conexion.close()


# Crear una instancia de la clase y llamar al método para listar las bases de datos
postgresql = ConexionPostgreSQL()
postgresql.conectarPG()
bd = postgresql.listar_bd()
postgresql.repaldar(bd)
#eliminamos los arhivos Viejos
postgresql.elimiarViejos()
