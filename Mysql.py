import mysql.connector
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import subprocess
import logging
import shutil
from byte_to_megabytes import *
load_dotenv()


# obtenemos la fecha actual
fecha = datetime.today().strftime("%Y_%m_%d")

#variables
ruta = os.getenv("BACKUP_DIR")
dia = int(os.getenv('DIA'))
host = os.getenv("DATABASE_HOST")
user = os.getenv("DATABASE_USER")
password = os.getenv("DATABASE_PASSWORD")

#Log de las backup hechas
logging.basicConfig(filename='log_Backup.txt', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Conectar a la base de datos
try:
    conn = mysql.connector.connect(
         host=host,
        user=user,
        password=password,
    )
    print("Conexión exitosa")
except Exception as e:
    logging.error(f"Fallo con la conexión: {e}")
    print(f"Falla la conexión: {e}")

# Crear un objeto cursor para ejecutar consultas SQL
cursor = conn.cursor()

# Ejecutar una consulta SQL
cursor.execute("show databases")

# Obtener los resultados de la consulta
db_list = [row[0] for row in cursor.fetchall()]
# Filtrar archivos que terminan con "_FE"
archivos_FE = list(filter(lambda x: x.endswith("_fe"), db_list))

print(f"Bases de datos disponibles: {len(archivos_FE)}")
peso_bytes=0 #Inicializamos para medir si realizó la backup
peso_megabytes =0
print("Empecemos... a respaldar BD mysql\n")

for db in archivos_FE:
    carpeta=os.path.join(ruta,"mysql", fecha)
    os.makedirs(carpeta, exist_ok = True)
    # archivo = carpeta+"/"+ f"{db}.sql.gz"
    archivo=os.path.join(carpeta, f"{db}.sql.gz")
    # comando para el respaldo de la BD postgres
    cmd=[
        'mysqldump',
        '-h', os.getenv('DATABASE_HOST'),
        '-u', os.getenv('DATABASE_USER'),
        '--password='+ os.getenv('DATABASE_PASSWORD'),
        db,
        '|', 'gzip', '>', archivo
        ]

    # with subprocess.Popen(" ".join(cmd), stdin=subprocess.PIPE, shell=True) as proc:
    #             proc.stdin.write(password.encode())
    #             proc.communicate()
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
    print("No se pudo generar el archivo de respaldo.")
# Cerrar el cursor y la conexión
cursor.close()
conn.close()

# para eliminar los backup viejos
fecha_limite= datetime.now() - timedelta(days=dia)
for nombreCarpeta in os.listdir(os.path.join(ruta, 'mysql')):
    #print("Nombre: ",nombreCarpeta)
    if nombreCarpeta.startswith('20'):
        fechaCarpeta = datetime.strptime(nombreCarpeta, '%Y_%m_%d')               
    if fechaCarpeta < fecha_limite:
        try:
            shutil.rmtree(os.path.join(ruta,"mysql", nombreCarpeta))
            print('Se ha eliminado la carpeta:', nombreCarpeta)
            logging.warning(f"'Se ha eliminado la carpeta:', {nombreCarpeta}")
        except OSError as e:
            print(f'Error: {e} - La carpeta no se ha eliminado')
            logging.error(f"{e} - La carpeta no se ha eliminado:', {nombreCarpeta}")
print("Fin...")
