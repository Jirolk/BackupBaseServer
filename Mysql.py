import mysql.connector
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import subprocess
import logging
import shutil
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

print(f"Cantidad de Bd: {len(archivos_FE)}")

for db in archivos_FE:
    carpeta=os.path.join(ruta,"Mysql", fecha)
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


logging.info(f"Mysql:Backup de la BD completado con exito. Cant:{len(archivos_FE)}")
# Cerrar el cursor y la conexión
cursor.close()
conn.close()


fecha_limite= datetime.now() - timedelta(days=dia)
for nombreCarpeta in os.listdir(os.path.join(ruta, 'Mysql')):
    print("Nombre: ",nombreCarpeta)
    if nombreCarpeta.startswith('20'):
        fechaCarpeta = datetime.strptime(nombreCarpeta, '%Y_%m_%d')               
    if fechaCarpeta < fecha_limite:
        try:
            shutil.rmtree(os.path.join(ruta,"Mysql", nombreCarpeta))
            print('Se ha eliminado la carpeta:', nombreCarpeta)
            logging.warning(f"'Se ha eliminado la carpeta:', {nombreCarpeta}")
        except OSError as e:
            print(f'Error: {e} - La carpeta no se ha eliminado')
            logging.error(f"{e} - La carpeta no se ha eliminado:', {nombreCarpeta}")

