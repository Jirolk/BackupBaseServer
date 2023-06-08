import psycopg2
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
host = os.getenv("pDATABASE_HOST")
dbname=os.getenv("pDATABASE_NAME")
user = os.getenv("pDATABASE_USER")
password = os.getenv("pDATABASE_PASSWORD")
port = os.getenv("pDATABASE_PORT")

# Establecer la variable de entorno PGPASSWORD con la contraseña
os.environ['PGPASSWORD'] = os.getenv('DATABASE_PASSWORD')

#Log de las backup hechas
logging.basicConfig(filename='log_Backup.txt', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Conectar a la base de datos
try:
    conn = psycopg2.connect(
        f"host={host} dbname={dbname} user={user} password={password} port={port}")
    print('ConexiÃ³n exitosa')
except (Exception, psycopg2.Error)  as error:
    logging.error(f"Fallo con la conexiÃ³n: {error}")
    print(f'Falla con la conexiÃ³n: {error}')
            
# Crear un objeto cursor para ejecutar consultas SQL
cursor = conn.cursor()

# Ejecutar una consulta SQL
cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")

# Obtener los resultados de la consulta
db_list = [row[0] for row in cursor.fetchall()]
db_list = list(filter(lambda x: x.endswith("metropolis"), db_list))
print("Bases de datos disponibles:", len(db_list))

# Filtrar archivos que terminan con "_FE"
# archivos_FE = list(filter(lambda x: x.endswith("_fe"), db_list))
# print(f"Cantidad de Bd: {len(archivos_FE)}")
print("Empecemos... a respaldar\n")
for db in db_list:
    carpeta=os.path.join(ruta,"postgresSql", fecha)
    os.makedirs(carpeta, exist_ok = True)
    # archivo = carpeta+"/"+ f"{db}.sql.gz"
    archivo=os.path.join(carpeta, f"{db}.sql.gz")
    # comando para el respaldo de la BD postgres
    cmd = [
        'pg_dump --no-privileges --no-owner --no-reconnect',
        '-h', os.getenv('pDATABASE_HOST'),
        '-U', os.getenv('pDATABASE_USER'),
        #'-W', os.getenv('DATABASE_PASSWORD'),
        '-F', 'p',
        '-w', db,
        '|', 'gzip', '>', archivo
            ]

    # subprocess.call(cmd,shell=True)
    # subprocess.call(os.getenv('DATABASE_PASSWORD'), shell=True)
    # with subprocess.Popen(" ".join(cmd), stdin=subprocess.PIPE, shell=True) as proc:
    #     password = os.getenv('DATABASE_PASSWORD') + '\n'
    #     proc.stdin.write(password.encode())
    #     proc.communicate()
    
    with subprocess.Popen(" ".join(cmd), shell=True) as proc:
        proc.communicate()


    
    

# Eliminar la variable de entorno PGPASSWORD después de su uso
del os.environ['PGPASSWORD']


print(f"\nBackup de la BD completado con exito en fecha: {datetime.now()}")
logging.info(f"Postgres:Backup de la BD completado con exito. Cant:{len(db_list)}")
# Cerrar el cursor y la conexiÃ³n
cursor.close()
conn.close()

#Fragmento para borrar los respaldos dependiendo de los dÃ­as
fecha_limite= datetime.now() - timedelta(days=dia)
for nombreCarpeta in os.listdir(os.path.join(ruta, 'postgresSql')):
    
    if nombreCarpeta.startswith('20'):
        fechaCarpeta = datetime.strptime(nombreCarpeta, '%Y_%m_%d')               
    if fechaCarpeta < fecha_limite:
        try:
            shutil.rmtree(os.path.join(ruta,"postgresSql", nombreCarpeta))
            print('Se ha eliminado la carpeta:', nombreCarpeta)
            logging.warning(f"'Se ha eliminado la carpeta:', {nombreCarpeta}")
        except OSError as e:
            print(f'Error: {e} - La carpeta no se ha eliminado')
            logging.error(f"{e} - La carpeta no se ha eliminado:', {nombreCarpeta}")

