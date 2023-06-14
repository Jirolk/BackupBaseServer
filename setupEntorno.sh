#!/bin/bash
# exit when any command fails
set -e

directorio=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo Instalación del entorno para realizar la BACkUP
echo $directorio
cd $directorio

#echo Git pull
#git status
#git pull

if [ -d "env" ] 
then
    echo "Entorno virtual existente" 
else
	echo "Creamos un nuevo entorno virtual" 
    python3.6 -m virtualenv env
    #virtualenv env
fi
echo Install Requirements
source env/bin/activate
pip install -r requirements.txt

#agregamos la tarea al crontab
<<<<<<< HEAD
sudo (crontab -l 2>/dev/null; echo "0 1 * * * sh $directorio/backup.sh") | crontab -
=======
(crontab -l 2>/dev/null; echo "0 1 * * * sh $directorio/backup.sh") | crontab -
>>>>>>> e68fbc53f2f7f3460289e9c40538c49f33b7772b
o/backup.sh") | crontab -
eriodica la tarea a las 01:00 AM
echo Fin