# Análisis de factores de victoria en CSGO
Proyecto Final de Cloud y Big Data - Facultad de Informática UCM 2022/23

El objetivo de este proyecto es analizar un conjunto de partidas competitivas Tier 1 de CSGO
para estudiar en que medida afectan distintas estadísitcas de dichas partidas a que un equipo
gane/pierda.

Para ello, nuestros datos recogen datos de partidas competitivas desde el 2016 hasta el 2020
donde cada una tiene información sobre cada jugador y equipos.

En total, hemos analizado las siguientes estadśiticas:

- En que medida afecta el ranking global de los equipos a la victoria.
- En que medida afecta la puntuación media del equipo a la victoria.
- En que medida afecta que el nivel de cada jugador sea diferente o parecido al resto de sus compañeros a la victoria.
- En que medida afecta el que un equipo tenga el MVP de la partida en su equipo a la victoria.
- En que medida afecta el que un equipo juegue con mas SNIPERS en su equipo a la victoria.
- En que medida afecta el daño medio por ronda del equipo a la victoria.

# Participantes
* Pablo Fernández Álvarez
* Javier Meitín Moreno

# Links de interés
* [Página web](https://jmeitin.github.io/)

# Configuración del proyecto:

En el directorio Datasets se encuentra un readme que describe como conseguir el dataset a utilizar.
Una vez se encuentre el archivo "csgo_games.csv" en el directorio Datasets, ejecutar el script de shell de 
la siguiente forma: 

1.- Abrir una terminal de linux
2.- Ejecutar el sh -> ./script.sh

Eso generará un csv de 2GB y los datos estarán listos.

En cuanto a los scripts, se encuentran en el directorio Scripts y son 5 casos en total.

-------------------- EN MODO LOCAL ------------------------------

Para poder ejecutarlos hay que seguir los siguientes pasos:

1.- Instalar pip, lo que luego permitirá instalar el resto de librerías
    sudo apt update
    sudo apt install python3-pip

2.- Instalar python

3.- Instalar PySpark
    pip install pyspark

4.- Ejecutar los scripts de la siguiente forma:
    Ejemplo: python csgo_mvp.py

--------------------- EN GOOGLE CLOUD ----------------------------


