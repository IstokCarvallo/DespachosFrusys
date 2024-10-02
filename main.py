import datetime
import pyodbc
from mysql.connector import Error
import mysql.connector

def insertar(mensaje):
    return str(f'{datetime.datetime.now()} - {mensaje}\n')

def archivo(mensaje):
    file = f'LOG_{datetime.datetime.now().strftime("%d%m%YT%H%M")}.TXT'

    file = open(file, "w")
    file.write(mensaje)
    file.close()

def conecta_SQL():
    try:
        conectar = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.20.37' +
                                  ';DATABASE=Produccion_2023;UID=UserCProduce' +
                                  ';PWD=rio.2023*;TrustedConnection=1;TrustServerCertificate=yes')
        conectado = True
    except:
        conectar = ''
        conectado = False

    return conectar, conectado

def conecta_MySQL():
    try:
        coneccion = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='istok',
            password='10Bh0q10*',
            db='World'
        )
    except Error as ex:
        print(f"Error durante la coneccion:{format(ex)}")
        coneccion = ''

    return coneccion


def cargar():
    mensaje = ''
    mensaje += insertar('Inicio de Proceso de Carga...')
    mensaje += insertar('Recuperacion de Despachos a cargar...')

    conexion, conectado = conecta_SQL()

    if conectado:
        produccion = conexion.cursor()

        storeproc = "execute dbo.frusys_despachos '20240927'"
        produccion.execute(storeproc)
        despachos = produccion.fetchall()

        mySQL = conecta_MySQL()

        if mySQL.is_connected():
            frusys = mySQL.cursor()

            for despacho in despachos:
                sql = ("Insert Into Despacho(operacion,embarque,planta,consignatario,fechaDespacho,guia,"
                "contenedor,folio,especie,variedad,variedadReal,embalaje,etiqueta,productor,predio,"
                "cuartel,productorReal,predioReal,cuartelReal,packing,fechaEmbalaje,categoria,"
                "categoriaReal,calibre,calibreReal,condicion,basePallet,cajas)"
                f"Values ({despacho[0]}, '{despacho[1]}', {despacho[2]}, {despacho[3]}, {despacho[4]},"
                f"{despacho[5]}, '{despacho[6]}', '{despacho[7]}', {despacho[8]}, {despacho[9]}, {despacho[10]},"
                f"'{despacho[11]}', {despacho[12]}, {despacho[13]}, {despacho[14]}, {despacho[15]}, {despacho[16]},"
                f"{despacho[17]}, {despacho[18]}, {despacho[19]}, {despacho[20]}, {despacho[21]}, {despacho[22]},"
                f"'{despacho[23]}', '{despacho[24]}', '{despacho[26]}', {despacho[25]}, {despacho[27]})")
                print(sql)

                # frusys.execute(sql)

            print(frusys.rowcount)
            print(len(despachos))
            
        mySQL.close()

        produccion.close()
        conexion.close()

        # archivo(mensaje)



# Inicio de programa
if __name__ == '__main__':
    print(f'Inicio proceso de carga :{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    cargar()
    print(f'Termino proceso de carga :{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')