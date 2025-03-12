import datetime
import pyodbc
from mysql.connector import Error
import mysql.connector

def insertar(mensaje):
    return str(f'{datetime.datetime.now()} - {mensaje}\n')

def archivo(mensaje):
    file = f'LOG_{datetime.datetime.now().strftime("%d%m%YT%H%M")}.LOG'

    file = open(file, "w")
    file.write(mensaje)
    file.close()

def conecta_SQL():
    try:
        conectar = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.20.37' +
                              ';DATABASE=Produccion_2024;Trusted_Connection=yes;TrustServerCertificate=yes')
    
        # conectar = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.20.37' +
        #                           ';DATABASE=Produccion_2024;UID=UserCProduce' +
        #                           ';PWD=rio.2023*;TrustedConnection=1;TrustServerCertificate=yes')
        conectado = True
    except:
        conectar = ''
        conectado = False

    return conectar, conectado

def conecta_MySQL():
    try:
        coneccion = mysql.connector.connect(
            host='192.168.20.5',
            port=3306,
            user='Uprag',
            password='rio.2024',
            db='frusys25'
        )
    except Error as ex:
        print(f"Error durante la coneccion:{format(ex)}")
        coneccion = ''

    return coneccion


def cargar():
    mensaje = ''
    fecha = datetime.datetime.now().strftime("%Y%m%d")
    # fecha = '20250223'

    mensaje += insertar('Inicio de Proceso de Carga...')
    mensaje += insertar('Recuperacion de Despachos a cargar...')

    conexion, conectado = conecta_SQL()

    if conectado:
        produccion = conexion.cursor()

        storeproc = f"execute dbo.frusys_despachos '{fecha}'"
        produccion.execute(storeproc)
        despachos = produccion.fetchall()

        mySQL = conecta_MySQL()

        if mySQL.is_connected():
            frusys = mySQL.cursor()

            elimina = (f"Delete From despacho "
                       f"Where Datediff(fechaDespacho, '{fecha}') Between -2 and 0 ")
                       # f"Or Datediff(fechaDespacho, '{fecha}') = -1")
            
            mensaje += insertar(f'Se eliminaran {frusys.rowcount} registros.')

            frusys.execute(elimina)
            mySQL.commit()
            mensaje += insertar('Registros Elimiandos.')
                                
            mensaje += insertar(f'Se cargaran {len(despachos)} registros.') 

            for despacho in despachos:
                sql = ("Insert Into Despacho(operacion,embarque,planta,consignatario,fechaDespacho,guia,"
                "contenedor,folio,especie,variedad,variedadReal,embalaje,etiqueta,productor,predio,"
                "cuartel,productorReal,predioReal,cuartelReal,packing,fechaEmbalaje,categoria,"
                "categoriaReal,calibre,calibreReal,condicion,basePallet,cajas,codigo_operacional,Tipo_Planilla_C, Tipo_Planilla)"
                f"Values ('{despacho[0]}', '{despacho[1]}', {despacho[2]}, {despacho[3]}, '{despacho[4]}', "
                f"{despacho[5]}, '{despacho[6]}', '{despacho[7]}', {despacho[8]}, {despacho[9]}, {despacho[10]}, "
                f"'{despacho[11]}', {despacho[12]}, {despacho[13]}, {despacho[14]}, {despacho[15]}, {despacho[16]}, "
                f"{despacho[17]}, {despacho[18]}, {despacho[19]}, '{despacho[20]}', {despacho[21]}, {despacho[22]}, "
                f"'{despacho[23]}', '{despacho[24]}', '{despacho[26]}', {despacho[25]}, {despacho[27]}, '{despacho[28]}', "
                f"'{despacho[29]}', '{despacho[30]}')")
                
                frusys.execute(sql)
                mySQL.commit()

        mySQL.close()

        produccion.close()
        conexion.close()

        mensaje += insertar('Termino de carga Despachos...')
        archivo(mensaje)



# Inicio de programa
if __name__ == '__main__':
    print(f'Inicio proceso de carga :{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    cargar()
    print(f'Termino proceso de carga :{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')