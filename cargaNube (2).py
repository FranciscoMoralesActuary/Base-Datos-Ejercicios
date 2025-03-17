import pandas as pd
import json
from sqlalchemy import create_engine
from sqlalchemy.engine.interfaces import Connectable
from sqlalchemy.types import VARCHAR, FLOAT, INTEGER, DATE, CHAR, DATETIME
import os
import warnings

warnings.filterwarnings('ignore')


def leerCredenciales(creds: str) -> dict:
    print("Leyendo credenciales...")
    creds = json.load(open(creds, 'rb'))
    return creds


def crearConexion(creds: dict) -> Connectable:
    print("Conectándose a la base de datos...")
    url = f"mysql+pymysql://{creds['user']}:{creds['password']}@{creds['servidor']}/{creds['bd']}"
    cnx = create_engine(url, encoding='utf8')
    cnx = cnx.connect()
    return cnx


def leerEntidadEstacion(datos: pd.DataFrame, cnx: Connectable) -> pd.DataFrame:
    print("Creando entidad estación...")
    entEstacion = pd.concat(
        [datos[[f'Ciclo_Estacion_{tipo}']].rename(columns={f'Ciclo_Estacion_{tipo}': 'idEstacion'}) for tipo in
         ['Arribo', 'Retiro']], ignore_index=True).drop_duplicates().reset_index(drop=True)
    cat = pd.read_csv('/home/jose/Documentos/bd/ecobici/estaciones-de-ecobici.csv',
                      usecols=['id', 'name', 'location_lat', 'location_lon', 'stationtype'])
    cat.columns = ['idEstacion', 'nombre', 'latitud', 'longitud', 'tipoEstacion']
    entEstacion = entEstacion.merge(cat, on='idEstacion', how='inner')
    entEstacion.to_sql(con=cnx,
                       name='estacion',
                       if_exists='append',
                       index=False,
                       chunksize=10000,
                       dtype={'idEstacion': INTEGER,
                              'nombre': VARCHAR(49),
                              'latitud': FLOAT,
                              'longitud': FLOAT,
                              'tipoEstacion': VARCHAR(13)})
    return entEstacion


def leerEntidadBici(datos: pd.DataFrame, cnx: Connectable) -> pd.DataFrame:
    print("Creando entidad bici...")
    entBici = datos[['Bici']].copy()
    entBici['Bici'] = pd.to_numeric(entBici['Bici'], errors='coerce')
    entBici = entBici.dropna().reset_index(drop=True)
    entBici['Bici'] =entBici['Bici'].astype(int)
    entBici = entBici.drop_duplicates().rename(columns={'Bici': 'idBici'})
    entBici.to_sql(con=cnx,
                   name='bici',
                   if_exists='append',
                   index=False,
                   chunksize=10000,
                   dtype={'idBici': INTEGER})
    return entBici


def leerEntidadViaje(datos: pd.DataFrame, entEstacion: pd.DataFrame, entBici: pd.DataFrame, cnx: Connectable):
    print("Creando entidad viaje...")
    entViaje = datos.copy().dropna().reset_index(drop=True)
    for tipo in ['Arribo', 'Retiro']:
        entViaje[f'Hora_{tipo}'] = entViaje[f'Hora_{tipo}'].map(lambda x: f'0{x}' if len(x) == 7 else x)
        entViaje[f'Fecha_{tipo}'] = entViaje[f'Fecha_{tipo}'] + ' ' + entViaje[f'Hora_{tipo}']
        entViaje.drop(f'Hora_{tipo}', axis=1, inplace=True)
        entViaje[f'Fecha_{tipo}'] = pd.to_datetime(entViaje[f'Fecha_{tipo}'],
                                                   format='%d/%m/%Y %H:%M:%S',
                                                   errors='coerce')
    entViaje.columns = ['generoUsuario', 'edadUsuario', 'idBici', 'idEstacionRetiro', 'fechaRetiro',
                        'idEstacionArribo', 'fechaArribo']
    entViaje.insert(0, 'idViaje', entViaje.index + 1)
    entViaje[['idBici', 'idEstacionRetiro', 'idEstacionArribo']] = entViaje[
        ['idBici', 'idEstacionRetiro', 'idEstacionArribo']].astype(int)
    entViaje = entViaje.merge(entEstacion[['idEstacion']].rename(columns={'idEstacion': 'idEstacionRetiro'}),
                              on='idEstacionRetiro', how='inner')
    entViaje = entViaje.merge(entEstacion[['idEstacion']].rename(columns={'idEstacion': 'idEstacionArribo'}),
                              on='idEstacionArribo', how='inner')
    entViaje = entViaje.merge(entBici[['idBici']], on='idBici', how='inner')

    entViaje.to_sql(con=cnx,
                    name='viaje',
                    if_exists='append',
                    index=False,
                    chunksize=10000,
                    dtype=dict(zip(entViaje.columns,
                                   [INTEGER, CHAR(1), INTEGER, INTEGER, INTEGER, DATETIME, INTEGER, DATETIME])))
    print("Proceso de carga concluído con éxito ^ _ ^")


if __name__ == '__main__':
    ruta = '/home/jose/Documentos/bd/ecobici/'
    listaArchivos = sorted([os.path.join(ruta, arch) for arch in os.listdir(ruta) if '20' in arch])
    cnx = crearConexion(leerCredenciales('credenciales_carga_nube.json'))
    print("leyendo datos...")
    datos = pd.concat(map(lambda archivo: pd.read_csv(archivo), listaArchivos), ignore_index=True)
    print(f"total de registros leídos: {datos.shape[0]}")
    leerEntidadViaje(datos, leerEntidadEstacion(datos, cnx), leerEntidadBici(datos, cnx), cnx)
