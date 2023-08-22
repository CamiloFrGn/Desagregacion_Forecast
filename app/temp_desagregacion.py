import modulo_conn_sql as mcq
import numpy as np
import pandas as pd 
import datetime 
import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
import random
import sqlalchemy as sa
import urllib
import sys

#---------------------SQLALCHEMY CONNECTION---------------------------------
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=USCLDBITVMP01;DATABASE=BI_Tableau;UID=usertableau;PWD=usertableau$")
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
#---------------------------------------------------------------------------

def send_df_to_sql(data,database_name):
    try:          
        data.to_sql(database_name, engine, index=False, if_exists="append", schema="dbo")  
        
        return "success"       
    except Exception as e:
        print(str(e))
        sys.exit()
        

def conectarSQL():
    conn = mcq.ConexionSQL()
    cursor = conn.getCursor()
    return cursor

#Query BD SQL-Server Cemex
def querySQL(query, parametros):
    #Conectar con base sql y ejecutar consulta
    cursor = conectarSQL()
    try:
        cursor.execute(query, parametros)
        #obtener nombre de columnas
        names = [ x[0] for x in cursor.description]
        
        #Reunir todos los resultado en rows
        rows = cursor.fetchall()
        resultadoSQL = []
            
        #Hacer un array con los resultados
        while rows:
            resultadoSQL.append(rows)
            if cursor.nextset():
                rows = cursor.fetchall()
            else:
                rows = None
                
        #Redimensionar el array para que quede en dos dimensiones
        resultadoSQL = np.array(resultadoSQL)
        resultadoSQL = np.reshape(resultadoSQL, (resultadoSQL.shape[1], resultadoSQL.shape[2]) )
    finally:
            if cursor is not None:
                cursor.close()
    return pd.DataFrame(resultadoSQL, columns = names)
 
# dataset(pandas dataframe): base de datos con el historico
# array_group_top(array) : array de la jerarquia mas ALTA con el nombre de las columnas del dataset por el cual se quiere agrupar las proporciones
# array_group_bottom(array) : array de la jerarquia mas BAJA con el nombre de las columnas del dataset por el cual se quiere agrupar las proporciones
# medida_target( string ) : nombre de la columna que contiene los datos objetivo de la proporcion
# group_target(array) : array de nombre de columnas con las cuales queremos la proporcion final
# name_proportion(string) : etiqueta de la dimension a la cual le estamos calculando la proporcion
    
def historical_proportion( dataset, array_group_top, array_group_bottom, medida_target, group_target, name_proportion  ):

    promedio_group_top = dataset.groupby(array_group_top)[medida_target].mean().reset_index()
    print("promedio_group_top")
    print(promedio_group_top)
    promedio_group_bottom = dataset.groupby(array_group_bottom)[medida_target].mean().reset_index()    
    print("promedio_group_bottom")
    print(promedio_group_bottom)
    proportion  = pd.merge(promedio_group_bottom, promedio_group_top, how = 'inner', left_on = array_group_top, right_on = array_group_top )
    print("proportion")
    print(proportion)
    proportion['proportion'] = proportion[medida_target + '_x'] / proportion[medida_target + '_y']
    print("proportion")
    print(proportion)
    proportion = proportion.groupby(group_target)['proportion'].median().reset_index()
    print("proportion")
    print(proportion)
    proportion.rename(columns={'proportion':'proportion_' + name_proportion}, inplace = True)
    print("HISTORICAL PROPORTION")
    print(proportion)
    print("------------------------------------------------")
    return proportion 

def random_number (num):
    
    num = num if num <= 1 else 1
    
    return 1 + random.uniform( 0, num) if  random.random() < 0.5 else 1 - random.uniform( 0, num) 

#desviacion estandar, media y coeficiente de variacion
def stats_serie(dataset, array_group, colum_target):
    ret = dataset.groupby(array_group)[colum_target].agg( ['std', 'mean']).reset_index()
    ret['cov'] = ret['std']/ret['mean']
    
    return ret

#PRINCIPAL

############################################# PARAMETROS #############################################

#años para entrenar las versiones
yearDesagregacion = 2023
mesDesagregacion = 6
pais = 'Colombia'
volPais = 0
inicioHistoria = datetime.datetime(2021, 1, 1) #'2013-05-01'
finHistoria = datetime.datetime(2023, 3 , 13)

#Targets


nivel_volatilidad = 0.0
criterio_historia_reciente = 90 #dias

#version = "ESTADISTICO_MAR_2023"
#version = "CONSENSO_MAR_2023"
version = "PRECIERRE_MAR_2023"

"""
PARAMETROS:
absorcionEstadistica = 1  -> Toma proporciones historicas
absorcionEstadistica = 0  -> Toma proporciones por archivo de volumen CIUDAD
absorcionEstadistica = -1 -> Toma proporciones por archivo de volumen LINEA PRODUCTIVA

"""
absorcionEstadistica = -1 #criterio para tomar las proporciones historicas de las plantas o False -> archivo que define el volumen por planta

######################################### FIN PARAMETROS #############################################

#Consulta de datos en la base SQL
despachosSQL = querySQL(  "{CALL SCAC_AP8_BaseForecast (?,?,?)}", (pais, inicioHistoria.strftime("%Y-%m-%d"), finHistoria.strftime("%Y-%m-%d") ) )
#despachosSQL = despachosSQL[despachosSQL['Planta']!= 'G014']
calendarioLogistico = querySQL( "{CALL SCAC_AP9_CalendarioLogistico (?,?,?)}" , (pais, yearDesagregacion, mesDesagregacion))
#agrego informacion geografica        
nombre_cluster = querySQL( "SELECT Centro, Ciudad_Cluster as Ciudad, [Desc Cluster] as Cluster, [Planta Unica] as PlantaUnica FROM SCAC_AT1_NombreCluster where Pais = ? and Activo = 1" , (pais) )

#Otras Consultas
irregularidades  = pd.read_excel(r"C:\Users\snortiz\OneDrive - CEMEX\Documentos\Proyectos\Proyectos-Cemex\Proyectos-Cemex\Desagregacion_Forecast\app\datos\BaseIrregularidades.xlsx")
irregularidades['FechaEntrega'] = pd.to_datetime(irregularidades['FechaEntrega'])

#arreglo de formatos
despachosSQL['totalEntregado'] = despachosSQL['totalEntregado'].astype(float)
calendarioLogistico['Dia_Semana'] = calendarioLogistico['Dia_Semana'].astype(int)

#SOLO CENTROS ACTIVOS
despachosSQL = pd.merge(despachosSQL,nombre_cluster[['Centro']], left_on='Planta', right_on='Centro' ).drop(columns=['Centro'])

#irregularidades a nivel pais
irr_nacional = irregularidades[irregularidades['Ciudad']==pais]
despachos_irregularidades = pd.merge(despachosSQL, nombre_cluster, left_on='Planta', right_on='Centro')
despachos_irregularidades = pd.merge(despachos_irregularidades, irr_nacional[['FechaEntrega','EtiquetaIrregularidad']], on='FechaEntrega', how='left')

def generardor_desagregacion(despachosSQL, calendarioLogistico, absorcionEstadistica, criterio_historia_reciente, inicioHistoria, finHistoria, nivel_volatilidad, volPais, pais , despachos_irregularidades, irr_nacional):

    #Proporciones con toda la historia

    """NOTAS: se obetiene las proporciones a nivel general de los ultimos 30 dias de la fecha final de historia establecido en los parametros."""
    proportion_plant = historical_proportion(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente))], ['Año', 'Mes'], ['Año', 'Mes', 'Planta'], 'totalEntregado', ['Planta'], 'planta')
    print("#######################################")
    """NOTAS: se obetiene las proporciones a nivel de semana de toda la historia establecida en los parametros"""
    proportion_week = historical_proportion(despachosSQL, ['Año', 'Mes', 'Planta'], ['Año', 'Mes', 'Planta', 'Semana_Relativa'], 'totalEntregado', ['Planta', 'Semana_Relativa'], 'semana')
    print("##################siguente#####################")
    """NOTAS: se obetiene las proporciones a nivel de semana de los ultimos 60 dias de la fecha final de historia establecido en los parametros."""
    proportion_week_recent = historical_proportion(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Año', 'Mes', 'Planta'], ['Año', 'Mes', 'Planta', 'Semana_Relativa'], 'totalEntregado', ['Planta', 'Semana_Relativa'], 'semana')
    print("#######################################")
    """NOTAS: se obetiene las proporciones a nivel de dia de semana de toda la historia establecida en los parametros"""
    proportion_wday = historical_proportion(despachosSQL, ['Año', 'Mes', 'Planta'], ['Año', 'Mes', 'Planta', 'DiaSemana'], 'totalEntregado', ['Planta', 'DiaSemana'], 'dia_semana')
    print("#######################################")
    """NOTAS: se obetiene las proporciones a nivel de dia de semana de los ultimos 60 dias de la fecha final de historia establecido en los parametros."""
    proportion_wday_recent = historical_proportion(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Año', 'Mes', 'Planta'], ['Año', 'Mes', 'Planta', 'DiaSemana'], 'totalEntregado', ['Planta', 'DiaSemana'], 'dia_semana')
    print("#######################################")
    proportion_irr = historical_proportion(despachos_irregularidades, ['Planta'], [ 'Planta', 'EtiquetaIrregularidad'], 'totalEntregado', ['Planta', 'EtiquetaIrregularidad'], 'irregularidad')
    print("#######################################")
    cov_plantas = stats_serie(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Planta'], 'totalEntregado')

    cov_planta_diasemana = stats_serie(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Planta', 'DiaSemana'], 'totalEntregado')

        #--------------- ESTIMACION DESAGREGACION FORECAST POR PLANTA ---------------# 

    if absorcionEstadistica == 1:
        #obtengo el listado de plantas que han estado activas los ultimos N dias
        DesagregacionPronosticoPlanta = despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ]
        DesagregacionPronosticoPlanta = pd.DataFrame({'Planta' : DesagregacionPronosticoPlanta["Planta"].unique()})
        #se divide el volumen por igual en cada planta
        DesagregacionPronosticoPlanta['M3ForecastPlanta'] = volPais / DesagregacionPronosticoPlanta['Planta'].count()

        #join con proporcion planta
        DesagregacionPronosticoPlanta = pd.merge(DesagregacionPronosticoPlanta, proportion_plant, how='left', left_on=['Planta'], right_on=['Planta'] ).fillna(1)
        #join con proporcion planta
        DesagregacionPronosticoPlanta = pd.merge(DesagregacionPronosticoPlanta, cov_plantas, how='left', left_on=['Planta'], right_on=['Planta'] ).fillna(1)
        #DesagregacionPronosticoPlanta['Aleatorio_planta'] = DesagregacionPronosticoPlanta['cov'].apply( random_number) * nivel_volatilidad

        #se itera con las proporciones y se ajusta el resultado hasta que el gap sea menor a 1
        gap_iteracion = 1000.0
        while( abs(gap_iteracion) > 1 ):
            #print('iteracion Planta: ' + str(gap_iteracion) )
            DesagregacionPronosticoPlanta['forecast_planta'] = DesagregacionPronosticoPlanta['M3ForecastPlanta'] *  DesagregacionPronosticoPlanta['proportion_planta'] # *  DesagregacionPronosticoPlanta['Aleatorio_planta'] 
            resultado_iteracion = DesagregacionPronosticoPlanta['forecast_planta'].sum()
            gap_iteracion = resultado_iteracion - volPais
            DesagregacionPronosticoPlanta['M3ForecastPlanta'] = DesagregacionPronosticoPlanta['M3ForecastPlanta'] - (gap_iteracion / DesagregacionPronosticoPlanta['Planta'].count() )

        DesagregacionPronosticoPlanta = DesagregacionPronosticoPlanta[['Planta','forecast_planta']]

    elif absorcionEstadistica == 0:
        
        #PROCEDIMIENTO PARA DESAGREGAR VOLUMEN CON BASE EN EL VOLUMEN CIUDAD
        # historia reciente
        abs_centro = despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente))]
        abs_centro = pd.merge(abs_centro, nombre_cluster[['Centro', 'Ciudad']], left_on='Planta', right_on = 'Centro').drop(['Centro'], axis=1)

        #volumen por ciudad
        abs_centro1 = abs_centro.groupby(['Ciudad']).sum('totalEntregado').reset_index()

        #volumen por planta
        abs_centro2 = abs_centro.groupby(['Ciudad','Planta']).sum('totalEntregado').reset_index()

        #calculo de absorcion
        abs_centro3 = pd.merge(abs_centro2, abs_centro1, on ='Ciudad')
        abs_centro3['absorcion_planta'] = abs_centro3['totalEntregado_x'] / abs_centro3['totalEntregado_y']
        abs_centro3 = abs_centro3[['Ciudad', 'Planta', 'absorcion_planta']]

        archivo_volumen_ciudad = pd.read_excel('../DatosAbsorcionPlantas/' + pais + 'Ciudad.xlsx')

        DesagregacionPronosticoPlanta =  pd.merge(abs_centro3,archivo_volumen_ciudad, left_on='Ciudad', right_on='ciudad_asignaciones' )

        DesagregacionPronosticoPlanta['forecast_planta'] = DesagregacionPronosticoPlanta['volumen'] * DesagregacionPronosticoPlanta['absorcion_planta']

        DesagregacionPronosticoPlanta = DesagregacionPronosticoPlanta[['Planta', 'forecast_planta']]
    
    elif absorcionEstadistica == -1:
        
        DesagregacionPronosticoPlanta = pd.read_excel('../DatosAbsorcionPlantas/' + pais + '.xlsx')

        #--------------- ESTIMACION DESAGREGACION FORECAST POR DIA ---------------#

    print("########DesagregacionPronosticoPlanta#################")
    print(DesagregacionPronosticoPlanta)
    #cross join tabla DesagregacionPronostico y calendario
    calendarioLogistico['key'] = 1
    DesagregacionPronosticoPlanta['key'] = 1
    DesagregacionPronosticoPlantaDia = pd.merge(calendarioLogistico, DesagregacionPronosticoPlanta, on = 'key').drop("key",1)

    #join con proporcion semana y semana reciente
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, proportion_week, how='left', left_on=['Planta','Semana_relativa'], right_on=['Planta','Semana_Relativa'] ).fillna(1)
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, proportion_week_recent, how='left', left_on=['Planta','Semana_relativa'], right_on=['Planta','Semana_Relativa'] ).fillna(1)

    #join con proporcion dia semana y dia semana reciente
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, proportion_wday, how='left', left_on=['Planta','Dia_Semana'], right_on=['Planta','DiaSemana'] ).fillna(1)
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, proportion_wday_recent, how='left', left_on=['Planta','Dia_Semana'], right_on=['Planta','DiaSemana'] ).fillna(1)
    DesagregacionPronosticoPlantaDia.head()
    
    #join con base de irregularidades para agregar etiqueta
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, irr_nacional[['FechaEntrega', 'EtiquetaIrregularidad']], how='left', left_on='Fecha de entrega', right_on='FechaEntrega' )
    
    #join con proporcion irregularidades
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, proportion_irr, how='left', left_on=['Planta','EtiquetaIrregularidad'], right_on=['Planta','EtiquetaIrregularidad'] ).fillna(1)
    
    #join con el coeficiente de variabilidad
    DesagregacionPronosticoPlantaDia  = pd.merge(DesagregacionPronosticoPlantaDia , cov_planta_diasemana, how='left', left_on=['Planta', 'Dia_Semana'], right_on=['Planta','DiaSemana'] )

    #quito columnas que no me interesan
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("DiaSemana_x",1)
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("DiaSemana_y",1)
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("Semana_Relativa_x",1)
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("Semana_Relativa_y",1) 

    #se agrega aleatoriedad a la desagregacion
    DesagregacionPronosticoPlantaDia['Aleatorio'] = DesagregacionPronosticoPlantaDia['cov'].apply( random_number) * nivel_volatilidad

    #SETUP MATRIZ DE ITERACIONES
    #DesagregacionPronosticoPlantaDia['M3Forecast'] = (DesagregacionPronosticoPlantaDia['Días_Operativos'] * ( DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']) * DesagregacionPronosticoPlantaDia['proportion_semana'] * DesagregacionPronosticoPlantaDia['proportion_dia_semana']  ).astype(float)
    DesagregacionPronosticoPlantaDia['M3Forecast'] =  (DesagregacionPronosticoPlantaDia['Días_Operativos'] * \
                                                       ( DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']) * \
                                                       DesagregacionPronosticoPlantaDia['proportion_semana_x'] * \
                                                       DesagregacionPronosticoPlantaDia['proportion_dia_semana_y'] * \
                                                       DesagregacionPronosticoPlantaDia['proportion_irregularidad'] * \
                                                       DesagregacionPronosticoPlantaDia['Aleatorio'] ).astype(float)

    matrizPPTO_Resultado = pd.DataFrame(DesagregacionPronosticoPlantaDia.groupby('Planta')['M3Forecast'].sum()).reset_index()
    matrizPPTO_Resultado = pd.merge(matrizPPTO_Resultado, DesagregacionPronosticoPlanta, on = 'Planta' ).drop("key",1)
    matrizPPTO_Resultado['gap'] = matrizPPTO_Resultado['M3Forecast'] - matrizPPTO_Resultado['forecast_planta']
    matrizPPTO_Resultado['ResultadoIteracion'] = matrizPPTO_Resultado['M3Forecast']
    matrizPPTO_Resultado['M3Forecast'] = matrizPPTO_Resultado['forecast_planta'] - matrizPPTO_Resultado['gap']

    gapTotal = matrizPPTO_Resultado['gap'].abs().sum()

    print('Ajustando Volumen a proporciones')
    while( gapTotal > 1 ):

        #print(str(gapTotal) )
        
        for index, row in matrizPPTO_Resultado.iterrows():
            DesagregacionPronosticoPlantaDia.loc[DesagregacionPronosticoPlantaDia['Planta'] == row['Planta'], ['forecast_planta']] = row['M3Forecast']

        #DesagregacionPronosticoPlantaDia['M3Forecast'] = ((DesagregacionPronosticoPlantaDia['forecast_planta'] / volPais ) * (DesagregacionPronosticoPlantaDia['Días_Operativos'] * ( DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']) * DesagregacionPronosticoPlantaDia['proportion_semana'] * DesagregacionPronosticoPlantaDia['proportion_dia_semana'])).astype(float)
        DesagregacionPronosticoPlantaDia['M3Forecast'] = (#(DesagregacionPronosticoPlantaDia['forecast_planta'] / volPais ) *
                                                           DesagregacionPronosticoPlantaDia['Días_Operativos'] * 
                                                           (DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']) * 
                                                           DesagregacionPronosticoPlantaDia['proportion_semana_x'] * 
                                                           #DesagregacionPronosticoPlantaDia['proportion_semana_y']  *  
                                                           DesagregacionPronosticoPlantaDia['proportion_dia_semana_x'] #*
                                                           #DesagregacionPronosticoPlantaDia['proportion_dia_semana_y'] 
                                                           #* DesagregacionPronosticoPlantaDia['proportion_irregularidad']
                                                           #* DesagregacionPronosticoPlantaDia['Aleatorio']
                                                           #* DesagregacionPronosticoPlantaDia['cov']
                                                           ).astype(float) #+ (DesagregacionPronosticoPlantaDia['Días_Operativos'] * DesagregacionPronosticoPlantaDia['Aleatorio'] * (DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']))


        matrizTemp = pd.DataFrame(DesagregacionPronosticoPlantaDia.groupby('Planta')['M3Forecast'].sum()).reset_index()

        for index, row in matrizTemp.iterrows():
            matrizPPTO_Resultado.loc[matrizPPTO_Resultado['Planta'] == row['Planta'], ['ResultadoIteracion']] = row['M3Forecast']

        matrizPPTO_Resultado['gap'] = matrizPPTO_Resultado['ResultadoIteracion'] - matrizPPTO_Resultado['forecast_planta']
        matrizPPTO_Resultado['M3Forecast'] = matrizPPTO_Resultado['M3Forecast'] - matrizPPTO_Resultado['gap']

        gapTotal = matrizPPTO_Resultado['gap'].abs().sum()

        DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("forecast_planta",1)   
    
    print('Version finalizada')

        
    return DesagregacionPronosticoPlantaDia


#ejecutar n veces la desagregacion
 
for i in range (0, 10):
    for j in range(0, 1):
        temp = generardor_desagregacion(despachosSQL, calendarioLogistico, absorcionEstadistica, ((j + 1) * 30) , inicioHistoria, finHistoria, nivel_volatilidad, volPais, pais, despachos_irregularidades, irr_nacional )
        temp['Version'] = str(i)
        if i == 0 :
            desagregacion_temp = temp
        else :
            desagregacion_temp = pd.concat([desagregacion_temp, temp])

df_result = pd.merge (desagregacion_temp, nombre_cluster, how='left', left_on='Planta', right_on='Centro')
df_result = df_result[['pais','Ciudad', 'Centro', 'PlantaUnica', 'Fecha de entrega', 'M3Forecast', 'proportion_semana_x','proportion_dia_semana_y','proportion_irregularidad', 'Cluster']]

#df_result = df_result.groupby(['pais','Ciudad', 'Centro', 'PlantaUnica', 'Fecha de entrega', 'Cluster']).agg({'M3Forecast': 'mean'}).reset_index()
df_result = df_result.groupby(['pais','Ciudad', 'Centro', 'PlantaUnica', 'Fecha de entrega']).agg({'M3Forecast': 'mean'}).reset_index().fillna(0)
#df_result = df_result.groupby(['pais','Ciudad', 'Centro', 'PlantaUnica', 'Fecha de entrega','proportion_semana_x','proportion_dia_semana_y','proportion_irregularidad']).agg({'M3Forecast': 'mean'}).reset_index()


# GUARDAR EN EXCEL PARA DEPURACIONES    
writer = pd.ExcelWriter("./datos/Desagregacion_" + pais + "_" + pd.to_datetime("now").strftime("%Y-%m-%d-%H-%M-%S") + ".xlsx", engine='xlsxwriter')
df_result.to_excel( writer, sheet_name="Desagregacion", index=False )
writer.save()
print("exitoso")
print(df_result.head())