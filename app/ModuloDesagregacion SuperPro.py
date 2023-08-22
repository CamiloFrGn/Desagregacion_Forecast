# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 14:48:48 2021

@author: jsdelgadoc
"""

import modulo_conn_sql as mcq
import numpy as np
import pandas as pd 
import datetime 
import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
import random

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
    promedio_group_bottom = dataset.groupby(array_group_bottom)[medida_target].mean().reset_index()    
    proportion  = pd.merge(promedio_group_bottom, promedio_group_top, how = 'inner', left_on = array_group_top, right_on = array_group_top )
    proportion['proportion'] = proportion[medida_target + '_x'] / proportion[medida_target + '_y']
    proportion = proportion.groupby(group_target)['proportion'].mean().reset_index()
    proportion.rename(columns={'proportion':'proportion_' + name_proportion}, inplace = True)
    
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

#años para entrenar las versiones
yearDesagregacion = 2022
mesDesagregacion = 1

#Targets
pais = 'Colombia'
volPais = 118756

nivel_volatilidad = 0.4

inicioHistoria = datetime.datetime(2015, 1, 1) #'2013-05-01'
finHistoria = datetime.datetime(2021, 12 , 31)

criterio_historia_reciente = 30 #dias
absorcionEstadistica = True #criterio para tomar las proporciones historicas de las plantas o False -> archivo que define el volumen por planta

#Consulta de datos en la base SQL
despachosSQL = querySQL(  "{CALL SCAC_AP8_BaseForecast (?,?,?)}", (pais, inicioHistoria.strftime("%Y-%m-%d"), finHistoria.strftime("%Y-%m-%d") ) )
calendarioLogistico = querySQL( "{CALL SCAC_AP9_CalendarioLogistico (?,?,?)}" , (pais, yearDesagregacion, mesDesagregacion))

#arreglo de formatos
despachosSQL['totalEntregado'] = despachosSQL['totalEntregado'].astype(float)
calendarioLogistico['Dia_Semana'] = calendarioLogistico['Dia_Semana'].astype(int)

#Proporciones con toda la historia

#proportion_plant = historical_proportion(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Año', 'Mes'], ['Año', 'Mes', 'Planta'], 'totalEntregado', ['Planta'], 'planta')
proportion_plant = historical_proportion(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente))], ['Año', 'Mes'], ['Año', 'Mes', 'Planta'], 'totalEntregado', ['Planta'], 'planta')

proportion_week = historical_proportion(despachosSQL, ['Año', 'Mes', 'Planta'], ['Año', 'Mes', 'Planta', 'Semana_Relativa'], 'totalEntregado', ['Planta', 'Semana_Relativa'], 'semana')
proportion_week_recent = historical_proportion(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Año', 'Mes', 'Planta'], ['Año', 'Mes', 'Planta', 'Semana_Relativa'], 'totalEntregado', ['Planta', 'Semana_Relativa'], 'semana')

proportion_wday = historical_proportion(despachosSQL, ['Año', 'Mes', 'Planta'], ['Año', 'Mes', 'Planta', 'DiaSemana'], 'totalEntregado', ['Planta', 'DiaSemana'], 'dia_semana')
proportion_wday_recent = historical_proportion(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Año', 'Mes', 'Planta'], ['Año', 'Mes', 'Planta', 'DiaSemana'], 'totalEntregado', ['Planta', 'DiaSemana'], 'dia_semana')


cov_plantas = stats_serie(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Planta'], 'totalEntregado')

cov_planta_diasemana = stats_serie(despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ], ['Planta', 'DiaSemana'], 'totalEntregado')

    #--------------- ESTIMACION DESAGREGACION FORECAST POR PLANTA ---------------# 

if absorcionEstadistica == True:
    #obtengo el listado de plantas que han estado activas los ultimos N dias
    DesagregacionPronosticoPlanta = despachosSQL[despachosSQL['FechaEntrega'] >= (finHistoria - datetime.timedelta(criterio_historia_reciente)) - MonthBegin(1) ]
    DesagregacionPronosticoPlanta = pd.DataFrame({'Planta' : DesagregacionPronosticoPlanta["Planta"].unique()})
    #se divide el volumen por igual en cada planta
    DesagregacionPronosticoPlanta['M3ForecastPlanta'] = volPais / DesagregacionPronosticoPlanta['Planta'].count()
    
    #join con proporcion planta
    DesagregacionPronosticoPlanta = pd.merge(DesagregacionPronosticoPlanta, proportion_plant, how='left', left_on=['Planta'], right_on=['Planta'] ).fillna(1)
    #join con proporcion planta
    DesagregacionPronosticoPlanta = pd.merge(DesagregacionPronosticoPlanta, cov_plantas, how='left', left_on=['Planta'], right_on=['Planta'] ).fillna(1)
    
    DesagregacionPronosticoPlanta['Aleatorio_planta'] = DesagregacionPronosticoPlanta['cov'].apply( random_number) * nivel_volatilidad
    
    #se itera con las proporciones y se ajusta el resultado hasta que el gap sea menor a 1
    gap_iteracion = 1000.0
    while( abs(gap_iteracion) > 1 ):
        #print('iteracion Planta: ' + str(gap_iteracion) )
        DesagregacionPronosticoPlanta['forecast_planta'] = DesagregacionPronosticoPlanta['M3ForecastPlanta'] *  DesagregacionPronosticoPlanta['proportion_planta'] *  DesagregacionPronosticoPlanta['Aleatorio_planta'] 
        resultado_iteracion = DesagregacionPronosticoPlanta['forecast_planta'].sum()
        gap_iteracion = resultado_iteracion - volPais
        DesagregacionPronosticoPlanta['M3ForecastPlanta'] = DesagregacionPronosticoPlanta['M3ForecastPlanta'] - (gap_iteracion / DesagregacionPronosticoPlanta['Planta'].count() )
    
    DesagregacionPronosticoPlanta = DesagregacionPronosticoPlanta[['Planta','forecast_planta']]
    
else:
    DesagregacionPronosticoPlanta = pd.read_excel('../DatosAbsorcionPlantas/' + pais + '.xlsx')


    #--------------- ESTIMACION DESAGREGACION FORECAST POR DIA ---------------#
    
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
DesagregacionPronosticoPlantaDia['M3Forecast'] =  (DesagregacionPronosticoPlantaDia['Días_Operativos'] * ( DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']) * DesagregacionPronosticoPlantaDia['proportion_semana_x'] * DesagregacionPronosticoPlantaDia['proportion_dia_semana_y'] * DesagregacionPronosticoPlantaDia['Aleatorio'] ).astype(float)

matrizPPTO_Resultado = pd.DataFrame(DesagregacionPronosticoPlantaDia.groupby('Planta')['M3Forecast'].sum()).reset_index()
matrizPPTO_Resultado = pd.merge(matrizPPTO_Resultado, DesagregacionPronosticoPlanta, on = 'Planta' ).drop("key",1)
matrizPPTO_Resultado['gap'] = matrizPPTO_Resultado['M3Forecast'] - matrizPPTO_Resultado['forecast_planta']
matrizPPTO_Resultado['ResultadoIteracion'] = matrizPPTO_Resultado['M3Forecast']
matrizPPTO_Resultado['M3Forecast'] = matrizPPTO_Resultado['forecast_planta'] - matrizPPTO_Resultado['gap']

gapTotal = matrizPPTO_Resultado['gap'].abs().sum()

while( gapTotal > 1 ):
    
    print('iteracion: ' + str(gapTotal) )

    for index, row in matrizPPTO_Resultado.iterrows():
        DesagregacionPronosticoPlantaDia.loc[DesagregacionPronosticoPlantaDia['Planta'] == row['Planta'], ['forecast_planta']] = row['M3Forecast']
        
    #DesagregacionPronosticoPlantaDia['M3Forecast'] = ((DesagregacionPronosticoPlantaDia['forecast_planta'] / volPais ) * (DesagregacionPronosticoPlantaDia['Días_Operativos'] * ( DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']) * DesagregacionPronosticoPlantaDia['proportion_semana'] * DesagregacionPronosticoPlantaDia['proportion_dia_semana'])).astype(float)
    DesagregacionPronosticoPlantaDia['M3Forecast'] = ((DesagregacionPronosticoPlantaDia['forecast_planta'] / volPais ) *
                                                       DesagregacionPronosticoPlantaDia['Días_Operativos'] * 
                                                       (DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']) * 
                                                       DesagregacionPronosticoPlantaDia['proportion_semana_x'] * 
                                                       #DesagregacionPronosticoPlantaDia['proportion_semana_y']  *  
                                                       #DesagregacionPronosticoPlantaDia['proportion_dia_semana_x'] * 
                                                       DesagregacionPronosticoPlantaDia['proportion_dia_semana_y'] 
                                                       * DesagregacionPronosticoPlantaDia['Aleatorio']
                                                       #DesagregacionPronosticoPlantaDia['cov']
                                                       ).astype(float) #+ (DesagregacionPronosticoPlantaDia['Días_Operativos'] * DesagregacionPronosticoPlantaDia['Aleatorio'] * (DesagregacionPronosticoPlantaDia['forecast_planta'] / DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes']))

    #DesagregacionPronosticoPlantaDia['cov'] = DesagregacionPronosticoPlantaDia['cov'] + ((1 - DesagregacionPronosticoPlantaDia['cov']) / 50)
    #DesagregacionPronosticoPlantaDia['cov'] = DesagregacionPronosticoPlantaDia['cov'] / 50
    #se agrega aleatoriedad a la desagregacion
    #DesagregacionPronosticoPlantaDia['Aleatorio'] = DesagregacionPronosticoPlantaDia['cov'].apply( random_number)
    
    #DesagregacionPronosticoPlantaDia['proportion_semana_y'] = ([random.uniform(1 - (DesagregacionPronosticoPlantaDia['cov']).abs() , 1), DesagregacionPronosticoPlantaDia['proportion_semana_y']]).mean()
    #DesagregacionPronosticoPlantaDia['proportion_dia_semana_y'] = random.uniform(1 - (DesagregacionPronosticoPlantaDia['cov']).abs() , 1)  * DesagregacionPronosticoPlantaDia['proportion_dia_semana_y']
    
    #el cov se va auemntando hacia 1 para asegurar convergencia

    
    matrizTemp = pd.DataFrame(DesagregacionPronosticoPlantaDia.groupby('Planta')['M3Forecast'].sum()).reset_index()
    
    for index, row in matrizTemp.iterrows():
        matrizPPTO_Resultado.loc[matrizPPTO_Resultado['Planta'] == row['Planta'], ['ResultadoIteracion']] = row['M3Forecast']
    
    matrizPPTO_Resultado['gap'] = matrizPPTO_Resultado['ResultadoIteracion'] - matrizPPTO_Resultado['forecast_planta']
    matrizPPTO_Resultado['M3Forecast'] = matrizPPTO_Resultado['M3Forecast'] - matrizPPTO_Resultado['gap']
    
    gapTotal = matrizPPTO_Resultado['gap'].abs().sum()
    
DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("forecast_planta",1)   

# GUARDAR EN EXCEL PARA DEPURACIONES    
nombre_cluster = querySQL( "SELECT Centro, Ciudad_Cluster as Ciudad, [Desc Cluster] as Cluster, [Planta Unica] as PlantaUnica FROM SCAC_AT1_NombreCluster where Pais = ?" , (pais) )
df_result = pd.merge (DesagregacionPronosticoPlantaDia, nombre_cluster, how='left', left_on='Planta', right_on='Centro')
df_result = df_result[['pais', 'Ciudad', 'Centro', 'PlantaUnica', 'Fecha de entrega', 'M3Forecast']]

writer = pd.ExcelWriter("../datos/Desagregacion_" + pais + "_" + pd.to_datetime("now").strftime("%Y-%m-%d-%H-%M-%S") + ".xlsx", engine='xlsxwriter')
df_result.to_excel( writer, sheet_name="Desagregacion" )
writer.save()

