# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 18:08:53 2020

@author: jsdelgadoc
"""

import modulo_conn_sql as mcq
import numpy as np
import pandas as pd 
import datetime 
import matplotlib.pyplot as plt
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin

def conectarSQL():
    conn = mcq.ConexionSQL()
    cursor = conn.getCursor()
    return cursor



def obtenerDatosForecast(pais, inicio, fin):
    #Conectar con base sql y ejecutar consulta
    cursor = conectarSQL()
    try:
        cursor.execute("{CALL SCAC_AP8_BaseForecast (?,?,?)}" , (pais, inicio, fin) )
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

def obtenerCalendario(pais, year, month):
    #Conectar con base sql y ejecutar consulta
    cursor = conectarSQL()
    try:
        cursor.execute("{CALL SCAC_AP9_CalendarioLogistico (?,?,?)}" , (pais, year, month))
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

def tendencia( historia ):
    #Obtenemos Tendencia de cada centro
    promediDiarioSemanal = historia.groupby(['Año','Mes', 'Planta', 'Semana_Relativa'])['totalEntregado'].mean().reset_index()
    promediDiarioMensual = historia.groupby(['Año','Mes', 'Planta'])['totalEntregado'].mean().reset_index()
    
    TendenciaSemanal = pd.merge(promediDiarioSemanal, promediDiarioMensual, how = 'inner', left_on=['Año', 'Mes', 'Planta'], right_on = ['Año', 'Mes', 'Planta'] )
    TendenciaSemanal['TendenciaSemana'] = TendenciaSemanal['totalEntregado_x']/TendenciaSemanal['totalEntregado_y']
    TendenciaSemanal = TendenciaSemanal.groupby(['Planta', 'Semana_Relativa'])['TendenciaSemana'].mean().reset_index()
    return TendenciaSemanal 
   
def ciclicidad( historia ):
    #Obtenemos Ciclicidad de cada centro
    promediDiarioMensual = historia.groupby(['Año','Mes', 'Planta'])['totalEntregado'].mean().reset_index()
    promedioDiarioDiaSemana = despachosSQL.groupby(['Año','Mes', 'Planta', 'DiaSemana'])['totalEntregado'].mean().reset_index()
    CiclicidadDiaSemana = pd.merge(promedioDiarioDiaSemana, promediDiarioMensual, how = 'inner', left_on=['Año', 'Mes', 'Planta'], right_on = ['Año', 'Mes', 'Planta'] )
    CiclicidadDiaSemana['CiclicidadDiaSemana'] = CiclicidadDiaSemana['totalEntregado_x']/CiclicidadDiaSemana['totalEntregado_y']
    CiclicidadDiaSemana = CiclicidadDiaSemana.groupby(['Planta', 'DiaSemana'])['CiclicidadDiaSemana'].mean().reset_index()
    return CiclicidadDiaSemana

def generarDesagregacion( pais, despachosSQL, despachosSQL_historiaReciente, despachosSQL_absPlantas, absorcionEstadistica ,calendarioLogistico, volumenPais ):
    #Obtenemos Tendencia y ciclicidad de cada centro    
    TendenciaSemanal = tendencia(despachosSQL) 
    CiclicidadDiaSemana = ciclicidad(despachosSQL)
    
    
    TendenciaSemanal_historiaReciente = tendencia(despachosSQL_historiaReciente) 
    TendenciaSemanal_historiaReciente.rename(columns={'Semana_Relativa':'Semana_Relativa2', 'TendenciaSemana': 'TendenciaSemana_inst'}, inplace = True)
    CiclicidadDiaSemana_historiaReciente = ciclicidad(despachosSQL_historiaReciente)
    CiclicidadDiaSemana_historiaReciente.rename(columns={'DiaSemana':'DiaSemana2', 'CiclicidadDiaSemana': 'CiclicidadDiaSemana_inst'},inplace = True)
    
    #SETUP DESAGREGACION
    
    #AbsorcionPlanta
    if absorcionEstadistica == True:  
        DesagregacionPronosticoPlanta = pd.DataFrame(despachosSQL_absPlantas.groupby(['Planta'])['totalEntregado'].sum()).reset_index()
        DesagregacionPronosticoPlanta['totalPais'] = despachosSQL_absPlantas['totalEntregado'].sum()
        DesagregacionPronosticoPlanta['absorcionPlanta'] = DesagregacionPronosticoPlanta['totalEntregado'] / DesagregacionPronosticoPlanta['totalPais']
        DesagregacionPronosticoPlanta['M3ForecastPlanta'] = DesagregacionPronosticoPlanta['absorcionPlanta'] * volumenPais
        DesagregacionPronosticoPlanta = DesagregacionPronosticoPlanta.drop("totalEntregado", 1)
        DesagregacionPronosticoPlanta = DesagregacionPronosticoPlanta.drop("totalPais", 1)
        DesagregacionPronosticoPlanta = DesagregacionPronosticoPlanta.drop("absorcionPlanta", 1)
    
    else :
        #volPaisPlanta = pd.read_excel('../DatosAbsorcionPlantas/' + pais + '.xlsx')
        DesagregacionPronosticoPlanta = pd.read_excel('../DatosAbsorcionPlantas/' + pais + '.xlsx')
        #DesagregacionPronosticoPlanta = pd.merge(DesagregacionPronosticoPlanta, volPaisPlanta, how='left', on='Planta' )
        
    
    
    #cross join tabla DesagregacionPronostico y calendario
    calendarioLogistico['key'] = 1
    DesagregacionPronosticoPlanta['key'] = 1
    DesagregacionPronosticoPlantaDia = pd.merge(calendarioLogistico, DesagregacionPronosticoPlanta, on = 'key').drop("key",1)
    #join con tendencia y ciclicidad anual
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, TendenciaSemanal, how='left', left_on=['Planta','Semana_relativa'], right_on=['Planta','Semana_Relativa'] ).fillna(1)
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, CiclicidadDiaSemana, how='left', left_on=['Planta','Dia_Semana'], right_on=['Planta','DiaSemana'] ).fillna(1)
    #join con tendencia y ciclicidad instantanea o mensual
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, TendenciaSemanal_historiaReciente, how='left', left_on=['Planta','Semana_relativa'], right_on=['Planta','Semana_Relativa2'] ).fillna(1)
    DesagregacionPronosticoPlantaDia = pd.merge(DesagregacionPronosticoPlantaDia, CiclicidadDiaSemana_historiaReciente, how='left', left_on=['Planta','Dia_Semana'], right_on=['Planta','DiaSemana2'] ).fillna(1)
    
    #quito columnas que no me interesan
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("DiaSemana",1)
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("Semana_Relativa",1) 
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("DiaSemana2",1)
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("Semana_Relativa2",1) 
    #DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("absorcionPlanta",1) 
    
    
    #SETUP MATRIZ DE ITERACIONES
    DesagregacionPronosticoPlantaDia['M3Forecast'] = (DesagregacionPronosticoPlantaDia['Días_Operativos'] * (DesagregacionPronosticoPlantaDia['M3ForecastPlanta']/DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes'] ) * DesagregacionPronosticoPlantaDia['TendenciaSemana'] * DesagregacionPronosticoPlantaDia['CiclicidadDiaSemana'] * DesagregacionPronosticoPlantaDia['TendenciaSemana_inst'] * DesagregacionPronosticoPlantaDia['CiclicidadDiaSemana_inst'] ).astype(float)
    
    matrizPPTO_Resultado = pd.DataFrame(DesagregacionPronosticoPlantaDia.groupby('Planta')['M3Forecast'].sum()).reset_index()
    #matrizPPTO_Resultado = pd.merge(matrizPPTO_Resultado, DesagregacionPronosticoPlanta, on = 'Planta' ).drop("absorcionPlanta",1).drop("key",1)
    matrizPPTO_Resultado = pd.merge(matrizPPTO_Resultado, DesagregacionPronosticoPlanta, on = 'Planta' ).drop("key",1)
    matrizPPTO_Resultado['gap'] = matrizPPTO_Resultado['M3Forecast'] - matrizPPTO_Resultado['M3ForecastPlanta']
    matrizPPTO_Resultado['ResultadoIteracion'] = matrizPPTO_Resultado['M3Forecast']
    matrizPPTO_Resultado['M3Forecast'] = matrizPPTO_Resultado['M3ForecastPlanta'] - matrizPPTO_Resultado['gap']
    
    gapTotal = matrizPPTO_Resultado['gap'].abs().sum()
    
    while( gapTotal > 1 ):
        
        print('iteracion: ' + str(gapTotal) )
    
        for index, row in matrizPPTO_Resultado.iterrows():
            DesagregacionPronosticoPlantaDia.loc[DesagregacionPronosticoPlantaDia['Planta'] == row['Planta'], ['M3ForecastPlanta']] = row['M3Forecast']
            
        DesagregacionPronosticoPlantaDia['M3Forecast'] = (DesagregacionPronosticoPlantaDia['Días_Operativos'] * (DesagregacionPronosticoPlantaDia['M3ForecastPlanta']/DesagregacionPronosticoPlantaDia['Total_Dias_Habiles_Mes'] ) * DesagregacionPronosticoPlantaDia['TendenciaSemana'] * DesagregacionPronosticoPlantaDia['CiclicidadDiaSemana'] * DesagregacionPronosticoPlantaDia['TendenciaSemana_inst'] * DesagregacionPronosticoPlantaDia['CiclicidadDiaSemana_inst'] ).astype(float)
        
        matrizTemp = pd.DataFrame(DesagregacionPronosticoPlantaDia.groupby('Planta')['M3Forecast'].sum()).reset_index()
        
        for index, row in matrizTemp.iterrows():
            matrizPPTO_Resultado.loc[matrizPPTO_Resultado['Planta'] == row['Planta'], ['ResultadoIteracion']] = row['M3Forecast']
        
        matrizPPTO_Resultado['gap'] = matrizPPTO_Resultado['ResultadoIteracion'] - matrizPPTO_Resultado['M3ForecastPlanta']
        matrizPPTO_Resultado['M3Forecast'] = matrizPPTO_Resultado['M3Forecast'] - matrizPPTO_Resultado['gap']
        
        gapTotal = matrizPPTO_Resultado['gap'].abs().sum()
        
    DesagregacionPronosticoPlantaDia = DesagregacionPronosticoPlantaDia.drop("M3ForecastPlanta",1)    
    
    return DesagregacionPronosticoPlantaDia
    

#PRINCIPAL

#años para entrenar las versiones
yearDesagregacion = 2021
mesDesagregacion = 3

#Targets
pais = 'Puerto Rico'    
volPais = 4912
yearTarget = 2021
mesTarget = 4

inicioHistoria = datetime.datetime(2019, 1, 1) #'2013-05-01'
finHistoria = datetime.datetime.today() #fecha actual

historiaAbsorcionPlanta = 30

#Consulta de datos en la base SQL
despachosSQL = obtenerDatosForecast(  pais, inicioHistoria.strftime("%Y-%m-%d"), finHistoria.strftime("%Y-%m-%d") )
calendarioLogistico = obtenerCalendario( pais, yearDesagregacion, mesDesagregacion)

#arreglo de formatos
despachosSQL['totalEntregado'] = despachosSQL['totalEntregado'].astype(float)
calendarioLogistico['Dia_Semana'] = calendarioLogistico['Dia_Semana'].astype(int)

#___________datos para probar precision_______________
#inicioMesActual = datetime.datetime.today() - MonthBegin(1)
#fechaMesActual = datetime.datetime.today()
inicioMesActual = datetime.datetime(yearDesagregacion, mesDesagregacion , 1)

#######################################################____________________________________-----------------------MMMMMMMMMMMMMMMM
#fechaMesActual = datetime.datetime.today()
fechaMesActual = datetime.datetime(yearDesagregacion, mesDesagregacion , 1) + MonthEnd(1)


despacho_test_pivot =  despachosSQL.loc[ (despachosSQL['FechaEntrega'] >= inicioMesActual.strftime("%Y-%m-%d")) & 
                                  (despachosSQL['FechaEntrega'] <= fechaMesActual.strftime("%Y-%m-%d")) ]

despacho_test_pivot = pd.DataFrame(despacho_test_pivot[['Planta', 'FechaEntrega', 'totalEntregado']] )


#el fin de la historia siempre va a ser el mes anterior al actual, para dejar los datos de este mes como testeo
fin = datetime.datetime.today() - datetime.timedelta(45)
fin = fin + MonthEnd(1)

#determino cuantos años tengo de historia
deltaYears = finHistoria.year - inicioHistoria.year

desagregacion = pd.DataFrame()

for i in range (deltaYears):
    print('año: ' + str(i+1))    
    inicio = datetime.datetime.today() - datetime.timedelta((i+1)* 365)
    inicio = inicio - MonthBegin(1)
    
    #filtro el despacho (solo la historia de cada mes ej, sept 2019, 2018, 2017...), voy aumentando historia anual en cada ciclo de i
    despacho_anual_filtrados = despachosSQL.loc[ (despachosSQL['FechaEntrega'] >= inicio.strftime("%Y-%m-%d")) & 
                                           (despachosSQL['FechaEntrega'] <= fin.strftime("%Y-%m-%d")) &
                                           (despachosSQL['FechaEntrega'].dt.month == mesDesagregacion) 
                                           ].reset_index()
    
    #historia instantanea
    for j in range (12):
        print('mes: ' + str(j+1))
        inicio_menusal = datetime.datetime.today() - datetime.timedelta((j+1)* 30*2)
        inicio_menusal = inicio_menusal - MonthBegin(1)
        despacho_mensual_filtrado =  despachosSQL.loc[ (despachosSQL['FechaEntrega'] >= inicio_menusal.strftime("%Y-%m-%d"))& 
                                           (despachosSQL['FechaEntrega'] <= fin.strftime("%Y-%m-%d"))
                                           ].reset_index()
        
        #-------Despacho absorcion plantas--------------
        despacho_Abs = despachosSQL.loc[ (despachosSQL['FechaEntrega']  >= datetime.datetime.today() - datetime.timedelta(historiaAbsorcionPlanta))].reset_index()
                
        #------------------------ GENERO LA DESAGREGACION ------------------------
        """ 
        pais:nombre pais
        despacho_anual: dataframe
        despacho_mensual: dataframe
        absorcion estadistica: booleano, True -> tomar unicamente la absorcion de los ultimos 30 dias, False-> tomar archivo con volumenes establecidos por planta
        calendarioLogistico: calendario del mes target
        volPais: numero del forecast a nivel nacional
        """
        #en este paso el parametro booleano deberia ser siempre TRUE !!!!!!!!!!! PILAS !!!!!!!!!!!!!
        temp = generarDesagregacion(pais, despacho_anual_filtrados, despacho_mensual_filtrado, despacho_Abs, True ,calendarioLogistico, volPais) 
        
        #Agrego nombre de version y volumen total
        temp['Version'] = 'M3Forecast_' + str(i) +'_'+ str(j)
        
        #Si es primera iteracion defino una base en que el M3Forecast es el Real, sobretodo por temas de plots
        if i == 0 and j == 0:
            desagregacion = pd.merge( temp, despacho_test_pivot, how = 'left',  left_on=['Planta', 'Fecha de entrega'], right_on=['Planta', 'FechaEntrega'] ).drop('FechaEntrega', 1)
            desagregacion['M3Forecast'] = desagregacion['totalEntregado']
            desagregacion['Version'] = 'Real'
            
        temp = pd.merge( temp, despacho_test_pivot, how = 'left',  left_on=['Planta', 'Fecha de entrega'], right_on=['Planta', 'FechaEntrega'] ).drop('FechaEntrega', 1)
        desagregacion = pd.concat([desagregacion, temp])

        
desagregacion['APE'] =( ((desagregacion['totalEntregado'] - desagregacion['M3Forecast']).abs())/desagregacion['totalEntregado'] ).fillna(0)
desagregacion['totalEntregado'] = desagregacion['totalEntregado'].fillna(0)

#Calculo de MAPE

mape_versiones = desagregacion.loc[desagregacion['Version'] != 'Real'].groupby(['Planta','Version'])['APE'].mean().reset_index()
version_mape_min = mape_versiones.loc[ mape_versiones.groupby(['Planta'])['APE'].idxmin()]

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#Una vez encontrandos las versiones optimizadas, procedemos a desagregar el mes target
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
print("DESAGREGACION MES TARGET")

calendarioLogistico = obtenerCalendario( pais, yearTarget, mesTarget)
calendarioLogistico['Dia_Semana'] = calendarioLogistico['Dia_Semana'].astype(int)
desagregacionTarget = pd.DataFrame()

for i in range (deltaYears):
    print('año: ' + str(i+1))    
    inicio = datetime.datetime.today() - datetime.timedelta((i+1)* 365)
    inicio = inicio - MonthBegin(1)
    
    #filtro el despacho (solo la historia de cada mes ej, sept 2019, 2018, 2017...), voy aumentando historia anual en cada ciclo de i
    despacho_anual_filtrados = despachosSQL.loc[ (despachosSQL['FechaEntrega'] >= inicio.strftime("%Y-%m-%d")) & 
                                           (despachosSQL['FechaEntrega'] <= fin.strftime("%Y-%m-%d")) &
                                           (despachosSQL['FechaEntrega'].dt.month == mesTarget) 
                                           ].reset_index()
    
    #historia instantanea
    for j in range (12):
        print('mes: ' + str(j+1))
        inicio_menusal = datetime.datetime.today() - datetime.timedelta((j+1)* 30)
        inicio_menusal = inicio_menusal - MonthBegin(1)
        despacho_mensual_filtrado =  despachosSQL.loc[ (despachosSQL['FechaEntrega'] >= inicio_menusal.strftime("%Y-%m-%d"))& 
                                           (despachosSQL['FechaEntrega'] <= fin.strftime("%Y-%m-%d"))
                                           ].reset_index()
        
        #-------Despacho absorcion plantas--------------
        despacho_Abs = despachosSQL.loc[ (despachosSQL['FechaEntrega']  >= datetime.datetime.today() - datetime.timedelta(historiaAbsorcionPlanta))].reset_index()
                
        #------------------------ GENERO LA DESAGREGACION ------------------------
        """ 
        pais:nombre pais
        despacho_anual: dataframe
        despacho_mensual: dataframe
        absorcion estadistica: booleano, True -> tomar unicamente la absorcion de los ultimos 30 dias, False-> tomar archivo con volumenes establecidos por planta
        calendarioLogistico: calendario del mes target
        volPais: numero del forecast a nivel nacional
        """
        #PARAMETRO IMPORTANTE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        temp = generarDesagregacion(pais, despacho_anual_filtrados, despacho_mensual_filtrado, despacho_Abs, True ,calendarioLogistico, volPais) 
        
        #Agrego nombre de version y volumen total
        temp['Version'] = 'M3Forecast_' + str(i) +'_'+ str(j)
        
        #Si es primera iteracion defino una base en que el M3Forecast es el Real, sobretodo por temas de plots
        if i == 0 and j == 0:
            desagregacionTarget = pd.merge( temp, despacho_test_pivot, how = 'left',  left_on=['Planta', 'Fecha de entrega'], right_on=['Planta', 'FechaEntrega'] ).drop('FechaEntrega', 1)
            desagregacionTarget['M3Forecast'] = desagregacionTarget['totalEntregado']
            desagregacionTarget['Version'] = 'Real'
            
        temp = pd.merge( temp, despacho_test_pivot, how = 'left',  left_on=['Planta', 'Fecha de entrega'], right_on=['Planta', 'FechaEntrega'] ).drop('FechaEntrega', 1)
        desagregacionTarget = pd.concat([desagregacionTarget, temp])

desagregacion_optimizada = pd.merge(desagregacionTarget, version_mape_min, how='inner', left_on=['Planta','Version'], right_on=['Planta','Version']  )

    
# GUARDAR EN EXCEL PARA DEPURACIONES    
nombre_cluster = querySQL( "SELECT Centro, Ciudad_Cluster as Ciudad, [Desc Cluster] as Cluster, [Planta Unica] as PlantaUnica FROM SCAC_AT1_NombreCluster where Pais = ?" , (pais) )
df_result = pd.merge (desagregacion_optimizada, nombre_cluster, how='left', left_on='Planta', right_on='Centro')
df_result = df_result[['pais', 'Ciudad', 'Centro', 'PlantaUnica', 'Fecha de entrega', 'M3Forecast']]

writer = pd.ExcelWriter("../datos/Desagregacion_" + pais + "_" + pd.to_datetime("now").strftime("%Y-%m-%d-%H-%M-%S") + ".xlsx", engine='xlsxwriter')
df_result.to_excel( writer, sheet_name="Desagregacion" )
#mape_versiones.to_excel( writer, sheet_name="Mape" )
writer.save()

