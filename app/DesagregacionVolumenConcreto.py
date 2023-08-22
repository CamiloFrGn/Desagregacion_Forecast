import modulo_conn_sql as mcq
import numpy as np
import pandas as pd 
import datetime 
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin

import sqlalchemy as sa
import urllib

class DesagregacionVolumenConcreto():
    
    def __init__(
            self,
            pais,
            inicioHistoria,
            finHistoria):
        
        self.df = self.querySQL( "{CALL SCAC_AP8_BaseForecast (?,?,?)}", (pais, inicioHistoria, finHistoria ) )
        self.df['year_month'] = self.df.FechaEntrega.dt.to_period('M')
        self.df['totalEntregado'] = self.df['totalEntregado'].astype(float)
        
        #SQL Methods to get operation data
    def conectarSQL(self):
        conn = mcq.ConexionSQL()
        cursor = conn.getCursor()
        return cursor
        

    #Query BD SQL-Server Cemex
    def querySQL(self, query, parametros):
        #Conectar con base sql y ejecutar consulta
        cursor = self.conectarSQL()
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



    def historical_proportion( 
        self,
        dataset, 
        array_group_top, 
        array_group_bottom, 
        medida_target, 
        group_target, 
        name_proportion  ):
        """    
             
        Args:
            dataset(pandas dataframe): base de datos con el historico
            array_group_top(array) : array de la jerarquia mas ALTA con el nombre de las columnas del dataset por el cual se quiere agrupar las proporciones
            array_group_bottom(array) : array de la jerarquia mas BAJA con el nombre de las columnas del dataset por el cual se quiere agrupar las proporciones
            medida_target( string ) : nombre de la columna que contiene los datos objetivo de la proporcion
            group_target(array) : array de nombre de columnas con las cuales queremos la proporcion final
            name_proportion(string) : etiqueta de la dimension a la cual le estamos calculando la proporcion
        """

        promedio_group_top = dataset.groupby(array_group_top)[medida_target].mean().reset_index()
        promedio_group_bottom = dataset.groupby(array_group_bottom)[medida_target].mean().reset_index()   

        proportion  = pd.merge(
            promedio_group_bottom, 
            promedio_group_top, 
            how = 'inner', 
            left_on = array_group_top, 
            right_on = array_group_top )

        proportion['proportion'] = proportion[medida_target + '_x'] / proportion[medida_target + '_y']
        proportion = proportion.groupby(group_target)['proportion'].mean().reset_index()
        proportion.rename(columns={'proportion':'proportion_' + name_proportion}, inplace = True)
        
        return proportion 

    def calculate_proportion(
        self,
        df_total, 
        months, 
        denominador, 
        numerador, 
        columna_target, 
        grupo_final, 
        name_column_return):
        
        df_proportion_semana = pd.DataFrame()

        for i in df_total['year_month'].unique():

            mes =  df_total[df_total['year_month'] == i]['Mes'].unique()[0]
            año =  df_total[df_total['year_month'] == i]['Año'].unique()[0]

            df_param = df_total[
                (df_total['FechaEntrega'] >= datetime.datetime(año, mes , 1) - MonthBegin(months)) &  
                (df_total['FechaEntrega'] < datetime.datetime(año, mes , 1))
                ]

            if len(df_param) > 0:

                df_proportion_semana_detalle = self.historical_proportion(
                    df_param, 
                    denominador, 
                    numerador, 
                    columna_target, 
                    grupo_final, 
                    name_column_return)

                df_proportion_semana_detalle['Año'] = año
                df_proportion_semana_detalle['Mes'] = mes
                if len(df_proportion_semana_detalle) == 0:
                    df_proportion_semana = df_proportion_semana_detalle
                else:
                    df_proportion_semana = pd.concat([
                        df_proportion_semana, 
                        df_proportion_semana_detalle
                        ])

        df_proportion_semana = df_proportion_semana.fillna(0)
        
        return df_proportion_semana.reset_index(drop=True)

    def media_diaria(self, df_total, months):
    
        media_diaria_total = pd.DataFrame()
    
        for i in df_total['year_month'].unique():
    
            mes =  df_total[df_total['year_month'] == i]['Mes'].unique()[0]
            año =  df_total[df_total['year_month'] == i]['Año'].unique()[0]
    
            df_param = df_total[
                (df_total['FechaEntrega'] >= datetime.datetime(año, mes , 1) - MonthBegin(months)) &  
                (df_total['FechaEntrega'] < datetime.datetime(año, mes , 1))
                ]
    
            media_diaria = df_param.groupby(
                [
                'Año', 
                'Mes', 
                'Planta'
                ]
            ).agg(
                {
                'totalEntregado': 'sum', 
                'DiasOperativos':'max' }
            ).reset_index()
    
            media_diaria['media'] = media_diaria['totalEntregado'] /  media_diaria['DiasOperativos']
    
            media_diaria = media_diaria.groupby(['Planta'])['media'].mean().reset_index()
            media_diaria['Año'] = año
            media_diaria['Mes'] = mes
            media_diaria.rename(columns={'media':'media'+str(months)}, inplace = True)
            
            if len(media_diaria_total) == 0:
                media_diaria_total = media_diaria
            else:
                media_diaria_total = pd.concat([media_diaria_total, media_diaria])
        
        return media_diaria_total.reset_index(drop=True)
    
    def construccion_dataset(self, despachosSQL):
        
        proportion_week_1 = self.calculate_proportion(despachosSQL, 
                                         1, 
                                         ['Año', 'Mes', 'Planta'], 
                                         ['Año', 'Mes', 'Planta', 'Semana_Relativa'], 
                                         'totalEntregado', 
                                         ['Planta', 'Semana_Relativa'],
                                         'semana' + str(1))

        proportion_week_2 =  self.calculate_proportion(despachosSQL, 
                                                 2, 
                                                 ['Año', 'Mes', 'Planta'], 
                                                 ['Año', 'Mes', 'Planta', 'Semana_Relativa'], 
                                                 'totalEntregado', 
                                                 ['Planta', 'Semana_Relativa'],
                                                 'semana' + str(2))
        
        proportion_week_3 =  self.calculate_proportion(despachosSQL, 
                                                 3, 
                                                 ['Año', 'Mes', 'Planta'], 
                                                 ['Año', 'Mes', 'Planta', 'Semana_Relativa'], 
                                                 'totalEntregado', 
                                                 ['Planta', 'Semana_Relativa'],
                                                 'semana' + str(3))
        
        proportion_week_6 =  self.calculate_proportion(despachosSQL, 
                                                 6, 
                                                 ['Año', 'Mes', 'Planta'], 
                                                 ['Año', 'Mes', 'Planta', 'Semana_Relativa'], 
                                                 'totalEntregado', 
                                                 ['Planta', 'Semana_Relativa'],
                                                 'semana' + str(6))
        
        proportion_weekday_1 = self.calculate_proportion(despachosSQL, 
                                         1, 
                                         ['Año', 'Mes', 'Planta'], 
                                         ['Año', 'Mes', 'Planta', 'DiaSemana'], 
                                         'totalEntregado', 
                                         ['Planta', 'DiaSemana'],
                                         'dia_semana' + str(1))

        proportion_weekday_2 = self.calculate_proportion(despachosSQL, 
                                                 2, 
                                                 ['Año', 'Mes', 'Planta'], 
                                                 ['Año', 'Mes', 'Planta', 'DiaSemana'], 
                                                 'totalEntregado', 
                                                 ['Planta', 'DiaSemana'],
                                                 'dia_semana' + str(2))
        
        proportion_weekday_3 = self.calculate_proportion(despachosSQL, 
                                                 3, 
                                                 ['Año', 'Mes', 'Planta'], 
                                                 ['Año', 'Mes', 'Planta', 'DiaSemana'], 
                                                 'totalEntregado', 
                                                 ['Planta', 'DiaSemana'],
                                                 'dia_semana' + str(3))
        
        proportion_weekday_6 = self.calculate_proportion(despachosSQL, 
                                                 6, 
                                                 ['Año', 'Mes', 'Planta'], 
                                                 ['Año', 'Mes', 'Planta', 'DiaSemana'], 
                                                 'totalEntregado', 
                                                 ['Planta', 'DiaSemana'],
                                                 'dia_semana' + str(6))
                
        media_diaria1 = self.media_diaria(despachosSQL, 1)
        media_diaria2 = self.media_diaria(despachosSQL, 2)
        media_diaria3 = self.media_diaria(despachosSQL, 3)
        media_diaria6 = self.media_diaria(despachosSQL, 6)

        self.df = pd.merge(despachosSQL, proportion_week_1, on=['Año', 'Mes', 'Planta', 'Semana_Relativa'], how='left')
        df = pd.merge(df, proportion_week_2, on=['Año', 'Mes', 'Planta', 'Semana_Relativa'], how='left')
        df = pd.merge(df, proportion_week_3, on=['Año', 'Mes', 'Planta', 'Semana_Relativa'], how='left')
        df = pd.merge(df, proportion_week_6, on=['Año', 'Mes', 'Planta', 'Semana_Relativa'], how='left')
        
        df = pd.merge(df, proportion_weekday_1, on=['Año', 'Mes', 'Planta', 'DiaSemana'], how='left')
        df = pd.merge(df, proportion_weekday_2, on=['Año', 'Mes', 'Planta', 'DiaSemana'], how='left')
        df = pd.merge(df, proportion_weekday_3, on=['Año', 'Mes', 'Planta', 'DiaSemana'], how='left')
        df = pd.merge(df, proportion_weekday_6, on=['Año', 'Mes', 'Planta', 'DiaSemana'], how='left')
        
        df = pd.merge(df, media_diaria1, on=['Año', 'Mes', 'Planta'], how='left')
        df = pd.merge(df, media_diaria2, on=['Año', 'Mes', 'Planta'], how='left')
        df = pd.merge(df, media_diaria3, on=['Año', 'Mes', 'Planta'], how='left')
        df = pd.merge(df, media_diaria6, on=['Año', 'Mes', 'Planta'], how='left')
        
        df['PlantaCentral'] = np.select(
            [
                df['TipoPlanta'] == 'Central'
            ],
            [
                1
            ],default=0)
        
        df = df.fillna(0)


    def run(self):

        self.construccion_dataset(self.df)

if __name__ == "__main__":
    
    new_forecast = DesagregacionVolumenConcreto('Colombia', '2021-01-01', '2021-12-31')
    new_forecast.run()