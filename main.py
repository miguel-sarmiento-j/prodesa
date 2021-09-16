
# gcloud functions deploy constructionReport_15 --runtime python37 --trigger-http --memory=8GiB --timeout=500


from google.cloud import storage
import pandas as pd
import numpy as np

from google.cloud import bigquery

print("Running Python Script")
    
#Taking the File from the Input Bucket

bucket_name = "prodesa-biaas-bucket"
blob_name = "Consolidado_Excel_13-08-2021.xlsx"

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)

data_bytes = blob.download_as_bytes()

esp_consolidado_corte = pd.read_excel(data_bytes)

# DataSet Preparation

print("Data loaded!!!")

stg_consolidado_corte=pd.DataFrame([],columns=['stg_project_id'])

stg_consolidado_corte['stg_project_id']=pd.to_numeric(esp_consolidado_corte['ID'])
stg_consolidado_corte['stg_wbs']=esp_consolidado_corte['WBS']
stg_consolidado_corte['stg_nombre_actividad']=esp_consolidado_corte['NAME']

auxCol=esp_consolidado_corte['ACTUAL_START_DATE'].str.split(" ", n=2,expand=True)
stg_consolidado_corte['stg_fecha_inicial_actual']=pd.to_datetime(auxCol[1], dayfirst=True)
auxCol=esp_consolidado_corte['ACTUAL_FINISH_DATE'].str.split(" ", n=2,expand=True)
stg_consolidado_corte['stg_fecha_final_actual']=pd.to_datetime(auxCol[1], dayfirst=True)

auxCol=esp_consolidado_corte['Duración_real'].str.split(" ", n=2,expand=True)
auxCol[0] = auxCol[0].str.replace(',','.')
stg_consolidado_corte['stg_duracion_real_cantidad']=pd.to_numeric(auxCol[0])
stg_consolidado_corte['stg_duracion_real_unidad']=auxCol[1]

auxCol=esp_consolidado_corte['LIKELY_START_DATE'].str.split(" ", n=2,expand=True)
stg_consolidado_corte['stg_fecha_inicio_planeada']=pd.to_datetime(auxCol[1], dayfirst=True)

auxCol=esp_consolidado_corte['DURATION'].str.split(" ", n=2,expand=True)
auxCol[0] = auxCol[0].str.replace(',','.')
stg_consolidado_corte['stg_duracion_cantidad']=pd.to_numeric(auxCol[0])
stg_consolidado_corte['stg_duracion_unidad']=auxCol[1]

auxCol=esp_consolidado_corte['LOW_RISK_DURATION'].str.split(" ", n=2,expand=True)
auxCol[0] = auxCol[0].str.replace(',','.')
stg_consolidado_corte['stg_riesgo_bajo_duracion_cantidad']=pd.to_numeric(auxCol[0])
stg_consolidado_corte['stg_riesgo_bajo_duracion_unidad']=auxCol[1]

stg_consolidado_corte['stg_recursos']=esp_consolidado_corte['RESOURCES']
stg_consolidado_corte['stg_actividad_sucesora']=esp_consolidado_corte['SUCCESORS']

stg_consolidado_corte['stg_impacto_buffer']=pd.to_numeric(esp_consolidado_corte['PROJECT_BUFFER_IMPACT'])
auxCol=esp_consolidado_corte['INDICATOR'].str.split(" ", n=2,expand=True)
auxCol[0] = auxCol[0].str.replace(',','.')
stg_consolidado_corte['stg_indicador_cantidad']=pd.to_numeric(auxCol[0])
stg_consolidado_corte['stg_indicador_unidad']=auxCol[1]

stg_consolidado_corte['stg_porc_completitud']=pd.to_numeric(esp_consolidado_corte['PCT_COMPLETED'])

stg_consolidado_corte['stg_ind_buffer']=esp_consolidado_corte['BUFFER']
stg_consolidado_corte['stg_indice_buffer']=esp_consolidado_corte['BUFFER_INDEX']

auxCol=esp_consolidado_corte['CRITICAL_NUMBER'].str.split(" ", n=2,expand=True)
auxCol[0] = auxCol[0].str.replace(',','.')
stg_consolidado_corte['stg_duracion_critica_cantidad']=pd.to_numeric(auxCol[0])
stg_consolidado_corte['stg_duracion_critica_unidad']=auxCol[1]

stg_consolidado_corte['stg_ind_tarea']=esp_consolidado_corte['TASK']
stg_consolidado_corte['stg_ind_tarea_critica']=esp_consolidado_corte['CRITICAL_TASK']

stg_consolidado_corte['stg_estado']=pd.to_numeric(esp_consolidado_corte['STATE'])

stg_consolidado_corte['stg_numero_esquema']=esp_consolidado_corte['SCHEME_NUMBER']

auxCol=esp_consolidado_corte['LIKELY_FINISH_DATE'].str.split(" ", n=2,expand=True)
stg_consolidado_corte['stg_fecha_fin_planeada']=pd.to_datetime(auxCol[1], dayfirst=True)

stg_consolidado_corte['stg_nombre_archivo']=esp_consolidado_corte['PROJECT']

auxCol=esp_consolidado_corte["PROJECT"].str.split("_", n=5,expand=True)

stg_consolidado_corte['stg_area_prodesa']=auxCol[0]
stg_consolidado_corte['stg_codigo_proyecto']=auxCol[1]
stg_consolidado_corte['stg_etapa_proyecto']=auxCol[2]
stg_consolidado_corte['stg_programacion_proyecto']=auxCol[3]
stg_consolidado_corte['stg_fecha_corte']=pd.to_datetime(auxCol[4], dayfirst=True)

month_mapping={
    'enero':'1',
    'febrero':'2',
    'marzo':'3',
    'abril':'4',
    'mayo':'5',
    'junio':'6',
    'julio':'7',
    'agosto':'8',
    'septiembre':'9',
    'octubre':'10',
    'noviembre':'11',
    'diciembre':'12'
}
auxCol=esp_consolidado_corte["D_START"].str.split(" ", n=5,expand=True)
auxCol['month']=auxCol[1].map(month_mapping)
auxCol['date']=auxCol[0] + '-' + auxCol['month'] + '-' + auxCol[2]
stg_consolidado_corte['stg_fecha_inicial']=pd.to_datetime(auxCol['date'], dayfirst=True)

auxCol=esp_consolidado_corte["D_FINISH"].str.split(" ", n=5,expand=True)
auxCol['month']=auxCol[1].map(month_mapping)
auxCol['date']=auxCol[0] + '-' + auxCol['month'] + '-' + auxCol[2]
stg_consolidado_corte['stg_fecha_fin']=pd.to_datetime(auxCol['date'], dayfirst=True)

stg_consolidado_corte['stg_actividad_predecesora']=esp_consolidado_corte['PREDECESSOR']
stg_consolidado_corte['stg_notas']=esp_consolidado_corte['NOTE']

auxCol=esp_consolidado_corte['DURATION_RESTANTE'].str.split(" ", n=2,expand=True)
auxCol[0] = auxCol[0].str.replace(',','.')
stg_consolidado_corte['stg_duracion_restante_cantidad']=pd.to_numeric(auxCol[0])
stg_consolidado_corte['stg_duracion_restante_unidad']=auxCol[1]

auxCol=esp_consolidado_corte['FIN_LINEA_BASE_EST'].str.split(" ", n=2,expand=True)
stg_consolidado_corte['stg_fin_linea_base_estimado']=pd.to_datetime(auxCol[1], dayfirst=True)

stg_consolidado_corte.reindex(columns=['stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual'])

## Parametrization

data =['CALI','PINTURAS','PINTURAS','PINTURAS',True,'2021-08-27'],['CALI','SANTABARBARA','SANTABARBARA','SANTABARBARA',True,'2021-08-27'],['CALI','PASCUAL','PASCUAL','PASCUAL',True,'2021-08-27'],['BOGOTA','MADNV','ALTOS DE MADELENA','MADELENA',True,'2021-08-27'],['BOGOTA','AMERICAN-PIPE-NOVIS','AMERICAN PIPE','AMERICAN PIPE NOVIS',True,'2021-08-27'],['BOGOTA','AMERICAN-PIPE-VIP','AMERICAN PIPE','AMERICAN PIPE VIP',True,'2021-08-27'],['BOGOTA','AMERICAN-PIPE-VIS','AMERICAN PIPE','AMERICAN PIPE VIS',True,'2021-08-27'],['BOGOTA','CALLE13','CALLE13','CALLE13',True,'2021-08-27'],['BOGOTA','CHANCO','CHANCO','CHANCO',True,'2021-08-27'],['BOGOTA','URDECO','CIPRES DE LA FLORIDA','CIPRES DE LA FLORIDA',True,'2021-08-27'],['BOGOTA','CVM55TV1','CIUDAD VERDE','YERBABUENA',True,'2021-08-27'],['BOGOTA','SMART2','EQUILIBRIUM','EQUILIBRIUM',True,'2021-08-27'],['BOGOTA','BELLAFLORA','BELLAFLORA','BELLAFLORA',True,'2021-08-27'],['BOGOTA','SNJORLCER','HACIENDA ALCALç','CEREZO',True,'2021-08-27'],['BOGOTA','SNJORROB','HACIENDA ALCALç','ROBLE',True,'2021-08-27'],['BOGOTA','SNJORL','HACIENDA ALCALç','SAUCE',True,'2021-08-27'],['BOGOTA','SNJORLT1','HACIENDA ALCALç','SAUCE TO1',True,'2021-08-27'],['BOGOTA','SNJORLT4','HACIENDA ALCALç','SAUCE TO4',True,'2021-08-27'],['BOGOTA','SNJORLT9','HACIENDA ALCALç','SAUCE TO9',True,'2021-08-27'],['BOGOTA','SNJORLSAM','HACIENDA ALCALç','SAMAN',True,'2021-08-27'],['BOGOTA','PDAW2TW','PALO DE AGUA','KATIOS',True,'2021-08-27'],['BOGOTA','PDAAISB','PALO DE AGUA','AISLADAS B',True,'2021-08-27'],['BOGOTA','PDAPAW','PALO DE AGUA','MACARENA',True,'2021-08-27'],['BOGOTA','PDASWTW','PALO DE AGUA','PALO DE AGUA',True,'2021-08-27'],['BOGOTA','PRAFU-MZ10','CIUDADELA FORESTA','MILANO',True,'2021-08-27'],['BOGOTA','PRAFU-MZ7','CIUDADELA FORESTA','IBIZ',True,'2021-08-27'],['BOGOTA','PRAFU-MZ2','CIUDADELA FORESTA','AMAZILIA',True,'2021-08-27'],['BOGOTA','PRAFU-MZ8','CIUDADELA FORESTA','ANDARRIOS',True,'2021-08-27'],['BOGOTA','PRAFU-MZ4','CIUDADELA FORESTA','CDF MZ4',True,'2021-08-27'],['BOGOTA','PRAFU-MZ5','CIUDADELA FORESTA','CDF MZ5',True,'2021-08-27'],['BOGOTA','RECREO','RECREO','RECREO',True,'2021-08-27'],['BOGOTA','SOLEM5','RESERVA DE MADRID','PALERMO',True,'2021-08-27'],['BOGOTA','SOLEM8','RESERVA DE MADRID','PAMPLONA',True,'2021-08-27'],['BOGOTA','SNHILARIO','SAN HILARIO','SAN HILARIO',True,'2021-08-27'],['BOGOTA','SNLUIS','SAN LUIS','SAN LUIS',True,'2021-08-27'],['BOGOTA','VINCULO','EL VINCULO','EL VINCULO',True,'2021-08-27'],['BOGOTA','TECHONOVIS','TECHO','TECHONOVIS',True,'2021-08-27'],['BOGOTA','TECHOVIP','TECHO','TECHOVIP',True,'2021-08-27'],['BOGOTA','TECHOVIS','TECHO','TECHOVIS',True,'2021-08-27'],['BOGOTA','MADPINE','MADRID PI„EROS','MADRID PI„EROS',True,'2021-08-27'],['BOGOTA','TUCANES','TUCANES','TUCANES',True,'2021-08-27'],['CARIBE','SANPABLO','VILLAS DE SAN PABLO','SAN PABLO',True,'2021-08-27'],['CARIBE','ALAM3','ALAMEDA DEL RIO','PELICANO',True,'2021-08-27'],['CARIBE','ALAMNO','ALAMEDA DEL RIO','PARDELA',True,'2021-08-27'],['CARIBE','ALAMVIS','ALAMEDA DEL RIO','PERDIZ',True,'2021-08-27'],['CARIBE','ALAMZ2','ALAMEDA DEL RIO','MZ 2',True,'2021-08-27'],['CARIBE','CDSALEGRIA','CIUDAD DE LOS SUE„OS','ALEGRIA',True,'2021-08-27'],['CARIBE','FELICIDAD','CIUDAD DE LOS SUE„OS','FELICIDAD',True,'2021-08-27'],['CARIBE','CDSMZ4-CAS','CIUDAD DE LOS SUE„OS','ARMONIA CASAS',True,'2021-08-27'],['CARIBE','CDSMZ4-TO','CIUDAD DE LOS SUE„OS','ARMONIA TORRES',True,'2021-08-27'],['CARIBE','CDSMZ5','CIUDAD DE LOS SUE„OS','VENTURA',True,'2021-08-27'],['CARIBE','CDSMZ3','CIUDAD DE LOS SUE„OS','CDS MZ3',True,'2021-08-27'],['CARIBE','HASANT','HACIENDA SAN ANTONIO','CAOBA',True,'2021-08-27'],['CARIBE','LOMA','LA LOMA','LA LOMA',True,'2021-08-27'],['CARIBE','MARBELLA','MARBELLA','MARBELLA',True,'2021-08-27'],['CARIBE','SITUM','IRATI','IRATI',True,'2021-08-27'],['CARIBE','SDM','SERENA DEL MAR','PORTELO',True,'2021-08-27'],['CARIBE','SERMAR','SERENA DEL MAR','PORTANOVA',True,'2021-08-27'],['CARIBE','SDMMZ4','SERENA DEL MAR','CASTELO',True,'2021-08-27'],['CARIBE','SDMMZ6','SERENA DEL MAR','SERENISIMA MZ6',True,'2021-08-27'],['CARIBE','CORAL11','CORAL','CORAL 11',True,'2021-08-27'],['CARIBE','CORAL6','CORAL','CORAL 6',True,'2021-08-27'],['CARIBE','BURECHE','BURECHE','BURECHE',True,'2021-08-27'],['CENTRO','GIRARDOT-MZ3','CIUDAD ESPLENDOR','INDIGO GIRARDOT MZ3',True,'2021-08-27'],['CENTRO','GIRARDOT-VIP','CIUDAD ESPLENDOR','TURQUESA',True,'2021-08-27'],['CENTRO','GIRARDOT-VIS','CIUDAD ESPLENDOR','CELESTE',True,'2021-08-27'],['CENTRO','IBAGUE-VIPMZ14','ECOCIUDADES','CARMESI',True,'2021-08-27'],['CENTRO','IBAGUE-VIS','ECOCIUDADES','GRANATE',True,'2021-08-27'],['CENTRO','IBAGUE-VIP','ECOCIUDADES','CARMIN',True,'2021-08-27'],['CENTRO','IBAGUE-VIPMZ11','ECOCIUDADES','IBAGUE VIP MZ11',True,'2021-08-27'],['CENTRO','IBAGUE-VIPMZ12','ECOCIUDADES','IBAGUE VIP MZ12',True,'2021-08-27'],['CENTRO','VILLETA-VIS','CIUDAD CRISTALES','OPALO',True,'2021-08-27'],['CENTRO','VILLETA-VIP','CIUDAD CRISTALES','ZAFIRO',True,'2021-08-27'],['CENTRO','VILLETA-NOVIS','CIUDAD CRISTALES','AMBAR',True,'2021-08-27'],['CENTRO','CIUDADCRISTALES','CIUDAD CRISTALES','CIUDADCRISTALES',True,'2021-08-27']
tbl_proyectos = pd.DataFrame(data, columns = ['tpr_regional', 'tpr_codigo_proyecto','tpr_macroproyecto','tpr_proyecto','tpr_estado','tpr_fecha_actualizacion'])
tbl_proyectos = tbl_proyectos[tbl_proyectos['tpr_estado']==True]

#Reports: Temporary Area

#Consolidado Proyectos de Construccion

construction_dataset=stg_consolidado_corte.loc[:, ('stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto','stg_area_prodesa', 'stg_ind_tarea', 'stg_nombre_actividad' ,'stg_fecha_inicio_planeada', 'stg_indicador_cantidad', 'stg_duracion_critica_cantidad','stg_ind_buffer','stg_duracion_cantidad', 'stg_fecha_fin', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual', 'stg_fecha_corte')]
construction_dataset['key']=construction_dataset['stg_codigo_proyecto']+'_'+construction_dataset['stg_etapa_proyecto']+'_'+construction_dataset['stg_programacion_proyecto']
construction_dataset=construction_dataset[construction_dataset['stg_area_prodesa']=='CS']

tmp_proyectos_construccion= construction_dataset.loc[:, ('key', 'stg_fecha_corte', 'stg_codigo_proyecto', 'stg_etapa_proyecto', 'stg_programacion_proyecto')]
tmp_proyectos_construccion.rename(columns={'stg_codigo_proyecto': 'tpc_codigo_proyecto', 'stg_etapa_proyecto': 'tpc_etapa', 'stg_programacion_proyecto': 'tpc_programacion'})
tmp_proyectos_construccion=tmp_proyectos_construccion.groupby(by=["key"]).first().reset_index()

auxCol=construction_dataset.loc[:, ('key', 'stg_ind_tarea', 'stg_fecha_inicio_planeada', 'stg_nombre_actividad')]
auxCol=auxCol[auxCol['stg_ind_tarea']=='Sí']
auxCol=auxCol.sort_values(by=['stg_fecha_inicio_planeada'],ascending=True)
auxCol=auxCol.groupby(by=["key"]).first().reset_index()

tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:,('key', 'stg_nombre_actividad')], on='key', how="left",)
tmp_proyectos_construccion=tmp_proyectos_construccion.rename(columns={'stg_nombre_actividad':'tpc_tarea_consume_buffer'})

auxCol=construction_dataset.loc[:, ('key', 'stg_indicador_cantidad')]
auxCol=auxCol.dropna(subset=['stg_indicador_cantidad'], axis=0, inplace=False)
auxCol.sort_values(by=['key',"stg_indicador_cantidad"],ascending=False, inplace=True)
auxCol=auxCol.groupby(by=["key"]).first().reset_index()

auxCol2=construction_dataset.loc[:, ('key', 'stg_duracion_critica_cantidad')]
auxCol2=auxCol2.dropna(subset=['stg_duracion_critica_cantidad'], axis=0, inplace=False)
auxCol2.sort_values(by=['key',"stg_duracion_critica_cantidad"],ascending=False, inplace=True)
auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

auxCol= pd.merge(auxCol,auxCol2, on='key', how="inner",)
auxCol['tpc_avance_cc']=(1-(auxCol['stg_indicador_cantidad'].astype(float)/auxCol['stg_duracion_critica_cantidad'].astype(float)))*100

tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:,('key', 'tpc_avance_cc')], on='key', how="left",)

auxCol=construction_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]

auxCol=auxCol[auxCol['stg_ind_buffer']=='Sí']
auxCol=auxCol.groupby(by=["key"]).first().reset_index()
auxCol=auxCol.loc[:, ('key','stg_duracion_cantidad')]

auxCol2=construction_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_fecha_fin')]
auxCol2=auxCol2[auxCol2['stg_ind_buffer']=='Sí']
auxCol2=auxCol2.groupby(by=["key"]).first().reset_index()

auxCol3=construction_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_project_id', 'stg_fecha_fin_planeada', 'stg_fecha_final_actual')]
auxCol3=auxCol3[auxCol3['stg_ind_buffer']=='No']
auxCol3.sort_values(by=['key',"stg_project_id"],ascending=False, inplace=True)
auxCol3=auxCol3.groupby(by=["key"]).first().reset_index()
auxCol3['fin_proyectada']=np.where(auxCol3['stg_fecha_fin_planeada'].isna(), auxCol3['stg_fecha_final_actual'], auxCol3['stg_fecha_fin_planeada'])

auxCol=pd.merge(auxCol,auxCol2.loc[:, ('key', 'stg_fecha_fin')], on='key', how="left",)
auxCol=pd.merge(auxCol,auxCol3.loc[:, ('key', 'fin_proyectada')], on='key', how="left",)

auxCol['delta_days']=(auxCol['stg_fecha_fin']-auxCol['fin_proyectada']).dt.days
auxCol['tpc_consumo_buffer']=100*(auxCol['stg_duracion_cantidad']-(auxCol['delta_days']-(auxCol['delta_days']/4.5)))/auxCol['stg_duracion_cantidad']

tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('key', 'tpc_consumo_buffer')], on='key', how="left",)

auxCol=construction_dataset.loc[:, ('key', 'stg_duracion_cantidad', 'stg_fecha_fin_planeada')]
auxCol=auxCol[auxCol['stg_duracion_cantidad']==0]
auxCol.sort_values(by=['key',"stg_fecha_fin_planeada"],ascending=False, inplace=True)
auxCol=auxCol.groupby(by=["key"]).first().reset_index()

tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('stg_fecha_fin_planeada','key')], on='key', how="left",)
tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'stg_fecha_fin_planeada': 'tpc_fin_proyectado_optimista'})

auxCol=construction_dataset.loc[:, ('key', 'stg_ind_buffer', 'stg_duracion_cantidad')]

auxCol=auxCol[auxCol['stg_ind_buffer']=='Sí']
auxCol=auxCol.groupby(by=["key"]).first().reset_index()
auxCol=auxCol.loc[:, ('key','stg_duracion_cantidad')]

auxCol=pd.merge(auxCol,tmp_proyectos_construccion.loc[:, ('tpc_avance_cc','tpc_fin_proyectado_optimista','key')], on='key', how="left",)

auxCol['delta']=(auxCol['stg_duracion_cantidad']*(1-(auxCol['tpc_avance_cc']/100)))
auxCol['delta_days'] = auxCol['delta'].apply(np.ceil).apply(lambda x: pd.Timedelta(x, unit='D'))
auxCol['tpc_fin_proyectado_pesimista']=auxCol['tpc_fin_proyectado_optimista']+auxCol['delta_days']

tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('tpc_fin_proyectado_pesimista','key')], on='key', how="left",)

auxCol=construction_dataset.loc[:, ('key', 'stg_fecha_fin')]

auxCol.sort_values(by=['key',"stg_fecha_fin"],ascending=False, inplace=True)
auxCol=auxCol.groupby(by=["key"]).first().reset_index()
tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,auxCol.loc[:, ('stg_fecha_fin','key')], on='key', how="left",)
tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'stg_fecha_fin': 'tpc_fin_programada'})

tmp_proyectos_construccion['tpc_dias_atraso']=(tmp_proyectos_construccion['tpc_fin_programada']-tmp_proyectos_construccion['tpc_fin_proyectado_optimista']).dt.days

#tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
#tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
#tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'tpr_codigo_proyecto' : 'tpc_codigo_proyecto','tpr_regional' : 'tpc_regional','tpr_macroproyecto' : 'tpc_macroproyecto', 'stg_etapa_proyecto' : 'tpc_etapa_proyecto', 'stg_programacion_proyecto' : 'tpc_programacion_proyecto', 'tpr_proyecto' : 'tpc_proyecto', 'stg_fecha_corte' : 'tpc_fecha_corte','tpc_etapa_proyecto':'tpc_etapa','tpc_programacion_proyecto':'tpc_programacion'})

tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'stg_codigo_proyecto': 'tpr_codigo_proyecto'})
tmp_proyectos_construccion=pd.merge(tmp_proyectos_construccion,tbl_proyectos.loc[:, ('tpr_codigo_proyecto','tpr_regional','tpr_macroproyecto','tpr_proyecto')], on='tpr_codigo_proyecto', how="left",)
tmp_proyectos_construccion = tmp_proyectos_construccion.rename(columns={'tpr_codigo_proyecto' : 'tpc_codigo_proyecto','tpr_regional' : 'tpc_regional','tpr_macroproyecto' : 'tpc_macroproyecto', 'stg_etapa_proyecto' : 'tpc_etapa', 'stg_programacion_proyecto' : 'tpc_programacion', 'tpr_proyecto' : 'tpc_proyecto', 'stg_fecha_corte' : 'tpc_fecha_corte'})

tmp_proyectos_construccion['tpc_avance_comparativo_semana']=0
tmp_proyectos_construccion['tpc_consumo_buffer_comparativo']=0
tmp_proyectos_construccion['tpc_ultima_semana']=0
tmp_proyectos_construccion['tpc_ultimo_mes']=0
tmp_proyectos_construccion['tpc_fecha_proceso']=pd.to_datetime("today")
tmp_proyectos_construccion['tpc_lote_proceso']=1

tmp_proyectos_construccion=tmp_proyectos_construccion.reindex(columns=['tpc_regional',
                                                        'tpc_codigo_proyecto',
                                                        'tpc_macroproyecto',
                                                        'tpc_proyecto',
                                                        'tpc_etapa',
                                                        'tpc_programacion',
                                                        'tpc_tarea_consume_buffer',
                                                        'tpc_avance_cc',
                                                        'tpc_avance_comparativo_semana',
                                                        'tpc_consumo_buffer',
                                                        'tpc_consumo_buffer_comparativo',
                                                        'tpc_fin_proyectado_optimista',
                                                        'tpc_fin_proyectado_pesimista',
                                                        'tpc_fin_programada',
                                                        'tpc_dias_atraso',
                                                        'tpc_ultima_semana',
                                                        'tpc_ultimo_mes',
                                                        'tpc_fecha_corte',
                                                        'tpc_fecha_proceso',
                                                        'tpc_lote_proceso'])

#Persisting at BigQuery
#modelo_biaas.tbl_inicio_venta

client = bigquery.Client()
table_id = 'modelo_biaas_python_test.tbl_proyectos_construccion_test2'
# Since string columns use the "object" dtype, pass in a (partial) schema
# to ensure the correct BigQuery data type.
job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("tpc_regional",                    "STRING",   mode="NULLABLE"),
    bigquery.SchemaField("tpc_codigo_proyecto",             "STRING",   mode="NULLABLE"),
    bigquery.SchemaField("tpc_macroproyecto",               "STRING",   mode="NULLABLE"),
    bigquery.SchemaField("tpc_proyecto",                    "STRING",   mode="NULLABLE"),
    bigquery.SchemaField("tpc_etapa",                       "STRING",   mode="NULLABLE"),
    bigquery.SchemaField("tpc_programacion",                "STRING",   mode="NULLABLE"),
    bigquery.SchemaField("tpc_tarea_consume_buffer",        "STRING",   mode="NULLABLE"),
    bigquery.SchemaField("tpc_avance_cc",                   "FLOAT64",  mode="NULLABLE"),
    bigquery.SchemaField("tpc_avance_comparativo_semana",   "INT64",    mode="NULLABLE"),
    bigquery.SchemaField("tpc_consumo_buffer",              "FLOAT64",  mode="NULLABLE"),
    bigquery.SchemaField("tpc_consumo_buffer_comparativo",  "INT64",    mode="NULLABLE"),
    bigquery.SchemaField("tpc_fin_proyectado_optimista",    "DATE",     mode="NULLABLE"),
    bigquery.SchemaField("tpc_fin_proyectado_pesimista",    "DATE",     mode="NULLABLE"),
    bigquery.SchemaField("tpc_fin_programada",              "DATE",     mode="NULLABLE"),
    bigquery.SchemaField("tpc_dias_atraso",                 "INT64",    mode="NULLABLE"),
    bigquery.SchemaField("tpc_ultima_semana",               "FLOAT64",  mode="NULLABLE"),
    bigquery.SchemaField("tpc_ultimo_mes",                  "FLOAT64",  mode="NULLABLE"),
    bigquery.SchemaField("tpc_fecha_corte",                 "DATE",     mode="NULLABLE"),
    bigquery.SchemaField("tpc_fecha_proceso",               "DATETIME",     mode="NULLABLE"),
    bigquery.SchemaField("tpc_lote_proceso",                "INT64",    mode="NULLABLE"),
])


job = client.load_table_from_dataframe(
    tmp_proyectos_construccion, table_id, job_config=job_config
)

# Wait for the load job to complete.
job.result()

print("Job Done!!!")