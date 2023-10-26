import os
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import statistics
import geojson
import seaborn as sns
import geopandas as gpd
import json 
from datetime import datetime, timedelta

st.set_page_config(page_title="Dashboard de ventas", layout="wide")

@st.cache_data
def load_xlsx(x):
    dd = pd.read_excel(x)
    return dd

@st.cache_data
def load_csv(x):
    dd = pd.read_csv(x)
    return dd

@st.cache_data
def convert_to_date(date_str):
    try:
        return pd.to_datetime(datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d'))
    except ValueError:
        try:
            return pd.to_datetime(datetime.strptime(date_str, '%Y-%d-%m %H:%M:%S').strftime('%Y-%m-%d'))
        except ValueError:
            return 'Invalid Date'
        
@st.cache_data
def transform_bom(dd):
    top_level_components = dd['componente'][dd['componente'].str.contains('MP|ME|MV')== False].unique()
    ddx = dd[dd['componente'].isin(top_level_components)]
    ddx.columns = ['A', 'B', 'Qx']
    ddy = dd[~dd['componente'].isin(top_level_components) | dd['subcomponente'].isin(ddx['B'])]
    ddy.columns = ['B', 'C', 'Qy']
    ddz = ddx.merge(ddy, left_on=['B'], right_on=['B'], how='left')
    ddz['Qy'] = np.where(ddz['B'].str.contains('MP'), ddz['Qy']/ddz.groupby(['A','B'], as_index=False)['Qy'].transform('sum'), ddz['Qy'])
    ddz['Qy'] = ddz['Qy'].fillna(1)
    ddz['Qz'] = ddz['Qx']*ddz['Qy']
    ddz['componente'] = np.where(ddz['C'].isnull(), ddz['B'], ddz['C'])
    ddz['cantidad'] = np.where(ddz['componente'].str.contains('MP'),ddz['Qz']/(1-0.05),ddz['Qz']/(1-0.03))
    ddz.drop(['B','Qx', 'C','Qy','Qz'], axis=1, inplace=True)
    ddz.rename(columns={"A": "sku"}, inplace=True)
    return ddz
      
@st.cache_data
def yield_utilidad():
    master = bom.merge(ventas[['codigo', 'marca', 'articulo']].drop_duplicates().dropna(how='any').rename(columns={'codigo':'sku',
                                                                                                                   'articulo':'descripcion'}), left_on=['sku'], right_on=['sku'], how='left')
    master.dropna(how='any', inplace=True)

    if master.shape[0]==0:
        return pd.DataFrame(columns=['sku','variable', 'value'])
    
    costos_year = compras[compras['fecha'].dt.year==max(ventas['year'])]
    costos_year['prop'] = costos_year['cantidad']/costos_year.groupby(['componente'], as_index=False)['cantidad'].transform('sum')
    costos_year['costo'] = costos_year['costo']*costos_year['prop']
    costos_year = costos_year[['componente', 'costo']].groupby(['componente'], as_index=False).agg('sum')
    costos_year['method'] = str("year")

    costos_slice = compras.sort_values(by=['componente','fecha'], ascending=False).drop(['fecha'], axis=1).groupby(['componente'], as_index=False).head(2)
    costos_slice['prop'] = costos_slice['cantidad']/costos_slice.groupby(['componente'], as_index=False)['cantidad'].transform('sum')
    costos_slice['costo'] = costos_slice['costo']*costos_slice['prop']
    costos_slice = costos_slice[['componente', 'costo']].groupby(['componente'], as_index=False).agg('sum')
    costos_slice['method'] = str("slice")

    costos = pd.concat([costos_year, costos_slice])
    costos = costos.pivot(index='componente', columns='method', values='costo').reset_index(drop=False)
    costos['costo'] = np.where(costos['year'].isnull(), costos['slice'], costos['year'])
    costos.drop(['slice','year'], axis=1, inplace=True)
    costos = pd.concat([costos, faltantes[~faltantes['componente'].isin(costos['componente'])]])

    precios = ventas[ventas['year']==max(ventas['year'])]
    precios = precios[precios['usd']>=0]
    precios['prop'] = precios['cantidad']/precios.groupby(['codigo'], as_index=False)['cantidad'].transform('sum')
    precios['precio'] = (precios['usd']/precios['cantidad'])*precios['prop']
    precios = precios.rename(columns={'codigo':'sku'})
    precios.dropna(how='any', inplace=True)
    precios = precios[['sku', 'precio']].groupby(['sku'], as_index=False).agg('sum')

    master = master.merge(costos, left_on=['componente'], right_on=['componente'], how='left')
    master['tipo'] = np.where(master['componente'].str.contains("ME"), "material_empaque", "liquido")
    master['costo'] = master['costo']*master['cantidad']
    master = master[['sku','tipo','costo']].groupby(['sku','tipo'], as_index=False).agg('sum')
    master = master.pivot(index='sku', columns='tipo', values='costo').reset_index()
    master['otros'] = 3.44+0.23
    master = master.merge(precios, left_on=['sku'], right_on=['sku'], how='left')
    master['margen'] = master['precio']-master['material_empaque']-master['liquido']-master['otros']
    master = master[['sku','margen']].dropna(how='any')
    
    return master


@st.cache_data
def yield_cost_breakdown(marca, year):

    master = bom.merge(ventas[['codigo', 'marca', 'articulo']].drop_duplicates().dropna(how='any').rename(columns={'codigo':'sku',
                                                                                                                   'articulo':'descripcion'}), left_on=['sku'], right_on=['sku'], how='left')
    master.dropna(how='any', inplace=True)
    master = master[master['marca']==marca]

    if master.shape[0]==0:
        return pd.DataFrame(columns=['descripcion','variable', 'value'])
    
    costos_year = compras[compras['fecha'].dt.year==year]
    costos_year['prop'] = costos_year['cantidad']/costos_year.groupby(['componente'], as_index=False)['cantidad'].transform('sum')
    costos_year['costo'] = costos_year['costo']*costos_year['prop']
    costos_year = costos_year[['componente', 'costo']].groupby(['componente'], as_index=False).agg('sum')
    costos_year['method'] = str("year")

    costos_slice = compras.sort_values(by=['componente','fecha'], ascending=False).drop(['fecha'], axis=1).groupby(['componente'], as_index=False).head(2)
    costos_slice['prop'] = costos_slice['cantidad']/costos_slice.groupby(['componente'], as_index=False)['cantidad'].transform('sum')
    costos_slice['costo'] = costos_slice['costo']*costos_slice['prop']
    costos_slice = costos_slice[['componente', 'costo']].groupby(['componente'], as_index=False).agg('sum')
    costos_slice['method'] = str("slice")

    costos = pd.concat([costos_year, costos_slice])
    costos = costos.pivot(index='componente', columns='method', values='costo').reset_index(drop=False)
    costos['costo'] = np.where(costos['year'].isnull(), costos['slice'], costos['year'])
    costos.drop(['slice','year'], axis=1, inplace=True)
    costos = pd.concat([costos, faltantes[~faltantes['componente'].isin(costos['componente'])]])

    precios = ventas[ventas['year']==year]
    precios = precios[precios['usd']>=0]
    precios['prop'] = precios['cantidad']/precios.groupby(['articulo'], as_index=False)['cantidad'].transform('sum')
    precios['precio'] = (precios['usd']/precios['cantidad'])*precios['prop']
    precios = precios.rename(columns={'articulo':'descripcion'})
    precios.dropna(how='any', inplace=True)
    precios = precios[['descripcion', 'precio']].groupby(['descripcion'], as_index=False).agg('sum')

    master = master.merge(costos, left_on=['componente'], right_on=['componente'], how='left')
    master['tipo'] = np.where(master['componente'].str.contains("ME"), "material_empaque", "liquido")
    master['costo'] = master['costo']*master['cantidad']
    master = master[['descripcion','tipo','costo']].groupby(['descripcion','tipo'], as_index=False).agg('sum')
    master = master.pivot(index='descripcion', columns='tipo', values='costo').reset_index()
    master['otros'] = 3.44+0.23
    master = master.merge(precios, left_on=['descripcion'], right_on=['descripcion'], how='left')
    master['margen'] = master['precio']-master['material_empaque']-master['liquido']-master['otros']
    master.drop(['precio'], axis=1, inplace=True)
    master.dropna(how='any', inplace=True)
    master = master.melt(id_vars=['descripcion'])
    master['value'] = master['value'].round(2)
    return master


## IMPLEMENTAR ALPHAVANTAGE/TIINGO

monedas = pd.DataFrame(
    {'moneda': ['usd', 'eur', 'rd', 'dkk', 'czk', 'sek', 'jpy', 'cnh', 'chf', 'gbp'], 
     'conversion': [1, 1.05, 0.018, 0.14, 0.043, 0.091, 0.0067, 0.136524, 1.11, 1.21]}
)

ventas = load_xlsx('ventas.xlsx')
ventas['articulo'] = ventas['articulo'].str.lower()
ventas = ventas[ventas['articulo'].str.contains('migraci')== False]
ventas = ventas[ventas['codigo'].str.contains("BPT|PT", na=True)]
ventas['fecha'] = pd.to_datetime(dict(year=ventas.year, month=ventas.month, day=ventas.day))
ventas['trimestre'] =  ventas['fecha'].dt.to_period('Q').dt.strftime('%Y-Q%q')
ventas.rename(columns={"familia": "marca",
                       "monto": "usd"}, inplace=True)
ventas['marca']=ventas['marca'].str.lower()
ventas.loc[ventas["marca"] == "bavaro premiun", "marca"] = "bavaro"
ventas.loc[ventas["marca"].str.contains("quorhum", na=False), "marca"] = "quorhum"
ventas.loc[ventas["marca"].str.contains("cubaney", na=False), "marca"] = "cubaney"
ventas.loc[ventas["marca"].str.contains("presidencial", na=False), "marca"] = "presidente"
ventas.sort_values(by=['trimestre'], ascending=True, inplace=True)
ventas['cliente'] = ventas['cliente'].str.lower()
ventas.loc[ventas["cliente"].str.contains("compagnia", na=False), "cliente"] = "compagnia dei caraibi"
ventas.loc[ventas["cliente"].str.contains("dufry", na=False), "cliente"] = "dufry"
ventas.loc[ventas["pais"].str.contains("Russian", na=False), "pais"] = "Russia"
ventas.loc[ventas["pais"].str.contains("USA", na=False), "pais"] = "United States of America"
ventas.loc[ventas["pais"].str.contains("Schweiz", na=False), "pais"] = "Switzerland"
ventas['pais'] = ventas['pais'].str.lower()
ventas['pais'] = ventas['pais'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
ventas['articulo'] = ventas['articulo'].replace({'s.s.': ''}, regex=True)
ventas['articulo'] = ventas['articulo'].replace({'ron': ''}, regex=True)
ventas['articulo'] = ventas['articulo'].replace({'licor': ''}, regex=True)
ventas['articulo'] = ventas['articulo'].str.replace('[().,-]', '', regex=True)
ventas['articulo'] = ventas['articulo'].replace({'eumac': 'eu'}, regex=True)
ventas['articulo'] = ventas['articulo'].replace({'eumo': 'eu'}, regex=True)
ventas['articulo'] = ventas['articulo'].replace({'uemac': 'eu'}, regex=True)
ventas['articulo'] = ventas['articulo'].replace({'spirit drink': ''}, regex=True)
ventas['articulo'] = ventas['articulo'].replace({'spirt drink': ''}, regex=True)
ventas['articulo'] = ventas['articulo'].replace({'ml': ''}, regex=True)
ventas['articulo'] = ventas['articulo'].replace({'6/700': ''}, regex=True)
ventas['articulo'] = ventas['articulo'].replace(r'\s+', ' ', regex=True)

compras = load_xlsx('compras.xlsx')
compras = compras.iloc[:, 2:9]
compras.columns = ['fecha','proveedor','componente','descripcion','cantidad','moneda', 'costo']
compras['fecha'] = compras['fecha'].astype(str).apply(convert_to_date)
compras['moneda'] = compras['moneda'].str.replace(r'[^\w\s]', '', regex=True).str.lower()
compras = compras.merge(monedas, left_on=['moneda'], right_on=['moneda'], how='left')
compras['costo'] = compras['costo']*compras['conversion']
compras.drop(['moneda', 'conversion' , 'descripcion'], axis=1, inplace=True)
compras = compras[compras['componente'].str.contains("ME|MP")]
compras.sort_values(by=['componente','fecha'], inplace=True)
compras['prop'] = compras['cantidad'] / compras.groupby(['componente','fecha'], as_index=False)['cantidad'].transform('sum')
compras['costo'] = compras['costo']*compras['prop']
compras.drop(['prop','proveedor'], axis=1, inplace=True)
compras = compras.groupby(['componente','fecha'], as_index=False).agg('sum')

faltantes = pd.concat([load_xlsx('costo_me.xlsx'), load_xlsx('costo_mp.xlsx')])
faltantes = faltantes.iloc[:,[1,10,11]]
faltantes.columns=['componente', 'moneda', 'costo']
faltantes = faltantes[~faltantes['componente'].isin(compras['componente'])]
faltantes.dropna(how='any', inplace=True)
faltantes['moneda'] = faltantes['moneda'].str.replace(r'[^\w\s]', '', regex=True).str.lower()
faltantes_extras = load_xlsx('costo_me_faltantes.xlsx').rename(columns={"id_item": "componente",
                                                                                       "precio": "costo"})
faltantes_extras['componente'] = faltantes_extras['componente'].str.upper()
faltantes_extras = faltantes_extras[~faltantes_extras['componente'].isin(faltantes['componente'])]
faltantes = pd.concat([faltantes, faltantes_extras]).reset_index(drop=True)
faltantes = faltantes.merge(monedas, left_on=['moneda'], right_on=['moneda'], how='left')
faltantes['costo'] = faltantes['costo']*faltantes['conversion']
faltantes.drop(['moneda', 'conversion'], axis=1, inplace=True)

bom = load_xlsx('bill_of_materials.xlsx')
bom = bom.iloc[:,[1,3,5]]
bom.columns = ['componente', 'subcomponente', 'cantidad']
bom = transform_bom(bom)

market_share = load_xlsx('market_share.xlsx')
market_share['pais'] = market_share['pais'].str.lower()

cxc = load_xlsx('cuentas_por_cobrar.xlsx')
cxc = cxc.iloc[:,1:8]
cxc.columns = ['factura','fecha', 'codigo_cliente', 'cliente', 'facturado', 'pagado', 'pendiente']
cxc['cliente'] = cxc['cliente'].str.lower()
cxc.loc[cxc["cliente"].str.contains("compagnia", na=False), "cliente"] = "compagnia dei caraibi"
cxc.loc[cxc["cliente"].str.contains("dufry", na=False), "cliente"] = "dufry"
cxc['fecha']= cxc['fecha'].astype(str).apply(convert_to_date)

condiciones = load_xlsx('condiciones.xlsx')
condiciones = condiciones.iloc[:,[1,4]]
condiciones.columns = ['codigo_cliente', 'dias_credito']
condiciones['dias_credito'] = condiciones['dias_credito'].str.extract('(\d+)').fillna(0)
condiciones['dias_credito'] = pd.to_numeric(condiciones['dias_credito'])

cxc = cxc.merge(condiciones, left_on=['codigo_cliente'], right_on=['codigo_cliente'], how='left')

resumen_cxc = cxc.drop(['factura','codigo_cliente'], axis=1)
resumen_cxc = resumen_cxc[resumen_cxc['pendiente']!=0]
resumen_cxc = resumen_cxc.groupby(['fecha','cliente','dias_credito'], as_index=False).agg('sum')
resumen_cxc['dias_vencidos'] = (datetime.today() - resumen_cxc['fecha']).dt.days -resumen_cxc['dias_credito']
resumen_cxc['vigencia'] = pd.cut(resumen_cxc['dias_vencidos'], 
                                                     [-1000,-100, -50, 0, 50, 100, 1000],
                                                     labels=['[-inf, -100]', '[-100, -50]', '[-50, 0]', '[0, 50]', '[50, 100]', '[100, inf]'])
resumen_cxc['pendiente'] = resumen_cxc['pendiente']/56
resumen_cxc = resumen_cxc.round(0)

st.subheader('Dashboard')

tab1, tab2 = st.tabs(["Resumen", "Marcas"])

with tab1:
    col1, col2= st.columns([1,3])
    with col1:
        metrica_ventas_ytd = ventas['usd'][ventas['year']>=max(ventas['year'])].sum()
        metrica_ventas_ytd_delta = (metrica_ventas_ytd/ventas['usd'][ventas['year']==(max(ventas['year'])-1)].sum())-1
        st.metric(label="Ingresos (YTD)", value='${:,.0f}'.format(metrica_ventas_ytd), delta='{:.0%}'.format(metrica_ventas_ytd_delta))
        st.divider()
        df_utilidad = ventas[['codigo', 'fecha', 'usd', 'cantidad']]
        df_utilidad = df_utilidad[(df_utilidad['fecha'].dt.year==max(df_utilidad['fecha'].dt.year))]
        df_utilidad = df_utilidad.merge(yield_utilidad(), left_on=['codigo'], right_on=['sku'], how='left')
        metrica_utilidad = sum(df_utilidad.fillna(0)['cantidad']*df_utilidad.fillna(0)['margen'])+sum(df_utilidad['usd'][df_utilidad['usd']<0])
        st.metric(label="Utilidad (YTD)", value='${:,.0f}'.format(metrica_utilidad), delta=None)
        st.divider()
        metrica_ap_ytd = ventas['usd'][(ventas['year']>=max(ventas['year'])) & (ventas['usd']<0)  & (ventas['cantidad']==0)].sum()*(-1)
        metrica_ap_ytd_delta = (metrica_ap_ytd/(ventas['usd'][(ventas['year']==(max(ventas['year'])-1)) & (ventas['usd']<0) & (ventas['cantidad']==0)].sum()*(-1)))-1
        st.metric(label="A&P (YTD)", value='${:,.0f}'.format(metrica_ap_ytd), delta='{:.0%}'.format(metrica_ap_ytd_delta))
    with col2:
        df_resumen = ventas[['year','usd']]
        df_resumen['clase'] = np.where(df_resumen['usd']<0, "a&p", "ingreso")
        df_resumen = df_resumen.groupby(['year','clase'], as_index=False).agg('sum')
        df_resumen['usd'] = df_resumen['usd'].abs()
        df_resumen['porcentaje'] = (df_resumen['usd'] / df_resumen.groupby('year')['usd'].transform('sum')) * 100
        fig_resumen = px.bar(df_resumen,
                      y='usd',
                      x='year',
                      text=[f"{value:.1f}%" for value in df_resumen['porcentaje']],
                      color='clase',
                      height=550)
        fig_resumen.update_layout(yaxis_title="(USD)")
        fig_resumen.update_layout(xaxis_title=None)
        fig_resumen.update_xaxes(type='category')
        st.plotly_chart(fig_resumen, theme="streamlit", use_container_width=True)
    
    col3, col4, col5 = st.columns(3)
    with col3:
        fig_resumen_marca = px.pie(ventas[['marca','usd']][ventas['year']>=max(ventas['year'])].groupby(['marca'], as_index=False).agg('sum'),
                                  values='usd',
                                  names='marca',
                                  title='Ventas x marca',
                                  hole=0.3)
        st.plotly_chart(fig_resumen_marca, theme="streamlit", use_container_width=True)        

    with col4:
        fig_resumen_pais = px.pie(ventas[['pais','usd']][ventas['year']>=max(ventas['year'])].groupby(['pais'], as_index=False).agg('sum'),
                                  values='usd',
                                  names='pais',
                                  title='Ventas x pais',
                                  hole=0.3)
        st.plotly_chart(fig_resumen_pais, theme="streamlit", use_container_width=True)


    with col5:
        fig_resumen_cliente = px.pie(ventas[['cliente','usd']][ventas['year']>=max(ventas['year'])].groupby(['cliente'], as_index=False).agg('sum'),
                                  values='usd',
                                  names='cliente',
                                  title='Ventas x cliente',
                                  hole=0.3)
        st.plotly_chart(fig_resumen_cliente, theme="streamlit", use_container_width=True)

    col6, col7 = st.columns(2)

    with col6:
        df_resumen_cxc_vigencia = resumen_cxc[['vigencia', 'pendiente']].groupby(['vigencia'], as_index=False).agg('sum').rename(columns={'pendiente':'usd'})
        fig_resumen_cxc_vigencia = px.bar(df_resumen_cxc_vigencia,
                                          x = 'usd',
                                          y='vigencia',
                                          title='Cuentas x cobrar x vigencia',
                                          height=550
                                          )
        fig_resumen_cxc_vigencia.update_layout(xaxis_title="(USD)")
        fig_resumen_cxc_vigencia.update_layout(yaxis_title="(dias vencidos)")
        st.plotly_chart(fig_resumen_cxc_vigencia, use_container_width=True)

    with col7:
        df_resumen_cxc_top = resumen_cxc.copy()
        df_resumen_cxc_top['vigencia'] = np.where(df_resumen_cxc_top['dias_vencidos']>0, str("vencido"), str("no vencido"))
        df_resumen_cxc_top = df_resumen_cxc_top[['cliente','vigencia', 'pendiente']].groupby(['cliente', 'vigencia'], as_index=False).agg('sum').rename(columns={'pendiente':'usd'})
        fig_resumen_cxc_top = px.bar(df_resumen_cxc_top,
                                     x='usd',
                                     y='cliente',
                                     color='vigencia',
                                     title='Cuentas x cobrar x cliente',
                                     height=550)
        fig_resumen_cxc_top.update_layout(xaxis_title="(USD)")
        fig_resumen_cxc_top.update_layout(yaxis_title=None)
        fig_resumen_cxc_top.update_layout(yaxis={'categoryorder':'total ascending'}) 
        st.plotly_chart(fig_resumen_cxc_top, use_container_width=True)

with tab2:
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        metrica_ventas_presidente = ventas['usd'][(ventas['year']>=max(ventas['year']))&(ventas['marca']=='presidente')].sum()
        metrica_ventas_presidente_delta = (metrica_ventas_presidente/ventas['usd'][(ventas['year']==(max(ventas['year'])-1))&(ventas['marca']=='presidente')&(ventas['fecha']<=(max(ventas['fecha'])-timedelta(days=360)))].sum())-1
        st.metric(label="Presidente (YTD)", value='${:,.0f}'.format(round(metrica_ventas_presidente,-3)), delta='{:.0%}'.format(metrica_ventas_presidente_delta))
    
    with col2:
        metrica_ventas_quorhum = ventas['usd'][(ventas['year']>=max(ventas['year']))&(ventas['marca']=='quorhum')].sum()
        metrica_ventas_quorhum_delta = (metrica_ventas_quorhum/ventas['usd'][(ventas['year']==(max(ventas['year'])-1))&(ventas['marca']=='quorhum')&(ventas['fecha']<=(max(ventas['fecha'])-timedelta(days=360)))].sum())-1
        st.metric(label="Quorhum (YTD)", value='${:,.0f}'.format(round(metrica_ventas_quorhum,-3)), delta='{:.0%}'.format(metrica_ventas_quorhum_delta))
    
    with col3:
        metrica_ventas_opthimus = ventas['usd'][(ventas['year']>=max(ventas['year']))&(ventas['marca']=='opthimus')].sum()
        metrica_ventas_opthimus_delta = (metrica_ventas_opthimus/ventas['usd'][(ventas['year']==(max(ventas['year'])-1))&(ventas['marca']=='opthimus')&(ventas['fecha']<=(max(ventas['fecha'])-timedelta(days=360)))].sum())-1
        st.metric(label="Opthimus (YTD)", value='${:,.0f}'.format(round(metrica_ventas_opthimus,-3)), delta='{:.0%}'.format(metrica_ventas_opthimus_delta))
    
    with col4:
        metrica_ventas_puntacana = ventas['usd'][(ventas['year']>=max(ventas['year']))&(ventas['marca']=='punta cana club')].sum()
        metrica_ventas_puntacana_delta = (metrica_ventas_puntacana/ventas['usd'][(ventas['year']==(max(ventas['year'])-1))&(ventas['marca']=='punta cana club')&(ventas['fecha']<=(max(ventas['fecha'])-timedelta(days=360)))].sum())-1
        st.metric(label="Punta Cana (YTD)", value='${:,.0f}'.format(round(metrica_ventas_puntacana,-3)), delta='{:.0%}'.format(metrica_ventas_puntacana_delta))
    
    with col5:
        metrica_ventas_cubaney = ventas['usd'][(ventas['year']>=max(ventas['year']))&(ventas['marca']=='cubaney')].sum()
        metrica_ventas_cubaney_delta = (metrica_ventas_cubaney/ventas['usd'][(ventas['year']==(max(ventas['year'])-1))&(ventas['marca']=='cubaney')&(ventas['fecha']<=(max(ventas['fecha'])-timedelta(days=360)))].sum())-1
        st.metric(label="Cubaney (YTD)", value='${:,.0f}'.format(round(metrica_ventas_cubaney,-3)), delta='{:.0%}'.format(metrica_ventas_cubaney_delta))


    st.title("")

    year_list = ventas.year.sort_values().unique()
    year_index=np.where(year_list==max(ventas.year))[0][0]

    col1, col2= st.columns([1,3])
    with col1:
        var_year = st.selectbox(
             'Año:',
            list(ventas.year.sort_values().unique()),
            index=int(year_index))
        
        var_marca = st.selectbox(
             'Marca:',
            list(ventas.marca.sort_values().unique()))
        
        var_metrica = st.selectbox(
             'Métrica:',
            ['ventas', 'market_share'])
        

    with col2:

        country_codes = load_csv('all.csv')
        country_codes = country_codes[['pais', 'alpha-3']]
        country_codes['pais'] = country_codes['pais'].str.lower()

        df_marca_mapa = ventas.copy()
        df_marca_mapa.dropna(how='any', inplace=True)
        df_marca_mapa = df_marca_mapa[(df_marca_mapa['marca']==var_marca) & (df_marca_mapa['year']==var_year)]
        df_marca_mapa['ventas'] = df_marca_mapa['usd'][df_marca_mapa['usd']>=0]
        df_marca_mapa = df_marca_mapa[['marca', 'pais', 'ventas','cantidad']].groupby(['marca', 'pais'],as_index=False).agg('sum')
        df_marca_mapa = df_marca_mapa.merge(country_codes, left_on=['pais'], right_on=['pais'], how='left')
        df_marca_mapa = df_marca_mapa.merge(market_share, left_on=['pais'], right_on=['pais'], how='left')
        df_marca_mapa['market_share'] = ((df_marca_mapa['cantidad']*4.2*0.4)/((df_marca_mapa['aa_per_capita']*df_marca_mapa['population'])*0.03))*100
        df_marca_mapa['market_share'] = df_marca_mapa['market_share'].round(3)

        fig_marca_mapa = px.choropleth(df_marca_mapa, locations="alpha-3", color=var_metrica,
                                       color_continuous_scale=px.colors.sequential.PuBu,
                                       hover_name="pais",
                                       width=1010)
        
        fig_marca_mapa.update_geos(projection_type="natural earth")
        fig_marca_mapa.update_geos(lataxis_showgrid=True, lonaxis_showgrid=True)
        fig_marca_mapa.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        fig_marca_mapa.update_layout(mapbox_style="carto-positron", mapbox_zoom=2, mapbox_center={"lat": 51, "lon": 10})    
        fig_marca_mapa.update_layout(title_text="European Sales Choropleth Map", geo=dict(showcoastlines=True))    

        st.plotly_chart(fig_marca_mapa)   

    
    col1, col2= st.columns(2)
    with col1:
        fig_marca_sku_pie = px.pie(ventas[['articulo','usd']][(ventas['year']==var_year)&(ventas['marca']==var_marca)].groupby(['articulo'], as_index=False).agg('sum'),
                                  values='usd',
                                  names='articulo',
                                  title='Ventas x SKU',
                                  hole=0.3)
        st.plotly_chart(fig_marca_sku_pie, theme="streamlit", use_container_width=True)
    with col2:
        df_marca_sku_bar = yield_cost_breakdown(var_marca, var_year)
        fig_marca_sku_bar = px.bar(df_marca_sku_bar, 
                                   y= 'descripcion',
                                   x= 'value',
                                   color='variable',
                                   title='Costos x SKU')
        fig_marca_sku_bar.update_layout(xaxis_title="(USD)")
        fig_marca_sku_bar.update_layout(yaxis_title=None)
        fig_marca_sku_bar.update_layout(yaxis={'categoryorder':'total ascending'})         
        st.plotly_chart(fig_marca_sku_bar, theme="streamlit", use_container_width=True)