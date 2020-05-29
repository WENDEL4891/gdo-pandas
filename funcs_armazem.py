import pandas as pd
import numpy as np
import os
import datetime

datetime_now = datetime.datetime.now()
dia = datetime_now.day
mes = datetime_now.month


def get_metas(mes=mes):
    '''Retorna um dicionário, com as metas '''
    dias_por_mes = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
    metas = pd.read_sql_table('tbl_metas', 'sqlite:///gdo.db')
    
    metas_by_cia_indicador_mes = metas[
        metas['MES'] <= mes
    ].groupby(['CIA','INDICADOR','MES']).sum()['META'].unstack('MES')
    
    metas = {}
    metas_somar = ['tcv', 'thc', 'ic', 'ols', 'rqv_ee', 'rqv_efet', 'tqf']
    metas_nao_somar = ['ddu_concluido', 'ddu_sucesso', 'iaf', 'tri']    
    for meta in metas_somar:
        metas[meta] = metas_by_cia_indicador_mes.xs(meta.upper(), level=1).copy()
        metas[meta].iloc[:,-1] = metas[meta].iloc[:,-1] / dias_por_mes[mes] * (dia - 1)
        metas[meta].loc[:,'ACUM'] = metas[meta].sum(1)
        metas[meta].loc['23 BPM'] = metas[meta].sum()
        metas[meta] = metas[meta].round(2)
    for meta in metas_nao_somar:
        metas[meta] = metas_by_cia_indicador_mes.xs(meta.upper(), level=1).copy()
        metas[meta].loc[:,'ACUM'] = metas[meta].iloc[:,0]
        metas[meta].loc['23 BPM'] = metas[meta].iloc[1]
        metas[meta] = metas[meta].round(2)
    return metas
    
metas = get_metas(mes=mes)

def read_files(path_file, sheet_name):
    df = pd.read_excel(path_file, sheet_name=sheet_name)
    df = df[df['Mês Numérico Fato'] <= mes]
    return df

def get_bd_dados():
    bd_dados = dict()

    indicadores = (
        ('tcv', 'BD', 'Qtde Ocorrências'),
        ('thc', 'HC VITIMAS', 'Qtde Envolvidos'),
        ('tqf', 'BD', 'Qtde Ocorrências'),
        ('iaf_armas', 'bd armas', 'Qtde  Armas de Fogo'),
        ('iaf_simulacros', 'bd simulacros', 'Qtde Materiais'),
        ('iaf_crimes', 'bd crimes af', 'Qtde Ocorrências'),
        ('tri_presos', 'BD_PRISOES', 'Qtde Envolvidos'),
        ('tri_crimes', 'BD_CV', 'Qtde Ocorrências')
    )

    file_list = os.listdir('files/Armazem/2020/')
    path_files = 'files/Armazem/2020/'

    for indicador in indicadores:
        bd_dados[indicador[0]] = read_files(path_files+list(filter(lambda file: indicador[0][0:3].upper() in file, file_list))[0], sheet_name=indicador[1])
        bd_dados[indicador[0]].rename(columns={'Mês Numérico Fato':'MES'}, inplace=True)
    return bd_dados


def get_tables():
    indicadores = ['tcv', 'thc', 'tqf', 'iaf', 'tri']
    tables = {
        indicador:dict() for indicador in indicadores
    }
    for key, value in tables.items():
        value['mes'] = dict()
        value['acum'] = dict()
        value['dados'] = dict()
    return tables

def set_tables_data(tables=get_tables()):
    cias = ('53 CIA', '139 CIA', '142 CIA', '51 CIA')
    itens_indicadores = (
            ('tcv', 'BD', 'Qtde Ocorrências'),
            ('thc', 'HC VITIMAS', 'Qtde Envolvidos'),
            ('tqf', 'BD', 'Qtde Ocorrências'),
            ('iaf_armas', 'bd armas', 'Qtde  Armas de Fogo'),
            ('iaf_simulacros', 'bd simulacros', 'Qtde Materiais'),
            ('iaf_crimes', 'bd crimes af', 'Qtde Ocorrências'),
            ('tri_presos', 'BD_PRISOES', 'Qtde Envolvidos'),
            ('tri_crimes', 'BD_CV', 'Qtde Ocorrências')

        )

    series_base = pd.Series([0, 0, 0, 0], index = cias)
    series_base_com_cia_invalida = pd.Series([0, 0, 0, 0, 0], index = cias + ('CIA INDEFINIDA',))
    cias = ('')
    
    for item in itens_indicadores:
        indicador = item[0][0:3]
        col_cia = list(filter(lambda cols: '23_CIA' in cols, bd_dados[item[0]]))[0]

        dados_table = tables[indicador]['dados']    
        dados_indicador = bd_dados[item[0]]
        
        
        dados_table['{}_{}'.format(item[0], 'mes')] = dados_indicador[dados_indicador['MES'] == mes].groupby(col_cia).sum()[item[2]]
        dados_table[item[0]+'_acum'] = dados_indicador[dados_indicador['MES'] <= mes].groupby(col_cia).sum()[item[2]]
        
        for periodo in ('mes', 'acum'):

            tem_cia_invalida = list(filter(lambda cia: cia in cias, dados_table['{}_{}'.format(item[0], periodo)].index))

            dados_table['{}_{}'.format(item[0], periodo)] = pd.concat([ 
                series_base if not tem_cia_invalida else series_base_com_cia_invalida,
                dados_table['{}_{}'.format(item[0], periodo)]
            ], axis=1, sort=False).fillna(0).astype('uint16').iloc[:,1]
            del tem_cia_invalida

            dados_table['{}_{}'.format(item[0], periodo)].loc['23 BPM'] = dados_table['{}_{}'.format(item[0], periodo)].sum()

       
    dados_armas = tables['iaf']['dados']
    for periodo in ('mes', 'acum'):
        dados_armas['aux_'+periodo] = pd.concat([
            dados_armas['iaf_armas_'+periodo],
            dados_armas['iaf_simulacros_'+periodo]
        ], axis=1, sort=False).fillna(0).astype('int16')
        
        dados_armas['armas_total_'+periodo] = (
            dados_armas['aux_'+periodo]['Qtde  Armas de Fogo'] + dados_armas['aux_'+periodo]['Qtde Materiais']
        )
        dados_armas['armas_total_'+periodo].name = 'AFA'
#         tables['iaf']['dados']['armas_total_'+periodo] = pd.Series(
#             tables['iaf']['dados']['iaf_armas_'+periodo].values + tables['iaf']['dados']['iaf_simulacros_'+periodo].values,
#         name='AFA', index = (
#             cias + ['23 BPM'] if 
#         ))
    
    return tables



def get_populacao():
    pop = pd.read_sql_table('tbl_populacoes', 'sqlite:///gdo.db')    
    pop.set_index('CIA', inplace=True)
    pop.index.name = 'CIA'    
    return pop
    

def get_farol(valor, polaridade):
    feliz = '&#128578'
    normal = '&#x1f610'
    triste = '&#128577'
    if polaridade == 'positiva':
        if valor >= 100:
            return feliz
        elif valor >= 70:
            return normal
        else:
            return triste
    if polaridade == 'negativa':
        if valor <= 0:
            return feliz
        elif valor < 10:
            return normal
        else:
            return triste

    
def set_tables_indicadores_polaridade_negativa(tables_dict):    
    for indicador in ['tcv','thc','tqf']:
        for periodo in ['mes', 'acum']:
                                   
            
            tables_dict[indicador][periodo] = pd.concat(
                [
                    get_populacao(),
                    tables_dict[indicador]['dados'][indicador+'_'+periodo]
                ], axis=1, sort=False
            )

            tables_dict[indicador][periodo].columns = ['POPULACAO', 'ABS']

            tables_dict[indicador][periodo][indicador.upper()] = (
                tables_dict[indicador][periodo]['ABS'].values / tables_dict[indicador][periodo]['POPULACAO'].values * 100000
            ).round(2)    

            tables_dict[indicador][periodo]['META ABS'] = metas[indicador][mes if periodo == 'mes' else 'ACUM'] 

            tables_dict[indicador][periodo]['META '+indicador.upper()] = (
                tables_dict[indicador][periodo]['META ABS'].values / tables_dict[indicador][periodo]['POPULACAO'] * 100000
            ).round(2)

            tables_dict[indicador][periodo]['VAR %'] = (
                ( tables_dict[indicador][periodo][indicador.upper()].values - tables_dict[indicador][periodo]['META '+indicador.upper()] )
                / ( tables_dict[indicador][periodo]['META '+indicador.upper()] ) * 100
            ).round(2)                        
            tables_dict[indicador][periodo]['PLP'] = '10,00 %'
            tables_dict[indicador][periodo]['FAROL'] = tables_dict[indicador][periodo]['VAR %'].apply(
                lambda var: get_farol(var, 'negativa')
            )            
            tables_dict[indicador][periodo]['VAR %'] = tables[indicador][periodo]['VAR %'].apply(lambda var: str(var)+' %')
                        
            tem_cia_invalida = list(filter(
                lambda cia: cia not in ('51 CIA', '53 CIA', '139 CIA', '142 CIA', '23 BPM'),
                tables_dict[indicador][periodo].index
            ))
            
            if tem_cia_invalida:
                tables_dict[indicador][periodo].loc['CIA INDEFINIDA'] = [
                    '-',
                    tables_dict[indicador][periodo].loc[ tem_cia_invalida[0], 'ABS' ],
                    '-', '-', '-', '-', '-', '-'
                ]
                tables_dict[indicador][periodo] = tables_dict[indicador][periodo].reindex([
                    '53 CIA', '139 CIA', '142 CIA', '51 CIA', 'CIA INDEFINIDA', '23 BPM'
                ])
            del tem_cia_invalida
            
            tables_dict[indicador][periodo].columns = pd.MultiIndex.from_product([
                [indicador.upper()+' - '+periodo.upper()], tables_dict[indicador][periodo].columns
            ])
            



def set_tables_iaf(tables_dict):
    for periodo in ('mes', 'acum'):
        tables_dict['iaf'][periodo] = pd.concat([
            pop,
            tables_dict['iaf']['dados']['armas_total_'+periodo],
            tables_dict['iaf']['dados']['iaf_crimes_'+periodo]
        ], axis=1, sort=False).fillna(0).astype('int')\
        .rename(columns={'Qtde Ocorrências': 'TCAF'})        

        tables_dict['iaf'][periodo]['TAXA'] = (
            tables_dict['iaf'][periodo]['AFA'] / ( tables_dict['iaf'][periodo]['TCAF'] + tables_dict['iaf'][periodo]['AFA'] )
            * 100
        ).round(2)    
        tables_dict['iaf'][periodo]['META'] = metas['iaf'][mes if periodo == 'mes' else 'ACUM']
        tables_dict['iaf'][periodo]['VAR %'] = (
            tables_dict['iaf'][periodo]['TAXA'] / tables_dict['iaf'][periodo]['META'] * 100        
        ).round(2)
        tables_dict['iaf'][periodo]['FAROL'] = tables_dict['iaf'][periodo]['VAR %'].apply(
            lambda val: get_farol(val, 'positiva')
        )
        tables_dict['iaf'][periodo]['VAR %'] = tables_dict['iaf'][periodo]['VAR %'].apply(lambda var: str(var) + ' %')
                
        tem_cia_invalida = list(filter(
                lambda cia: cia not in ('51 CIA', '53 CIA', '139 CIA', '142 CIA', '23 BPM'),
                tables_dict['iaf'][periodo].index
            ))
            
        if tem_cia_invalida:
            tables_dict['iaf'][periodo].loc['CIA INDEFINIDA'] = [
                '-',
                tables_dict['iaf'][periodo].loc[ tem_cia_invalida, 'AFA' ].sum(),
                tables_dict['iaf'][periodo].loc[ tem_cia_invalida, 'TCAF' ].sum(),
                '-', '-', '-', '-'
            ]
            tables_dict['iaf'][periodo] = tables_dict['iaf'][periodo].reindex([
                '53 CIA', '139 CIA', '142 CIA', '51 CIA', 'CIA INDEFINIDA', '23 BPM'
            ])
        del tem_cia_invalida
        
        tables_dict['iaf'][periodo].columns = pd.MultiIndex.from_product([
            ['IAF - '+periodo.upper()], tables_dict['iaf'][periodo].columns
        ])
        
        
def set_tables_tri(tables_dict):
    for periodo in ('mes', 'acum'):
        tables_dict['tri'][periodo] = pd.concat([pop, tables_dict['tri']['dados']['tri_presos_'+periodo]], axis=1, sort=False)
        tables_dict['tri'][periodo].rename(columns={'Qtde Envolvidos': 'NPAA'}, inplace=True)
        tables_dict['tri'][periodo]['TRCV'] = tables_dict['tri']['dados']['tri_crimes_'+periodo]
        tables_dict['tri'][periodo]['TAXA'] = (
            tables_dict['tri'][periodo]['NPAA'].values / tables_dict['tri'][periodo]['TRCV']
            * 100
        ).round(2)    
        tables_dict['tri'][periodo]['META'] = metas['tri'][mes if periodo == 'mes' else 'ACUM']
        tables_dict['tri'][periodo]['VAR %'] = (
            tables_dict['tri'][periodo]['TAXA'] / tables_dict['tri'][periodo]['META'] * 100        
        ).round(2)
        tables_dict['tri'][periodo]['FAROL'] = tables_dict['tri'][periodo]['VAR %'].apply(
            lambda val: get_farol(val, 'positiva')
        )
        tables_dict['tri'][periodo]['VAR %'] = tables_dict['tri'][periodo]['VAR %'].apply(lambda var: str(var) + ' %')        
        tables_dict['tri'][periodo].columns = pd.MultiIndex.from_product([
            ['TRI - '+periodo.upper()], tables_dict['tri'][periodo].columns
        ])
        

def farol_colors(val):
    feliz = '&#128578'
    normal = '&#x1f610'
    triste = '&#128577'
    if val == feliz:
        color = 'green'
    elif val == normal:
        color = 'yellow'
    elif val == triste:
        color = 'red'
    else:
        color = 'orange'
    return (
        '''                
        background-color: {};
        font-size: 17px;
        text-align: center;        
        '''.format(color)
    )
    

def multiple_dfs(df_list, sheets, file_name, spaces):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    row = 0
    for dataframe in df_list:
        dataframe.to_excel(writer,sheet_name=sheets,startrow=row , startcol=0)   
        row = row + len(dataframe.index) + spaces + 1
    writer.save()

    
    
bd_dados = get_bd_dados()
tables = get_tables()
tables = set_tables_data()
pop = get_populacao()
metas = get_metas()
set_tables_indicadores_polaridade_negativa(tables)
set_tables_iaf(tables)
set_tables_tri(tables)

tables_html = dict()
for indicador in ['tcv', 'thc', 'tqf', 'iaf', 'tri']:
    for periodo in ['mes', 'acum']:
        table = tables[indicador][periodo].style\
        .applymap(lambda val: 'text-align: center')\
        .applymap(farol_colors, subset=[(indicador.upper()+' - '+periodo.upper(), 'FAROL')])
        display(table)
        tables_html[indicador+' - '+periodo] = table