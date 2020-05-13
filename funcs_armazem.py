import pandas as pd
import numpy as np
import os

file_list = os.listdir('files/Armazem/2020/'+str(mes))

def get_metas(mes):
    '''Retorna um dicionário, com as metas '''
    metas = pd.read_sql_table('tbl_metas', 'sqlite:///gdo.db')
    
    metas_by_cia_indicador_mes = metas[
        metas['MES'] <= mes
    ].groupby(['CIA','INDICADOR','MES']).sum()['META'].unstack('MES')
    
    metas = {}
    metas_somar = ['tcv', 'thc', 'ic', 'ols', 'rqv_ee', 'rqv_efet', 'tqf']
    metas_nao_somar = ['ddu_concluido', 'ddu_sucesso', 'iaf', 'tri']    
    for meta in metas_somar:
        metas[meta] = metas_by_cia_indicador_mes.xs(meta.upper(), level=1).copy()
        metas[meta].loc[:,'ACUM'] = metas[meta].sum(1)
        metas[meta].loc['TOTAL'] = metas[meta].sum()
        metas[meta] = metas[meta].round(2)
        metas[meta].columns = pd.MultiIndex.from_product([['meta_'+meta+'_abs'], metas[meta].columns])
    for meta in metas_nao_somar:
        metas[meta] = metas_by_cia_indicador_mes.xs(meta.upper(), level=1).copy()
        metas[meta].loc[:,'ACUM'] = metas[meta].iloc[:,0]
        metas[meta].loc['TOTAL'] = metas[meta].iloc[1]
        metas[meta].columns = pd.MultiIndex.from_product([['metas_'+meta], metas[meta].columns])
        metas[meta] = metas[meta].round(2)
    return metas
    
metas = get_metas(mes=mes)

def read_files(path_file, sheet_name):
    df = pd.read_excel(path_file, sheet_name=sheet_name)
    df = df[df['Mês Numérico Fato'] <= mes]
    return df

tcv = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'TCV' in file, file_list))[0], sheet_name='BD')
thc = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'THC' in file, file_list))[0], sheet_name='HC VITIMAS')
tqf = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'TQF' in file, file_list))[0], sheet_name='BD')
tqf['Qtde Ocorrências'] = 1
iaf_armas = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'IAF' in file, file_list))[0], sheet_name='bd armas')
iaf_simulacros = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'IAF' in file, file_list))[0], sheet_name='bd simulacros')
iaf_crimes = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'IAF' in file, file_list))[0], sheet_name='bd crimes af')
tri_presos = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'TRI' in file, file_list))[0], sheet_name='BD_PRISOES')
tri_crimes = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'TRI' in file, file_list))[0], sheet_name='BD_CV')

for df in [tcv,thc,tqf,iaf_armas,iaf_simulacros,iaf_crimes,tri_presos,tri_crimes]:
    df.rename(columns={
        'Ano Fato':'ANO',
        'Mês Numérico Fato':'MES'
    }, inplace=True)


def get_data():
    '''Return um dicionário, com todas as tabelas de dados do Armazém de Dados, com os seguintes índices:
    tcv, thc, tqf, iaf_armas, iaf_simulacros, iaf_crimes, tri_presos, tri_crimes
    '''
    by_cia_mes_dict = dict()
    tables = [
        (tcv,'Qtde Ocorrências','tcv'),
        (thc,'Qtde Envolvidos','thc'),
        (tqf,'Qtde Ocorrências','tqf'),
        (iaf_armas,'Qtde  Armas de Fogo','iaf_armas'),
        (iaf_simulacros,'Qtde Materiais','iaf_simulacros'),
        (iaf_crimes,'Qtde Ocorrências','iaf_crimes'),
        (tri_presos,'Qtde Envolvidos','tri_presos'),
        (tri_crimes,'Qtde Ocorrências','tri_crimes')
    ] 
    for table in tables:    
        cia_col = list(filter(lambda col: '23_CIA' in col, table[0].columns))[0]    
        by_cia_mes_dict[table[2]] = table[0].groupby(['MES', cia_col]).sum()[table[1]].unstack('MES')
        by_cia_mes_dict[table[2]] = by_cia_mes_dict[table[2]].fillna(0).astype('int16')
        by_cia_mes_dict[table[2]]['ACUM'] = by_cia_mes_dict[table[2]].sum(1)
        by_cia_mes_dict[table[2]].loc['TOTAL'] = by_cia_mes_dict[table[2]].sum()
        by_cia_mes_dict[table[2]].index.name = 'CIA'
        by_cia_mes_dict[table[2]].columns = list(map(lambda x: int(x) if isinstance(x,float) else x, by_cia_mes_dict[table[2]].columns))
        by_cia_mes_dict[table[2]].columns = pd.MultiIndex.from_product([[table[2]+'_abs'], by_cia_mes_dict[table[2]].columns])
        
    armas = by_cia_mes_dict['iaf_armas'].droplevel(0, axis=1)
    simulacros = by_cia_mes_dict['iaf_simulacros'].droplevel(0, axis=1)
    
    concat_arm_sim = pd.concat([armas,simulacros], sort=True).fillna(0)
    by_cia_mes_dict['iaf_armas_+_simulacros'] = concat_arm_sim.groupby(concat_arm_sim.index).sum().astype('uint16')    
    by_cia_mes_dict['iaf_armas_+_simulacros'].columns = pd.MultiIndex.from_product([['iaf_armas_+_simulacros'], by_cia_mes_dict['iaf_armas_+_simulacros'].columns])    
    
    total_armas = by_cia_mes_dict['iaf_armas_+_simulacros'].droplevel(0, axis=1)
    crimes_iaf = by_cia_mes_dict['iaf_crimes'].droplevel(0, axis=1)
    by_cia_mes_dict['iaf_indice'] = (total_armas / ( total_armas + crimes_iaf ) * 100).round(2)
    
    presos = by_cia_mes_dict['tri_presos'].droplevel(0, axis=1)
    crimes = by_cia_mes_dict['tri_crimes'].droplevel(0, axis=1)
    by_cia_mes_dict['tri_taxa'] = (presos / crimes * 100).round(2)
           
    pop = get_populacao()
    
    pop = pop.loc[by_cia_mes_dict['tcv'].index]
    
    by_cia_mes_dict['tcv_taxa'] = by_cia_mes_dict['tcv'].join(pop)
    by_cia_mes_dict['thc_taxa'] = by_cia_mes_dict['thc'].join(pop)
    by_cia_mes_dict['tqf_taxa'] = by_cia_mes_dict['tqf'].join(pop)

    
    for indicador in ['tcv', 'thc', 'tqf']:        
        pop = by_cia_mes_dict[indicador+'_taxa'].loc[:,('populacoes', 'POPULACAO')]    
        abs = by_cia_mes_dict[indicador+'_taxa'].loc[:,indicador+'_abs']
        for col in abs.columns:
            by_cia_mes_dict[indicador+'_taxa'].loc[:,(indicador+'_taxa', col)] = (
                abs.loc[:,col] / pop * 100000
            ).round(2)
        by_cia_mes_dict[indicador+'_taxa'] = by_cia_mes_dict[indicador+'_taxa'].join(metas[indicador])
        meta_abs = by_cia_mes_dict[indicador+'_taxa'].loc[:, 'meta_'+indicador+'_abs']        
        for col in meta_abs.columns:
            by_cia_mes_dict[indicador+'_taxa'].loc[:,('meta_'+indicador+'_taxa', col)] = (
                meta_abs.loc[:,col] / pop * 100000
            ).round(2)

        abs_mes = by_cia_mes_dict[indicador+'_taxa'].loc[:,indicador+'_abs'].iloc[:,-2]
        abs_acum = by_cia_mes_dict[indicador+'_taxa'].loc[:,indicador+'_abs'].iloc[:,-1]
        
        taxa_mes = by_cia_mes_dict[indicador+'_taxa'].loc[:,indicador+'_taxa'].iloc[:,-2]
        taxa_acum = by_cia_mes_dict[indicador+'_taxa'].loc[:,indicador+'_taxa'].iloc[:,-1]
        
        meta_abs_mes = by_cia_mes_dict[indicador+'_taxa'].loc[:,'meta_'+indicador+'_abs'].iloc[:,-2]
        meta_abs_acum = by_cia_mes_dict[indicador+'_taxa'].loc[:,'meta_'+indicador+'_abs'].iloc[:,-1]
        
        meta_taxa_mes = by_cia_mes_dict[indicador+'_taxa'].loc[:,'meta_'+indicador+'_taxa'].iloc[:,-2]
        meta_taxa_acum = by_cia_mes_dict[indicador+'_taxa'].loc[:,'meta_'+indicador+'_taxa'].iloc[:,-1]
        
        
        by_cia_mes_dict[indicador+'_mes'] = pd.concat([
            by_cia_mes_dict[indicador+'_taxa'].loc[:,'populacoes'],
            abs_mes,
            taxa_mes,
            meta_abs_mes,
            meta_taxa_mes
        ], axis=1)
        mes_table = by_cia_mes_dict[indicador+'_mes']
        mes_table.columns = [
            'pop', indicador+'_abs', indicador+'_taxa', 'meta_abs', 'meta_taxa'
        ]
        
        mes_table.loc[:, 'var%'] = ( ( taxa_mes - meta_taxa_mes ) / meta_taxa_mes ).round(2)

        by_cia_mes_dict[indicador+'_acum'] = pd.concat([
            by_cia_mes_dict[indicador+'_taxa'].loc[:,'populacoes'],
            abs_acum,
            taxa_acum,
            meta_abs_acum,
            meta_taxa_acum
        ], axis=1)
        acum_table = by_cia_mes_dict[indicador+'_acum']
        acum_table.columns = [
            'pop', indicador+'_abs', indicador+'_taxa', 'meta_abs', 'meta_taxa'
        ]
        
        acum_table.loc[:, 'var%'] = ( ( taxa_acum - meta_taxa_acum ) / meta_taxa_acum ).round(2)
        
        mes_table.loc[:,'farol'] = np.select(
            [
                by_cia_mes_dict[indicador+'_mes']['var%'] <= 0,
                by_cia_mes_dict[indicador+'_mes']['var%'] < 1
            ],
            [
                'J', 'K'
            ], default = 'L'
        )
        
        acum_table.loc[:,'farol'] = np.select(
            [
                by_cia_mes_dict[indicador+'_acum']['var%'] <= 0,
                by_cia_mes_dict[indicador+'_acum']['var%'] < 1
            ],
            [
                'J', 'K'
            ], default = 'L'
        )
        
        mes_table.columns = pd.MultiIndex.from_product([[indicador.upper()+'_MES'], mes_table.columns])
        acum_table.columns = pd.MultiIndex.from_product([[indicador.upper()+'_ACUM'], acum_table.columns])    
       
    
    
    by_cia_mes_dict['iaf_mes'] = pd.concat([        
        by_cia_mes_dict['iaf_armas_+_simulacros'].loc[:,'iaf_armas_+_simulacros'].iloc[:,-2],
        by_cia_mes_dict['iaf_crimes'].loc[:,'iaf_crimes_abs'].iloc[:,-2],
        by_cia_mes_dict['iaf_indice'].iloc[:, -2],
        metas['iaf'].loc[:,'metas_iaf'].iloc[:,-2]
        
    ], sort=True, axis=1)
    mes_table = by_cia_mes_dict['iaf_mes']
    mes_table.columns = ['AFA', 'TCAF','Índice', 'Meta']
    mes_table.loc[:,'var%'] = ( mes_table['Índice'] / mes_table['Meta'] ).round(2)
    mes_table.loc[:,'Farol'] = np.select(
    [
        mes_table.loc[:, 'var%'] < 0.7,
        mes_table.loc[:, 'var%'] < 1
    ],
    [
        'L',
        'K'
    ],
    default='J')
    mes_table.columns = pd.MultiIndex.from_product([['IAF_MES'], mes_table.columns])
    
    by_cia_mes_dict['iaf_acum'] = pd.concat([        
        by_cia_mes_dict['iaf_armas_+_simulacros'].loc[:,'iaf_armas_+_simulacros'].iloc[:,-1],
        by_cia_mes_dict['iaf_crimes'].loc[:,'iaf_crimes_abs'].iloc[:,-1],
        by_cia_mes_dict['iaf_indice'].iloc[:, -1],
        metas['iaf'].loc[:,'metas_iaf'].iloc[:,-1]
        
    ], sort=True, axis=1)
    acum_table = by_cia_mes_dict['iaf_acum']
    acum_table.columns = ['AFA', 'TCAF','Índice', 'Meta']
    acum_table.loc[:,'var%'] = ( acum_table['Índice'] / acum_table['Meta'] ).round(2)
    acum_table.loc[:,'Farol'] = np.select(
    [
        acum_table.loc[:, 'var%'] < 0.7,
        acum_table.loc[:, 'var%'] < 1
    ],
    [
        'L',
        'K'
    ],
    default='J')
    acum_table.columns = pd.MultiIndex.from_product([['IAF_ACUM'], acum_table.columns])
    
    
    by_cia_mes_dict['tri_mes'] = pd.concat([        
        by_cia_mes_dict['tri_presos'].loc[:,'tri_presos_abs'].iloc[:,-2],
        by_cia_mes_dict['tri_crimes'].loc[:,'tri_crimes_abs'].iloc[:,-2],
        by_cia_mes_dict['tri_taxa'].iloc[:,-2],
        metas['tri'].loc[:, 'metas_tri'].iloc[:,-2]        
    ], sort=True, axis=1)
    mes_table = by_cia_mes_dict['tri_mes']
    mes_table.columns = ['NPAA', 'TRCV', 'Taxa', 'Meta']
    mes_table.loc[:,'var%'] = ( mes_table['Taxa'] / mes_table['Meta'] ).round(2)
    mes_table.loc[:,'Farol'] = np.select(
    [
        mes_table.loc[:, 'var%'] < 0.7,
        mes_table.loc[:, 'var%'] < 1
    ],
    [
        'L',
        'K'
    ],
    default='J')
    mes_table.columns = pd.MultiIndex.from_product([['TRI_MES'], mes_table.columns])
    
    
    by_cia_mes_dict['tri_acum'] = pd.concat([        
        by_cia_mes_dict['tri_presos'].loc[:,'tri_presos_abs'].iloc[:,-1],
        by_cia_mes_dict['tri_crimes'].loc[:,'tri_crimes_abs'].iloc[:,-1],
        by_cia_mes_dict['tri_taxa'].iloc[:,-1],
        metas['tri'].loc[:, 'metas_tri'].iloc[:,-1]        
    ], sort=True, axis=1)
    acum_table = by_cia_mes_dict['tri_acum']
    acum_table.columns = ['NPAA', 'TRCV', 'Taxa', 'Meta']
    acum_table.loc[:,'var%'] = ( acum_table['Taxa'] / acum_table['Meta'] ).round(2)
    acum_table.loc[:,'Farol'] = np.select(
    [
        acum_table.loc[:, 'var%'] < 0.7,
        acum_table.loc[:, 'var%'] < 1
    ],
    [
        'L',
        'K'
    ],
    default='J')
    acum_table.columns = pd.MultiIndex.from_product([['TRI_ACUM'], acum_table.columns])
    
        
    return by_cia_mes_dict
    



def get_populacao():
    pop = pd.read_sql_table('tbl_populacoes', 'sqlite:///gdo.db')
    pop.columns = pd.MultiIndex.from_product([['populacoes'], pop.columns])
    pop.set_index(('populacoes','CIA'), inplace=True)
    pop.index.name = 'CIA'
    return pop

# funtion
def multiple_dfs(df_list, sheets, file_name, spaces):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    row = 0
    for dataframe in df_list:
        dataframe.to_excel(writer,sheet_name=sheets,startrow=row , startcol=0)   
        row = row + len(dataframe.index) + spaces + 1
    writer.save()
