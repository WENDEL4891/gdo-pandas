import pandas as pd
import numpy as np
import os

file_list = os.listdir('files/Armazem/2020/'+str(mes))

def read_files(path_file, sheet_name):
    df = pd.read_excel(path_file, sheet_name=sheet_name)
    return df

tcv = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'TCV' in file, file_list))[0], sheet_name='BD')
thc = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'THC' in file, file_list))[0], sheet_name='HC VITIMAS')
tqf = read_files('files/Armazem/2020/'+str(mes)+'/'+list(filter(lambda file: 'TQF' in file, file_list))[0], sheet_name='BD')
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


tcv_by_cia_mes = tcv[tcv['MES'] <= mes].groupby(['MES','23_CIA']).sum()['Qtde Ocorrências'].unstack('MES')
thc_by_cia_mes = thc.groupby(['MES','23_CIA']).sum()['Qtde Envolvidos'].unstack('MES')
thc_by_cia_mes = thc_by_cia_mes.fillna(0).astype('int16')
tqf_by_cia_mes = tqf.groupby(['MES','23_CIA']).count()['Número REDS'].unstack('MES')
tri_crimes_by_cia_mes = tri_crimes.groupby(['MES','23_CIA_CV']).sum()['Qtde Ocorrências'].unstack('MES')
tri_presos_by_cia_mes = tri_presos.groupby(['MES','23_CIA_PRISOES']).sum()['Qtde Envolvidos'].unstack('MES')
iaf_crimes_by_cia_mes = iaf_crimes.groupby(['MES','23_CIA_TCAF']).sum()['Qtde Ocorrências'].unstack('MES')
iaf_crimes_by_cia_mes = iaf_crimes_by_cia_mes.fillna(0).astype('uint16')
iaf_crimes_by_cia_mes.columns = [int(mes) for mes in iaf_crimes_by_cia_mes.columns]
iaf_armas_by_cia_mes = iaf_armas.groupby(['MES','23_CIA_AFA']).count()['Número REDS'].unstack('MES')
iaf_armas_by_cia_mes = iaf_armas_by_cia_mes.fillna(0).astype('uint16')
iaf_simulacros_by_cia_mes = iaf_simulacros.groupby(['MES','23_CIA_SIMULACRO']).sum()['Qtde Materiais'].unstack('MES')
iaf_simulacros_by_cia_mes = iaf_simulacros_by_cia_mes.fillna(0).astype('uint16')

def get_metas(mes):
    '''Retorna um dicionário, com as metas '''
    metas = pd.read_sql_table('tbl_metas', 'sqlite:///gdo.db')
    
    metas_by_cia_indicador_mes = metas[
        metas['MES'] <= mes
    ].groupby(['CIA','INDICADOR','MES']).sum()['META'].unstack('MES')
    
    metas = {}
    metas_somar = ['TCV', 'THC', 'IC', 'OLS', 'RQV_EE', 'RQV_EFET', 'TQF']
    metas_nao_somar = ['DDU_CONCLUIDO', 'DDU_SUCESSO', 'IAF', 'TRI']    
    for meta in metas_somar:
        metas[meta] = metas_by_cia_indicador_mes.xs(meta, level=1).copy()
        metas[meta].loc[:,'ACUM'] = metas[meta].sum(1)
        metas[meta].loc['TOTAL'] = metas[meta].sum()
        metas[meta].columns = pd.MultiIndex.from_product([['META '+meta], metas[meta].columns])
    for meta in metas_nao_somar:
        metas[meta] = metas_by_cia_indicador_mes.xs(meta, level=1).copy()
        metas[meta].loc[:,'ACUM'] = metas[meta].iloc[:,0]
        metas[meta].loc['TOTAL'] = metas[meta].iloc[1]
        metas[meta].columns = pd.MultiIndex.from_product([['META '+meta], metas[meta].columns])
    return metas
    
metas = get_metas(mes=mes)

def get_populacao():
    pop = pd.read_sql_table('tbl_populacoes', 'sqlite:///gdo.db')
    pop.columns = pd.MultiIndex.from_product([['POPULACOES'], pop.columns])
    pop.set_index(('POPULACOES','CIA'), inplace=True)
    return pop

