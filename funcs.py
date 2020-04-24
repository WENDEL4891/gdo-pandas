import pandas as pd


def get_files_names(only_not_imported=False):
    '''retorna uma lista, com 4 listas de nomes de arquivos
    de rat (rat, viatura, efetivo, produtividade)'''
    import os,re    

    dir = os.getcwd()
    rats_files_names = os.listdir(dir+'/files/RAT')
    rat_pattern = re.compile('REDS_RAT_2')
    rat_viatura_pattern = re.compile('RAT_VIATURA')
    rat_efetivo_pattern = re.compile('RAT_EFETIVO')
    rat_produtividade_pattern = re.compile('RAT_Produtividade')

    rats_files = list()
    rats_viatura_files = list()
    rats_efetivo_files = list()
    rats_produtividade_files = list()

    if only_not_imported:
        df_imported_files = pd.read_sql_table('tbl_imported_files', 'sqlite:///gdo.db')
        rats_files_names = [file_name for file_name in rats_files_names if file_name not in df_imported_files['0'].values ]

    for file_name in rats_files_names:
        is_rat = rat_pattern.search(file_name) != None
        is_rat_viatura = rat_viatura_pattern.search(file_name) != None
        is_rat_efetivo = rat_efetivo_pattern.search(file_name) != None
        is_rat_produtividade = rat_produtividade_pattern.search(file_name) != None
        if is_rat:
            rats_files.append(file_name)
        elif is_rat_viatura:
            rats_viatura_files.append(file_name)
        elif is_rat_efetivo:
            rats_efetivo_files.append(file_name)
        elif is_rat_produtividade:
            rats_produtividade_files.append(file_name)
            
    rats_files.sort(key=lambda name: int(name[9:17]))
    rats_viatura_files.sort(key=lambda name: int(name[18:26]))
    rats_efetivo_files.sort(key=lambda name: int(name[18:26]))
    rats_produtividade_files.sort(key=lambda name: int(name[23:31]))
    return {
        'rat':rats_files,
        'ratv':rats_viatura_files,
        'rate':rats_efetivo_files,
        'ratp':rats_produtividade_files
    }