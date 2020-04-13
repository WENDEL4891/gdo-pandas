cond_setor = [
    df_acum['RAT.NUM_ATIVIDADE'].isin(df_classif['VALIDADOR'])
]
res_setor = [
    df_classif[df_classif['SETOR']]
]


