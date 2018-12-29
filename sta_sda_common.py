#! -*- coding:utf-8 -*-

# 定义参数列表
import codecs
import os
import shutil

import re

apostrophe ='`'
blanks = ' ' * 12

def get_input_info_tables():
    data_froms = ['excel','database'] # 元数据来源
    project_names = ['crm','ehr','fin','app','cnsm_car','cnsm_house','anshuo','dmk','house','car','zx_cis','zx_300','zx_getway','fk']
    districts = ['cd','wh','bj']
    layers = ['sta','sda']
    extract_types = ['allData','increment']
    return data_froms,project_names,districts,layers,extract_types

def get_connection_informations(conn_name):
    connection_dict = {
        'crm_cd': ['10.1.2.216', 3306, 'root', 'mysql', 'edaicrm'],
        'crm_wh': ['10.1.2.216', 3306, 'root', 'mysql', 'edaicrm_wh'],
        'crm_bj': ['10.1.2.216', 3306, 'root', 'mysql', 'edaicrm_bj'],
        'app': ['10.1.2.216', 3306, 'yidaidw', '', 'yidaidw'],
        'fin': ['192.168.0.163', 1433, 'bquser', 'IytWDa2VK3yU1fmB', 'U9V30'],
        'ehr': ['10.1.2.219', 1433, 'sa', 'moer12#$', 'HCM'],
        'cnsm_car': ['10.1.27.3', 3306, 'metadata', 'metadata', 'cnsm_car'],
        'cnsm_house': ['10.1.27.3', 3306, 'metadata', 'metadata','cnsm_house'],
        'edw': ['10.1.27.2', 3306, 'edw', 'edw', 'edw'],
        'edw2': ['10.3.1.10', 9306, 'dwedw', 'edwdw12#$', 'edw2'],
        'anshuo': ['39.106.143.251', 1521, 'C##AMARCREDIT', 'amarcredit', 'orcl'],
        'house': ['10.3.1.234', 3306, 'edaihl', 'edaihl!@#123', 'house_pro'],
        'car': ['10.3.1.234', 3306, 'edaihl', 'edaihl!@#123', 'carloanfront'],
        'dmk': ['10.3.1.16', 1433, 'lidong', 'Edai@168', 'mel'],
        'zx_300': ['10.3.1.12', 3306, 'edaizx', 'edaizx!@#123', 'che_threehundred'],
        'zx_cis': ['10.3.1.12', 3306, 'edaizx', 'edaizx!@#123', 'cis'],
        'zx_getway': ['10.3.1.12', 3306, 'edaizx', 'edaizx!@#123', 'moer_getway'],
        'fk': ['10.3.1.12', 3306, 'edaifk', 'edaifk!@#123', 'anti_fraud']}
    return connection_dict[conn_name]

def reinput_hint(str_arr):
    print('输入错误，可选的输入值有：', ', '.join(str_arr))

def get_indents():
    indent0 = '\n' + ' ' * 4
    indent1 = '\n' + ' ' * 6
    indent2 = '\n' + ' ' * 8
    return indent0, indent1, indent2

# 创建程序保存目录
def make_server_folders(extract_type,layers,project_names,district='',root='kettleProgram'):
    if extract_type == 'increment':
        extract_type =''
    for layer in layers:
        for project_name in project_names:
            make_server_folder(extract_type,layer,project_name)

# 整合生成的目录及文件
def shuffle_files(project_name
               ,current_layer
               ,extract_type
               ,root='kettleProgram'
               ,district_suffix=''
               ,project_connect_name=''):
    source_path = project_name + district_suffix + os.sep + extract_type + os.sep +current_layer
    dest_path = root+os.sep
    all_data_path = 'alldata' + os.sep
    increment_data_path = ''
    if extract_type == 'alldata':
        dest_path += all_data_path
    elif extract_type == 'increment':
        dest_path += increment_data_path
    else:
        print('拷贝文件错误!!!')
        return

    dest_path += current_layer + os.sep + project_name + os.sep
    shutil.copytree(source_path,dest_path)
    shutil.rmtree(project_connect_name)

def make_server_folder(extract_type
                       ,layer=''
                       ,project_name=''
                       ,form1=''
                       ,form2=''
                       ,period=''
                       ,district=''
                       ,root='kettleProgram'):
    temp_pathes = [project_name, district, form1, form2, period]
    path = root + os.sep + extract_type + os.sep + layer + os.sep
    for item in temp_pathes:
        if item!='':
            path += item + os.sep
    path = path[:-1]
    os.makedirs(path)

def delete_old_files_mk_folder(folder_path):
    if os.path.exists(folder_path):
        delete_old_files(folder_path)

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

def delete_old_files(folder):
    if os.path.exists(folder):
        for root, dirs, files in os.walk(folder,topdown=False):
            for name in files:
                os.remove(os.path.join(root,name))
            for name in dirs:
                os.rmdir(os.path.join(root,name))

def move_sql_file_to_related_folder(current_folder_path,result_sql_file):
    filedir = '.'+os.path.sep + current_folder_path
    filenames = os.listdir(filedir)
    sqlfilenames = [file for file in filenames if file.endswith('.sql')]
    with codecs.open(result_sql_file,'a+',encoding='utf-8') as g:
        for filename in sqlfilenames:
            filepath = filedir + os.path.sep + filename
            for line in codecs.open(filepath,'r',encoding='utf-8'):
                g.writelines(line)
            os.remove(filepath)
    result_sql_src_path = '.' + os.path.sep + result_sql_file
    result_sql_dst_path = '.' + os.path.sep + current_folder_path + os.path.sep + result_sql_file
    shutil.move(result_sql_src_path,result_sql_dst_path)


def add_apostrophe(chg_list,alias =''):
    return list(map(lambda field:alias+apostrophe+str(field)+apostrophe
                    if str(field).upper() in get_mysql_keywords() else
                    alias + str(field),chg_list))

def get_rid_of_apostrophe(chg_list,alias = ''):
    return list(map(lambda field:alias+str(field)[1:-1]
                    if apostrophe in str(field) else
                    alias + str(field),chg_list))

# 生成主键和索引部分条件
def get_useful_key_index(
        primary_key,
        idx,
        sql_str):
    tmp_str = sql_str
    if primary_key != '':
        or_idx = primary_key.find('|')
        useful_key_index = primary_key[
            :primary_key.find('|')] if or_idx > 0 else primary_key
    elif idx != '':
        or_idx = idx.find('|')
        useful_key_index = idx[:idx.find('|')] if or_idx > 0 else idx
    useful_key_indexes = useful_key_index.split('+')
    useful_key_indexes_a = add_apostrophe(useful_key_indexes, 'a.')
    useful_key_indexes_b = add_apostrophe(useful_key_indexes, 'b.')

    length = len(useful_key_indexes_a)
    if length == 1:
        tmp_str += useful_key_indexes_a[0] + \
            ' = ' + useful_key_indexes_b[0] + '\n'
    else:
        for i in range(length - 1):
            tmp_str += useful_key_indexes_a[i] + ' = ' + \
                useful_key_indexes_b[i] + '\n    AND '
        tmp_str += useful_key_indexes_a[length - 1] + \
            ' = ' + useful_key_indexes_b[length - 1]
    return tmp_str, useful_key_indexes_a, useful_key_indexes_b, length

def sql_str_add_fields(sql_str,fields,types,layer=''
                       ,table_alias= 'a'
                       ,table_alias1= ''
                       ,oper_type = 'SELECT'
                       ,comp_type = ''
                       ,keys = ''
                       ,indexes = ''
                       ,condition = ''
                       ,middle_str = '\n'
                       ,end_str = '\n'):
    sql_str1,sql_str2 = " CONCAT_WS('|@|', "," CONCAT_WS('|@|', "
    tmp_strs = [s + ',\n' for s in sql_str.split(',\n')]
    tmp_strs[-1] = tmp_strs[-1][:-2]
    if tmp_strs[-1] == '':
        tmp_strs.remove(tmp_strs[-1])
    format_gap = ' ' * 20
    whole_str = sql_str

    for i in range(len(fields) -1):
        field = fields[i]
        if layer == 'sta' and oper_type.upper() == 'SELECT' and types[i][0:7] == 'tinyint': # 字段类型转换
            field = ' CAST(' + table_alias+field+ ' AS CHAR) AS ' + field
            sql_str += field + ',' + middle_str
        elif oper_type.upper() == 'JUDGE':
            if keys != '' and indexes != '':
                if field in keys or field in indexes:
                    continue
            sql_str += ' ' + table_alias + field + ' ' + comp_type + ' ' + table_alias1 + field + ' ' + condition + middle_str
        elif oper_type.upper() == 'COMBINE_JUGE':
            if keys != '' and indexes != '':
                if field in keys or field in indexes:
                    continue
            sql_str1 += table_alias+field + ', '
            sql_str2 += table_alias1+field + ', '
        elif oper_type.upper() == 'AS':
            tmp_str = ' '+table_alias +field+format_gap + 'AS'+field+','+middle_str
            whole_str += tmp_str
            tmp_strs.append(tmp_str)
        else:
            sql_str += ' ' + table_alias + field + ',' + middle_str

    if oper_type.upper() == 'JUDGE':
        sql_str += ' ' + table_alias + \
            fields[len(fields) - 1] + ' ' + comp_type + ' ' + \
            table_alias1 + fields[len(fields) - 1] + end_str
    elif oper_type.upper() == 'COMBINE_JUDGE':
        sql_str1 += table_alias + fields[len(fields) - 1] + ")"
        sql_str2 += table_alias1 + fields[len(fields) - 1] + ")"

        sql_str += sql_str1 + end_str + ' ' + comp_type + end_str + sql_str2
    elif oper_type.upper() == 'AS':
        tmp_str = ' ' + table_alias + fields[len(fields) - 1] + \
            format_gap + 'AS ' + fields[len(fields) - 1] + end_str
        whole_str += tmp_str
        tmp_strs.append(tmp_str)
        whole_str = format_sql_as_str(whole_str, tmp_strs)
        sql_str = whole_str
    else:
        sql_str += ' ' + table_alias + fields[len(fields) - 1] + end_str
    return sql_str

def format_sql_as_str(
        whole_str,
        str_arr=None):
    if str_arr is None:
        return ''
    max_blank = max([string.find(' AS') for string in str_arr])
    append_blanks = [max_blank - string.find(' AS') for string in str_arr]

    new_str_arr = list(map(lambda string: string[:string.find(' AS')] +
                           append_blanks[str_arr.index(string)] * ' ' +
                           string[string.find(' AS'):], str_arr))
    whole_str = ''.join(new_str_arr)
    return whole_str

# 全量插入所有数据
def insert_all_table_data(table_out
                          ,insert_type
                          ,tables_in
                          ,useful_fields
                          ,useful_types
                          ,table_alias
                          ,sys_from=[]
                          ,prj_name=''):
    sql_strs = []
    for table_in in tables_in:
        sql_str = 'INSERT INTO ' + table_out + '\n(\n  op_time,\n'
        if insert_type == '增量拉链' or insert_type == '主键拉链' or insert_type == '全量拉链' or insert_type == '全表拉链':
            sql_str += '  sda_end_date,\n'
        if prj_name == 'crm':
            sql_str += '  system_id,\n'
        sql_str = sql_str_add_fields(sql_str, useful_fields,useful_types, table_alias=table_alias)
        sql_str += ')\nSELECT\n  op_time,\n  '
        if insert_type == '增量拉链' or insert_type == '主键拉链' or insert_type == '全量拉链' or insert_type == '全表拉链':
            sql_str += '20990101,\n'
        if prj_name == 'crm':
            sql_str += '  \'' + sys_from[tables_in.index(table_in)] + '\',\n'
        sql_str = sql_str_add_fields(
            sql_str, useful_fields, useful_types, table_alias=table_alias)
        sql_str += 'FROM\n  ' + table_in + \
                   '\n' + 'WHERE op_time <= ${op_time};\n'
        sql_strs.append(sql_str)
    return sql_strs

# 增量插入当前（T-1）数据
def insert_type1_data(table_out
                      ,tables_in
                      ,useful_fields
                      ,useful_types
                      ,sys_from = []
                      ,prj_name = ''):
    sql_strs = []
    for table_in in tables_in:
        sql_str = 'INSERT INTO ' + table_out + '\n(\n  op_time,\n'
        if prj_name == 'crm':
            sql_str += '  system_id,\n'
        sql_str = sql_str_add_fields(sql_str, useful_fields, useful_types, table_alias='')
        sql_str += ')\nSELECT\n  op_time,\n'
        if prj_name == 'crm':
            sql_str += '  \'' + sys_from[tables_in.index(table_in)] + '\',\n'
        sql_str = sql_str_add_fields(
            sql_str, useful_fields, useful_types, table_alias='')
        sql_str += 'FROM\n  ' + table_in + \
            '\n' + 'WHERE op_time = ${op_time};\n'
        sql_strs.append(sql_str)
    return sql_strs

# STEP1
def insert_type2_step1_data(
        table_out_ins,
        table_out,
        tables_in,
        useful_fields,
        useful_types,
        primary_key,
        idx,
        sys_from,
        prj_name):
    sql_str = '##--[STEP1]:识别当日新增数据 - 创建临时表' + \
        table_out_ins + '，存储当日新增数据\n\n' + \
        '# ----[STEP1.1] - 创建临时表' + table_out_ins.upper() + \
        '\nDROP TABLE IF EXISTS ' + table_out_ins + ';' + \
        '\nCREATE TABLE ' + table_out_ins + ' LIKE ' + \
        table_out + ';\n\n'

    sql_strs_tmp = insert_type2_data(
        table_out_ins,
        table_out,
        tables_in,
        useful_fields,
        useful_types,
        primary_key,
        idx,
        '',
        'where',
        sys_from,
        prj_name
    )
    sql_str = sql_str + sql_strs_tmp[0]
    return sql_str

# STEP2
def insert_type2_step2_data(
        table_out_upd,
        table_out,
        tables_in,
        useful_fields,
        useful_types,
        primary_key,
        idx,
        sys_from,
        prj_name):
    sql_str = '##--[STEP2]:识别当日修改数据 - 创建临时表' + \
        table_out_upd + '，存储当日修改数据\n\n' + \
        '# ----[STEP2.1] - 创建临时表' + table_out_upd.upper() + \
        '\nDROP TABLE IF EXISTS ' + table_out_upd + ';' + \
        '\nCREATE TABLE ' + table_out_upd + ' LIKE ' + \
        table_out + ';\n\n'

    sql_strs_tmp = insert_type2_data(
        table_out_upd,
        table_out,
        tables_in,
        useful_fields,
        useful_types,
        primary_key,
        idx,
        '',
        'and',
        sys_from,
        prj_name
    )
    sql_str = sql_str + sql_strs_tmp[0]
    return sql_str

#STEP3
def insert_type2_step3_data(
        sql_str,
        table_out,
        tables_in,
        primary_key,
        idx):
    sql_str = sql_str.replace('sda_crm_accounts_h',table_out)
    if 'sda_crm' in table_out:
        sql_str = sql_str.replace('sta_crm_accounts_cd',tables_in[0])
        sql_str = sql_str.replace('sta_crm_accounts_bj',tables_in[1])
        sql_str = sql_str.replace('sta_crm_accounts_wh',tables_in[2])
    else:
        sql_str = sql_str.replace('sta_crm_accounts_cd',tables_in[0])
        sql_str = sql_str.replace(r'    ##--OD有,且ND1\ND2\ND3均无：即为删除数据',r'    ##--OD有,且ND1无：即为删除数据')
    sql_str = sql_str.replace('SDA_CRM_ACCOUNTS_H',table_out.upper())
    idxes = sql_str_get_indexes(primary_key, idx)
    indexes = idxes[0].split('+')
    index_count = len(indexes)
    if index_count == 1:
        sql_str = sql_str.replace(',id', ',' + indexes[0])
        sql_str = sql_str.replace('.id', '.' + indexes[0])
    else:
        new_strs = [''] * 4
        old_as_str_arr = ['     od.op_time                         ' +
                          '                  AS op_time\n']
        for item in indexes:
            new_strs[0] += '    ,' + item + '\n'
            tmp_str = '    ,od.' + item + '         AS ' + item + '\n'
            new_strs[1] += tmp_str
            old_as_str_arr.append(tmp_str)
            new_strs[2] += '    od.' + item + ' = nd1.' + item + ' AND\n'
            new_strs[3] += '    nd1.' + item + ' IS NULL\n    AND'
        new_strs[2] = new_strs[2][:-5]
        new_strs[3] = new_strs[3][:-4]

        sql_str = sql_str.replace('    ,id', new_strs[0])
        new_strs[1] = format_sql_as_str(new_strs[1], old_as_str_arr)
        new_strs[1] = '\n' + new_strs[1]
        pattern = r'\s+od\.\w+\s+AS\s\w+\n\s+,od\.\w+\s+AS\s\w+\n'
        sql_str = re.sub(pattern, new_strs[1], sql_str)
        sql_str = sql_str.replace('    od.id = nd1.id', new_strs[2])
        sql_str = sql_str.replace('    nd1.id IS NULL', new_strs[3])
    return sql_str

#STEP4
def insert_type2_step4_data(
        sql_str,
        table_out):
    sql_str = sql_str.replace('sda_crm_accounts_h',table_out)
    return sql_str

#STEP5
def insert_type2_step5_data(
        fields,
        table_out,
        table_out_ins,
        table_out_del,
        table_out_upd,
        primary_key,
        idx,
        prj_name):
    sql_str = ''
    sql_str = update_old_table(
        sql_str,
        table_out,
        table_out_del,
        table_out_upd,
        primary_key,
        idx,
        prj_name
    )
    sql_str += insert_ins_upd_data(
        table_out,
        table_out_upd,
        table_out_ins,
        fields,
        prj_name)
    return sql_str

# [STEP5.1] [STEP5.2]
def update_old_table(
        sql_str,
        table_out,
        table_out_del,
        table_out_upd,
        keys,
        idx,
        prj_name='crm'):
    sql_str = '##--[STEP5]:数据拉链：当日变更数据（增删改）与' + \
        '历史存量数据拉链合并\n\n##----[STEP5.1] - 已删除数据关链\n'
    sql_str += 'UPDATE\n\t' + table_out + ' od\nSET\n\tod.sda_end_date = ' + \
        '${op_time}\nWHERE\n\tod.op_time <= ${op_time}\nAND ' + \
        'od.sda_end_date > ${op_time}\nAND EXISTS (\n' + blanks + \
        'SELECT 1\n' + blanks + ' FROM ' + table_out_del + ' del\n' + \
        blanks + 'WHERE od.op_time = del.op_time\n'
    if prj_name == 'crm':
        sql_str += blanks + ' AND od.system_id = del.system_id\n'

    idxes = sql_str_get_indexes(keys, idx)
    indexes = idxes[0].split('+')
    index_count = len(indexes)

    if index_count == 1:
        sql_str += blanks + ' AND od.' + indexes[0] + ' = del.' + \
            indexes[0] + '\n' + blanks + ')\n;\n'
    else:
        for item in indexes:
            sql_str += blanks + 'AND od.' + item + ' = del.' + \
                item + '\n'
        sql_str += blanks + ')\n;\n'

    sql_str += '##----[STEP5.2] - 已修改数据的历史版本关链\n' + \
        'UPDATE\n\t' + table_out + ' od\nSET\n\tod.sda_end_date = ' + \
        '${op_time}\nWHERE\n\tod.op_time < ${op_time}\nAND ' + \
        'od.sda_end_date >= ${op_time}\nAND EXISTS (\n' + blanks + \
        'SELECT 1\n' + blanks + ' FROM ' + table_out_upd + ' upd\n'

    if index_count == 1:
        sql_str += blanks + ' WHERE od.' + indexes[0] + ' = upd.' + \
            indexes[0] + '\n'
    else:
        sql_str += blanks + 'WHERE'
        for item in indexes:
            sql_str += '  od.' + item + ' = upd.' + \
                item + '\n' + blanks + '   AND'
        sql_str = sql_str[:-16]
    if prj_name == 'crm':
        sql_str += blanks + ' AND od.system_id = upd.system_id\n'
    sql_str += blanks + ')\n;\n'
    return sql_str

#[STEP5.3] 已修改数据的最新版本开链
def insert_ins_upd_data(
        table_out,
        table_out_upd,
        table_out_ins,
        useful_fields,
        prj_name='crm'):
    def insert_ins_upd_data_inner(
            sql_str,
            table_out,
            table_out_tmp,
            useful_fields,
            prj_name='crm'):
        sql_str = sql_str_add_fields(
            sql_str,
            useful_fields,
            types='',
            table_alias='',
            table_alias1='',
            oper_type='',
            comp_type='',
            keys='',
            indexes='',
            condition='',
            middle_str='\n')
        sql_str += ')\nSELECT\n  op_time,\n  sda_end_date,\n'
        if prj_name == 'crm':
            sql_str += '  system_id,\n'
        sql_str = sql_str_add_fields(
            sql_str,
            useful_fields,
            types='',
            layer='',
            table_alias='',
            table_alias1='',
            oper_type='SELECT',
            comp_type='',
            keys='',
            indexes='',
            condition='',
            middle_str='\n',
            end_str='\n') + 'FROM\n  ' + table_out_tmp + '\n;\n\n'
        return sql_str

    sql_str = '##----[STEP5.3] - 已修改数据的最新版本开链\nINSERT INTO ' + \
        table_out + '\n(\n  op_time,\n  sda_end_date,\n'
    if prj_name == 'crm':
        sql_str += '  system_id,\n'
    sql_str = insert_ins_upd_data_inner(
        sql_str,
        table_out,
        table_out_upd,
        useful_fields,
        prj_name)
    sql_str += '##----[STEP5.4] - 新增数据的最新版本开链\nINSERT INTO ' + \
        table_out + '\n(\n  op_time,\n  sda_end_date,\n'
    if prj_name == 'crm':
        sql_str += '  system_id,\n'
    sql_str = insert_ins_upd_data_inner(
        sql_str,
        table_out,
        table_out_ins,
        useful_fields,
        prj_name)

    return sql_str

def insert_type2_data(
        table_out_tmp,
        table_out,
        tables_in,
        useful_fields,
        useful_types,
        primary_key,
        idx,
        table_alias,
        more,
        sys_from=[],
        prj_name=''):

    sql_strs = []
    sql_str = ''
    step = 1 if '_ins' in table_out_tmp else 2

    for table_in in tables_in:
        sql_str += '##----[STEP' + str(step) + '.' + str(tables_in.index(table_in) + 2) + '] - ' + prj_name.upper()
        if prj_name == 'crm':
            sql_str += '_' + sys_from[tables_in.index(table_in)].upper()
        sql_str += '库新增数据写入' + table_out_tmp.upper() + '表\n'
        sql_str1 = 'INSERT INTO ' + table_out_tmp + '\n(\n  op_time,\n  sda_end_date,\n'
        if prj_name == 'crm':
            sql_str1 += '  system_id,\n\n'
        sql_str1 = sql_str_add_fields(sql_str1,
                                      useful_fields,
                                      useful_types,
                                      table_alias='',
                                      oper_type='',
                                      comp_type='',
                                      keys=primary_key,
                                      indexes=idx,
                                      condition='')
        sql_str1 += ')\nSELECT\n\n'
        sql_str2 = '  nd.op_time' + blanks + 'AS op_time,\n' + \
            '  20990101' + blanks + 'AS sda_end_date,\n'
        if prj_name == 'crm':
            sql_str2 += '  \'' + sys_from[tables_in.index(table_in)] + '\'' + blanks + 'AS system_id,\n\n'
        sql_str1 += sql_str_add_fields(sql_str2,
                                       useful_fields,
                                       useful_types,
                                       table_alias='nd.',
                                       oper_type='AS',
                                       comp_type='',
                                       keys=primary_key,
                                       indexes=idx,
                                       condition='')

        sql_str1 += 'FROM\n    (\n    SELECT\n      *\n    FROM\n        ' + \
            table_in + '\n    WHERE\n        op_time = ${op_time}\n' + \
            '    ) nd                 ##--ND(New Data)表为STA层当日抽取数据'
        if more.upper() == 'WHERE':
            sql_str1 += '\nLEFT JOIN'
        else:
            sql_str1 += '\nINNER JOIN'
        sql_str1 += '\n    (\n    SELECT\n      *\n' + \
            '    FROM\n        ' + table_out + '\n    WHERE\n        '
        if prj_name == 'crm':
            sql_str1 += 'system_id = \'' + \
                sys_from[tables_in.index(table_in)] + '\'\n    AND '
        sql_str1 += 'op_time < ${op_time}\n    AND sda_end_date ' + \
            '>= ${op_time}\n    ) od\n\nON\n    '

        idxes = sql_str_get_indexes(primary_key, idx)
        indexes = idxes[0].split('+')
        index_count = len(indexes)
        if index_count == 1:
            sql_str1 += 'nd.' + indexes[0] + ' = od.' + indexes[0]
        else:
            for item in indexes:
                sql_str1 += 'nd.' + item + ' = od.' + item + '\n AND '
            sql_str1 = sql_str1[:-5]
        sql_str1 += '                 ' + \
            '##--关联字段：物理主键或逻辑主键（注意：若ND表的关联主键有重复，' + \
            '可在ND子查询中加distinct或使用其他方式去重）\n\n'
        if more.upper() == 'WHERE':
            sql_str1 += 'WHERE\n    '
            if index_count == 1:
                sql_str1 += 'od.' + indexes[0] + ' IS NULL'
            else:
                for item in indexes:
                    sql_str1 += 'od.' + item + ' IS NULL' + '\n OR '
                sql_str1 = sql_str1[:-5]
            sql_str1 += '                   ' + \
                '  # --ND有,OD无：即为新增\n;\n\n'
        elif more.upper() == 'AND':
            sql_str1 += 'AND\n  '
            sql_str1 += sql_str_add_compare_fields(
                sql_str1,
                useful_fields,
                table_alias='nd.',
                table_alias1='od.',
                comp_type='<>',
                keys=primary_key,
                indexes=idx,
                end_str='\n')
        else:
            pass
        sql_str += sql_str1

    sql_strs.append(sql_str)
    return sql_strs

# 全量覆盖SQL
def insert_type4_data(
        table_out,
        tables_in,
        useful_fields,
        useful_types,
        table_alias,
        sys_from=[],
        prj_name=''):
    sql_strs = []
    for table_in in tables_in:
        sql_str = 'INSERT INTO ' + table_out + '\nSELECT\n  op_time,\n'
        if prj_name == 'crm':
            sql_str += '  \'' + sys_from[tables_in.index(table_in)] + '\',\n'
        sql_str = sql_str_add_fields(sql_str, useful_fields, useful_types, table_alias=table_alias)
        sql_str += 'FROM\n  ' + table_in + ';'
        sql_strs.append(sql_str)
    return sql_strs

# 输出xmldoc ETL程序文件
def append_xml_header(dest_file,xmldoc):
    xmldoc.write(dest_file,encoding='utf-8')
    with codecs.open(dest_file,'r+',encoding='utf-8') as f:
        old =f.read()
        f.seek(0)
        f.write('<?xml version=\"1.0\" encoding=\"UTF-8\"?>')
        f.write('\n')
        f.write(old)

# 获取表主键索引字段
def sql_str_get_indexes(key, index):
    indexes = sql_str_get_key_index(key)
    if indexes == []:
        indexes = sql_str_get_key_index(index)
    return indexes

# 索引字段通过 '|' 分割
def sql_str_get_key_index(key_index):
    key_index = str(key_index)
    if key_index != '' and key_index != 'nan':
        try:
            key_indexes = key_index.split('|')
        except Exception as e:
            print(e)
    else:
        key_indexes = []
    return key_indexes

def sql_str_add_compare_fields(
        sql_str,
        fields,
        table_alias='a.',
        table_alias1='',
        comp_type='<>',
        keys='',
        indexes='',
        end_str='\n'):
    sql_str1, sql_str2 = "  CONCAT_WS('|@|',\n", "  CONCAT_WS('|@|',\n"
    for i in range(len(fields) - 1):
        field = fields[i]
        if field in keys or field in indexes:
            continue
        sql_str1 += blanks + table_alias + field + ",\n"
        sql_str2 += blanks + table_alias1 + field + ",\n"
    sql_str1 += blanks + table_alias + fields[len(fields) - 1] + ")"
    sql_str2 += blanks + table_alias1 + fields[len(fields) - 1] + ")"

    sql_str = sql_str1 + end_str + '  ' + comp_type + end_str + \
        sql_str2 + ';' + end_str + end_str
    return sql_str

# 定义MySql关键字
def get_mysql_keywords():
    return ['ACCESSIBLE', 'ACCOUNT', 'ACTION', 'ADD', 'AFTER', 'AGAINST',
            'AGGREGATE', 'ALGORITHM', 'ALL', 'ALTER', 'ALWAYS', 'ANALYSE',
            'ANALYZE', 'AND', 'ANY', 'AS', 'ASC', 'ASCII', 'ASENSITIVE',
            'AT', 'AUTOEXTEND_SIZE', 'AUTO_INCREMENT', 'AVG',
            'AVG_ROW_LENGTH', 'BACKUP', 'BEFORE', 'BEGIN', 'BETWEEN',
            'BIGINT', 'BINARY', 'BINLOG', 'BIT', 'BLOB', 'BLOCK', 'BOOL',
            'BOOLEAN', 'BOTH', 'BTREE', 'BY', 'BYTE', 'CACHE', 'CALL',
            'CASCADE', 'CASCADED', 'CASE', 'CATALOG_NAME', 'CHAIN',
            'CHANGE', 'CHANGED', 'CHANNEL', 'CHAR', 'CHARACTER', 'CHARSET',
            'CHECK', 'CHECKSUM', 'CIPHER', 'CLASS_ORIGIN', 'CLIENT',
            'CLOSE', 'COALESCE', 'CODE', 'COLLATE', 'COLLATION', 'COLUMN',
            'COLUMNS', 'COLUMN_FORMAT', 'COLUMN_NAME', 'COMMENT', 'COMMIT',
            'COMMITTED', 'COMPACT', 'COMPLETION', 'COMPRESSED',
            'COMPRESSION', 'CONCURRENT', 'CONDITION', 'CONNECTION',
            'CONSISTENT', 'CONSTRAINT', 'CONSTRAINT_CATALOG',
            'CONSTRAINT_NAME', 'CONSTRAINT_SCHEMA', 'CONTAINS', 'CONTEXT',
            'CONTINUE', 'CONVERT', 'CPU', 'CREATE', 'CROSS', 'CUBE',
            'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
            'CURRENT_USER', 'CURSOR', 'CURSOR_NAME', 'DATA', 'DATABASE',
            'DATABASES', 'DATAFILE', 'DATE', 'DATETIME', 'DAY', 'DAY_HOUR',
            'DAY_MICROSECOND', 'DAY_MINUTE', 'DAY_SECOND', 'DEALLOCATE',
            'DEC', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DEFAULT_AUTH',
            'DEFINER', 'DELAYED', 'DELAY_KEY_WRITE', 'DELETE', 'DESC',
            'DESCRIBE', 'DES_KEY_FILE', 'DETERMINISTIC', 'DIAGNOSTICS',
            'DIRECTORY', 'DISABLE', 'DISCARD', 'DISK', 'DISTINCT',
            'DISTINCTROW', 'DIV', 'DO', 'DOUBLE', 'DROP', 'DUAL',
            'DUMPFILE', 'DUPLICATE', 'DYNAMIC', 'EACH', 'ELSE', 'ELSEIF',
            'ENABLE', 'ENCLOSED', 'ENCRYPTION', 'END', 'ENDS', 'ENGINE',
            'ENGINES', 'ENUM', 'ERROR', 'ERRORS', 'ESCAPE', 'ESCAPED',
            'EVENT', 'EVENTS', 'EVERY', 'EXCHANGE', 'EXECUTE', 'EXISTS',
            'EXIT', 'EXPANSION', 'EXPIRE', 'EXPLAIN', 'EXPORT', 'EXTENDED',
            'EXTENT_SIZE', 'FALSE', 'FAST', 'FAULTS', 'FETCH', 'FIELDS',
            'FILE', 'FILE_BLOCK_SIZE', 'FILTER', 'FIRST', 'FIXED', 'FLOAT',
            'FLOAT4', 'FLOAT8', 'FLUSH', 'FOLLOWS', 'FOR', 'FORCE',
            'FOREIGN', 'FORMAT', 'FOUND', 'FROM', 'FULL', 'FULLTEXT',
            'FUNCTION', 'GENERAL', 'GENERATED', 'GEOMETRY',
            'GEOMETRYCOLLECTION', 'GET', 'GET_FORMAT', 'GLOBAL', 'GRANT',
            'GRANTS', 'GROUP', 'GROUP_REPLICATION', 'HANDLER', 'HASH',
            'HAVING', 'HELP', 'HIGH_PRIORITY', 'HOST', 'HOSTS', 'HOUR',
            'HOUR_MICROSECOND', 'HOUR_MINUTE', 'HOUR_SECOND', 'IDENTIFIED',
            'IF', 'IGNORE', 'IGNORE_SERVER_IDS', 'IMPORT', 'IN', 'INDEX',
            'INDEXES', 'INFILE', 'INITIAL_SIZE', 'INNER', 'INOUT',
            'INSENSITIVE', 'INSERT', 'INSERT_METHOD', 'INSTALL',
            'INSTANCE', 'INT', 'INT1', 'INT2', 'INT3', 'INT4', 'INT8',
            'INTEGER', 'INTERVAL', 'INTO', 'INVOKER', 'IO',
            'IO_AFTER_GTIDS', 'IO_BEFORE_GTIDS', 'IO_THREAD', 'IPC', 'IS',
            'ISOLATION', 'ISSUER', 'ITERATE', 'JOIN', 'JSON', 'KEY',
            'KEYS', 'KEY_BLOCK_SIZE', 'KILL', 'LANGUAGE', 'LAST',
            'LEADING', 'LEAVE', 'LEAVES', 'LEFT', 'LESS', 'LEVEL', 'LIKE',
            'LIMIT', 'LINEAR', 'LINES', 'LINESTRING', 'LIST', 'LOAD',
            'LOCAL', 'LOCALTIME', 'LOCALTIMESTAMP', 'LOCK', 'LOCKS',
            'LOGFILE', 'LOGS', 'LONG', 'LONGBLOB', 'LONGTEXT', 'LOOP',
            'LOW_PRIORITY', 'MASTER', 'MASTER_AUTO_POSITION',
            'MASTER_BIND', 'MASTER_CONNECT_RETRY', 'MASTER_DELAY',
            'MASTER_HEARTBEAT_PERIOD', 'MASTER_HOST', 'MASTER_LOG_FILE',
            'MASTER_LOG_POS', 'MASTER_PASSWORD', 'MASTER_PORT',
            'MASTER_RETRY_COUNT', 'MASTER_SERVER_ID', 'MASTER_SSL',
            'MASTER_SSL_CA', 'MASTER_SSL_CAPATH', 'MASTER_SSL_CERT',
            'MASTER_SSL_CIPHER', 'MASTER_SSL_CRL', 'MASTER_SSL_CRLPATH',
            'MASTER_SSL_KEY', 'MASTER_SSL_VERIFY_SERVER_CERT',
            'MASTER_TLS_VERSION', 'MASTER_USER', 'MATCH', 'MAXVALUE',
            'MAX_CONNECTIONS_PER_HOUR', 'MAX_QUERIES_PER_HOUR', 'MAX_ROWS',
            'MAX_SIZE', 'MAX_STATEMENT_TIME的', 'MAX_UPDATES_PER_HOUR',
            'MAX_USER_CONNECTIONS', 'MEDIUM', 'MEDIUMBLOB', 'MEDIUMINT',
            'MEDIUMTEXT', 'MEMORY', 'MERGE', 'MESSAGE_TEXT', 'MICROSECOND',
            'MIDDLEINT', 'MIGRATE', 'MINUTE', 'MINUTE_MICROSECOND',
            'MINUTE_SECOND', 'MIN_ROWS', 'MOD', 'MODE', 'MODIFIES',
            'MODIFY', 'MONTH', 'MULTILINESTRING', 'MULTIPOINT',
            'MULTIPOLYGON', 'MUTEX', 'MYSQL_ERRNO', 'NAME', 'NAMES',
            'NATIONAL', 'NATURAL', 'NCHAR', 'NDB', 'NDBCLUSTER',
            'NEVER', 'NEW', 'NEXT', 'NO', 'NODEGROUP', 'NONBLOCKING',
            'NONE', 'NOT', 'NO_WAIT', 'NO_WRITE_TO_BINLOG', 'NULL',
            'NUMBER', 'NUMERIC', 'NVARCHAR', 'OFFSET', 'OLD_PASSWORD',
            'ON', 'ONE', 'ONLY', 'OPEN', 'OPTIMIZE', 'OPTIMIZER_COSTS',
            'OPTION', 'OPTIONALLY', 'OPTIONS', 'OR', 'ORDER', 'OUT',
            'OUTER', 'OUTFILE', 'OWNER', 'PACK_KEYS', 'PAGE', 'PARSER',
            'PARSE_GCOL_EXPR', 'PARTIAL', 'PARTITION', 'PARTITIONING',
            'PARTITIONS', 'PASSWORD', 'PHASE', 'PLUGIN', 'PLUGINS',
            'PLUGIN_DIR', 'POINT', 'POLYGON', 'PORT', 'PRECEDES',
            'PRECISION', 'PREPARE', 'PRESERVE', 'PREV', 'PRIMARY',
            'PRIVILEGES', 'PROCEDURE', 'PROCESSLIST', 'PROFILE',
            'PROFILES', 'PROXY', 'PURGE', 'QUARTER', 'QUERY',
            'QUICK', 'RANGE', 'READ', 'READS', 'READ_ONLY', 'READ_WRITE',
            'REAL', 'REBUILD', 'RECOVER', 'REDOFILE', 'REDO_BUFFER_SIZE',
            'REDUNDANT', 'REFERENCES', 'REGEXP', 'RELAY', 'RELAYLOG',
            'RELAY_LOG_FILE', 'RELAY_LOG_POS', 'RELAY_THREAD', 'RELEASE',
            'RELOAD', 'REMOVE', 'RENAME', 'REORGANIZE', 'REPAIR', 'REPEAT',
            'REPEATABLE', 'REPLACE', 'REPLICATE_DO_DB',
            'REPLICATE_DO_TABLE', 'REPLICATE_IGNORE_DB',
            'REPLICATE_IGNORE_TABLE', 'REPLICATE_REWRITE_DB',
            'REPLICATE_WILD_DO_TABLE', 'REPLICATE_WILD_IGNORE_TABLE',
            'REPLICATION', 'REQUIRE', 'RESET', 'RESIGNAL', 'RESTORE',
            'RESTRICT', 'RESUME', 'RETURN', 'RETURNED_SQLSTATE', 'RETURNS',
            'REVERSE', 'REVOKE', 'RIGHT', 'RLIKE', 'ROLLBACK', 'ROLLUP',
            'ROTATE', 'ROUTINE', 'ROW', 'ROWS', 'ROW_COUNT', 'ROW_FORMAT',
            'RTREE', 'SAVEPOINT', 'SCHEDULE', 'SCHEMA', 'SCHEMAS',
            'SCHEMA_NAME', 'SECOND', 'SECOND_MICROSECOND', 'SECURITY',
            'SELECT', 'SENSITIVE', 'SEPARATOR', 'SERIAL', 'SERIALIZABLE',
            'SERVER', 'SESSION', 'SET', 'SHARE', 'SHOW', 'SHUTDOWN',
            'SIGNAL', 'SIGNED', 'SIMPLE', 'SLAVE', 'SLOW', 'SMALLINT',
            'SNAPSHOT', 'SOCKET', 'SOME', 'SONAME', 'SOUNDS', 'SOURCE',
            'SPATIAL', 'SPECIFIC', 'SQL', 'SQLEXCEPTION', 'SQLSTATE',
            'SQLWARNING', 'SQL_AFTER_GTIDS', 'SQL_AFTER_MTS_GAPS',
            'SQL_BEFORE_GTIDS', 'SQL_BIG_RESULT', 'SQL_BUFFER_RESULT',
            'SQL_CACHE', 'SQL_CALC_FOUND_ROWS', 'SQL_NO_CACHE',
            'SQL_SMALL_RESULT', 'SQL_THREAD', 'SQL_TSI_DAY',
            'SQL_TSI_HOUR', 'SQL_TSI_MINUTE', 'SQL_TSI_MONTH',
            'SQL_TSI_QUARTER', 'SQL_TSI_SECOND', 'SQL_TSI_WEEK',
            'SQL_TSI_YEAR', 'SSL', 'STACKED', 'START', 'STARTING',
            'STARTS', 'STATS_AUTO_RECALC', 'STATS_PERSISTENT',
            'STATS_SAMPLE_PAGES', 'STATUS', 'STOP', 'STORAGE',
            'STORED', 'STRAIGHT_JOIN', 'STRING', 'SUBCLASS_ORIGIN',
            'SUBJECT', 'SUBPARTITION', 'SUBPARTITIONS', 'SUPER',
            'SUSPEND', 'SWAPS', 'SWITCHES', 'TABLE', 'TABLES',
            'TABLESPACE', 'TABLE_CHECKSUM', 'TABLE_NAME', 'TEMPORARY',
            'TEMPTABLE', 'TERMINATED', 'TEXT', 'THAN', 'THEN', 'TIME',
            'TIMESTAMP', 'TIMESTAMPADD', 'TIMESTAMPDIFF', 'TINYBLOB',
            'TINYINT', 'TINYTEXT', 'TO', 'TRAILING', 'TRANSACTION',
            'TRIGGER', 'TRIGGERS', 'TRUE', 'TRUNCATE', 'TYPE', 'TYPES',
            'UNCOMMITTED', 'UNDEFINED', 'UNDO', 'UNDOFILE',
            'UNDO_BUFFER_SIZE', 'UNICODE', 'UNINSTALL', 'UNION',
            'UNIQUE', 'UNKNOWN', 'UNLOCK', 'UNSIGNED', 'UNTIL', 'UPDATE',
            'UPGRADE', 'USAGE', 'USE', 'USER', 'USER_RESOURCES', 'USE_FRM',
            'USING', 'UTC_DATE', 'UTC_TIME', 'UTC_TIMESTAMP', 'VALIDATION',
            'VALUE', 'VALUES', 'VARBINARY', 'VARCHAR', 'VARCHARACTER',
            'VARIABLES', 'VARYING', 'VIEW', 'VIRTUAL', 'WAIT', 'WARNINGS',
            'WEEK', 'WEIGHT_STRING', 'WHEN', 'WHERE', 'WHILE', 'WITH',
            'WITHOUT', 'WORK', 'WRAPPER', 'WRITE', 'X509', 'XA', 'XID',
            'XML', 'XOR', 'YEAR', 'YEAR_MONTH']
