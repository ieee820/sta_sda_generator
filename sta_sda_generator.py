#! -*- coding:utf-8 -*-
import codecs
import os
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
import pandas as pd
import re

import time

import sta_sda_common as common
import numpy as np
import pymssql
import pymysql
import cx_Oracle

# 获取基本用户输入信息（接收用户输入参数）
def get_basic_information(excel_config_dir,district_suffix,current_layer):
    district = ''
    data_from = 'excel' # 默认为excel
    print('请输入抽取方式：' + ', '.join(extract_types))
    extract_type = input('抽取方式 = ')
    while extract_type.strip().lower() not in [s.lower() for s in extract_types]:
        common.reinput_hint(extract_types)
        extract_type = input('抽取方式 = ')
    print('请输入你想生成哪层代码：' + ', '.join(layers))
    current_layer = input('层次 = ')
    while current_layer.strip().lower() not in [s.lower() for s in layers]:
        common.reinput_hint(layers)
        current_layer = input('层次 = ')
    print('请输入要生成的项目名称：' + ', '.join(project_names))
    project_name = input('项目名称 = ')
    while project_name.strip().lower() not in [s.lower() for s in project_names]:
        common.reinput_hint(project_names)
        project_name = input('项目名称 = ')

    # 由于crm 在各区域都有数据，所以需要特殊处理
    if project_name.strip().lower() == 'crm' and current_layer.strip().lower() == layer_sta:
        print('请输入地区：' + ', '.join(districts))
        district = input('地区 = ')
        while district.lower().strip() not in [s.lower() for s in districts]:
            common.reinput_hint(districts)
            district = input('地区 = ')
        district_suffix = '_'+district

    extract_folder_name = extract_type
    if extract_folder_name == 'increment':
        extract_folder_name = '' # 增量抽取不需要创建increment 这样的根目录
    if not os.path.exists(root + os.sep + extract_folder_name):
        common.make_server_folders(extract_folder_name,layers,project_names)

    '''
    目前元数据主要来源excel，所以这里暂时不需要对来源数据库的做处理。有需要时，再添加处理逻辑
    '''
    # 获取数据来源文件名
    project_connect_name = project_name + district_suffix
    excel_file_name = excel_config_dir + os.sep + project_connect_name + xlsx_suffix # excel文件名
    if project_name.strip().lower() == 'crm' and current_layer.strip().lower() == layer_sda:
        excel_file_name = excel_config_dir + os.sep + project_connect_name + '_cd' + xlsx_suffix

    return [excel_file_name,project_name,project_connect_name,district_suffix,current_layer,data_from,extract_type]


# 从EXCEL中获取需要抽取的表信息
def get_table_info_from_excel(excel_file_name):
    # 从第二个sheet获取表的相关信息
    project_def = pd.read_excel(excel_file_name, sheet_name=2)
    # 分别取D列（物理表名）、F列（主键字段）、G例（索引字段）、P列（增量识别条件）、S列（是否接入数仓）、U列（STA近源表类型）、W列（加载方式）
    project_def_tables = project_def.iloc[:,[3,5,6,15,18,20,22]]
    # 表名
    table_names = np.array(project_def_tables[project_def_tables.iloc[:,4]=='Y'].iloc[:,0])
    table_names = np.array([str(name).strip() for name in list(table_names)])
    # 主键
    primary_keys = np.array(project_def_tables[project_def_tables.iloc[:,4]=='Y'].iloc[:,1])
    # 索引
    indexes = np.array(project_def_tables[project_def_tables.iloc[:,4]=='Y'].iloc[:,2])
    # 增量识别条件是否为空
    conditions = np.array(project_def_tables[project_def_tables.iloc[:, 4] == 'Y'].iloc[:, 3])
    # STA近源表类型
    table_types = np.array(project_def_tables[project_def_tables.iloc[:, 4] == 'Y'].iloc[:, 5])
    # 抽取方式
    insert_types = np.array(project_def_tables[project_def_tables.iloc[:, 4] == 'Y'].iloc[:, 6])

    # 从第三个sheet获取字段的相关信息
    fields_df = pd.read_excel(excel_file_name, sheet_name=3)
    # 分别取C列（英文表名）、D列（中文字段名）、E列（英文字段名）、F列（字段类型）、H列（是否可为空）
    fields_df_useful = fields_df.iloc[:,[2,3,4,5,7]]

    return [table_names,primary_keys,indexes,conditions,table_types,insert_types,fields_df_useful]

# 生成SQL脚本 及 ETL XML 文件
def generate_sql_xml_files(project_connect_name
                           ,table_names
                           ,table_types
                           ,insert_types
                           ,primary_keys
                           ,indexes
                           ,conditions
                           ,fields_df_useful
                           ,current_layer
                           ,extract_type):
    result_sql_file = current_layer + underline + project_name + table_create_sql_str
    name_stripped_tables = np.array([str(table).strip() for table in (fields_df_useful.iloc[:,0])]) #第三个sheet 中表名

    # 如果元数据信息来源于数据库
    if data_from == 'database':
        connection = common.get_connection_informations(project_connect_name)
        # 针对不同的数据库使用不用的库连接
        if project_connect_name in ('ehr', 'fin', 'dmk'):
            db = pymssql.connect(host = connection[0],
                                 port = connection[1],
                                 user = connection[2],
                                 password = connection[3],
                                 db = connection[4],
                                 charset = 'utf8')
        elif project_connect_name in ('anshuo'):
            db = cx_Oracle.connect(connection[2]
                                   ,connection[3]
                                   ,connection[0]+':'+connection[1]+'/'+connection[4])
        else:
            db = pymysql.connect(host = connection[0],
                                 port = connection[1],
                                 user = connection[2],
                                 password = connection[3],
                                 db = connection[4],
                                 charset = 'utf8')
    # 目前元数据来源于excel
    for table_name in table_names:
        if data_from == 'excel':
            useful_comments = np.array(fields_df_useful[name_stripped_tables == table_name].iloc[:,1])
            useful_fields = np.array(fields_df_useful[name_stripped_tables == table_name].iloc[:,2])
            useful_types = np.array(fields_df_useful[name_stripped_tables == table_name].iloc[:, 3])
            useful_attrs = np.array(fields_df_useful[name_stripped_tables == table_name].iloc[:, 4])
        elif data_from == 'database':
            cursor = db.cursor()
            if project_connect_name in ('ehr', 'fin', 'dmk'):
                cursor.execute('')
            elif project_connect_name in ('anshuo'):
                cursor.execute('')
            else:
                cursor.execute('select column_name,column_type,column_comment,is_nullable ' +
                               'from information_schema.columns' +
                               'where table_name = "' + table_name +
                               '" and table_schema = "' + connection[4] + '"')
            results = cursor.fetchall()
            useful_fields = np.array([results[i][0] for i in range(len(results))])
            useful_types = np.array([results[i][1] for i in range(len(results))])
            useful_comments = np.array([results[i][2] for i in range(len(results))])
            useful_attrs= np.array([results[i][3][0] for i in range(len(results))])
        else:
            print("元数据来源不对！！！")
            return

        primary_key = str(primary_keys[list(table_names).index(table_name)])
        idx = str(indexes[list(table_names).index(table_name)]) # 所有表索引
        primary_key = '' if primary_key == 'nan' else primary_key # 所有表主键
        useful_fields = np.array(common.add_apostrophe(useful_fields)) # 所有表字段
        useful_types = np.array([re.sub('double|float','decimal',str(useful_type)) for useful_type in useful_types]) # 将其它库double|float替换成mysql decimal
        useful_types = np.array([str(useful_type) for useful_type in list(useful_types)]) # 所有表字段类型

        # 财务和EHR有些特殊地方需要处理
        if project_connect_name in ('fin', 'ehr', 'dmk'):
            # 将uniqueidentifier 数据类型转为char(50)
            useful_types = np.array([re.sub('uniqueidentifier','char(50)',str(useful_type)) for useful_type in useful_types])
            # 将varchar或者nvachar字段长度大于1000的数据类型转为text
            useful_types = np.array([re.sub('varchar\(\d{4,}\)|nvarchar\(\d{4,}\)','text',str(useful_type)) for useful_type in useful_types])
            # 将varchar或者nvachar字段长度大于Int最大值的数据类型转为blob
            useful_types = np.array(
                [re.sub('varchar\(-1\)|nvarchar\(-1\)', 'blob', str(useful_type)) for useful_type in
                 useful_types])

        if len(useful_fields) >0 and len(useful_attrs) >0:
            table_suffix = ''
            if current_layer == layer_sta:
                dest_file = current_folder_path + os.path.sep + current_layer + underline + project_name + underline + table_name + district_suffix + ktr_suffix
                if project_connect_name in ('fin','ehr','dmk'):
                    useful_fields = np.array(common.get_rid_of_apostrophe(useful_fields))
                generate_sta_xmldoc(project_connect_name,table_name,useful_fields,useful_types,
                                    sta_source_file,dest_file,extract_type)
            elif current_layer == layer_sda:
                dest_file = current_folder_path + os.path.sep + current_layer + underline + project_name + underline + table_name
                insert_type = str(insert_types[list(table_names).index(table_name)])
                table_type = str(table_types[list(table_names).index(table_name)])
                if table_type == '历史表' or table_type == '拉链表':
                    table_suffix = '_h'
                elif table_type == '流水表':
                    table_suffix = '_a'
                elif table_type == '切片表':
                    table_suffix = '_w'
                else:
                    pass
                generate_sda_xml_files(table_name,useful_fields,useful_types,
                                       dest_file,insert_type,table_suffix,
                                       primary_key,idx,extract_type)
            else:
                print('no such layer')
        else:
            print('Error!' + table_name + 'has no fields or attributes!')

        useful_fields = np.array(common.add_apostrophe(useful_fields))
        generate_table_create_sql(table_name,table_suffix,useful_fields,
                                  useful_types,useful_comments,useful_attrs,
                                  primary_key,idx)
    if data_from == 'database':
        cursor.close()
        db.close()
    common.move_sql_file_to_related_folder(current_folder_path,result_sql_file)


# 生成sta程序
def generate_sta_xmldoc(project_connect_name
                        ,table_name
                        ,useful_fields
                        ,useful_types
                        ,sta_source_file
                        ,dest_file
                        ,extract_type):
    xmldoc, sta_table_name = change_sta_attributes(project_connect_name,table_name,sta_source_file)
    xmldoc = change_sta_connection_info(xmldoc)
    xmldoc = generate_sta_sql_str(xmldoc,useful_fields,useful_types,table_names,table_name,extract_type)
    xmldoc = generate_sta_table_fields(xmldoc,useful_fields)
    xmldoc = generate_sta_log_message(xmldoc,table_name,sta_table_name)
    common.append_xml_header(dest_file,xmldoc)


# 生成sda程序
def generate_sda_xml_files(table_name
                           ,useful_fields
                           ,useful_types
                           ,dest_file
                           ,insert_type
                           ,table_suffix
                           ,primary_key
                           ,idx
                           ,extract_type):
    if insert_type == '直接追加':
        dest_file += '_1' + kjb_suffix
        strategy = '[01]'
        generate_sda_xmldoc(table_name,sda_source_file1,useful_fields,
                            useful_types,insert_type,dest_file,strategy,
                            table_suffix,primary_key,idx,extract_type)
    elif insert_type == '主键拉链' or insert_type == '增量拉链':
        dest_file += '_2' + kjb_suffix
        strategy = '[02]'
        generate_sda_xmldoc(table_name,sda_source_file2,useful_fields,
                            useful_types,insert_type,dest_file,strategy,
                            table_suffix,primary_key,idx,extract_type)
    elif insert_type == '全表拉链' or insert_type == '全量拉链':
        dest_file += '_3' + kjb_suffix
        strategy = '[03]'
        generate_sda_xmldoc(table_name,sda_source_file3,useful_fields,
                            useful_types,insert_type,dest_file,strategy,
                            table_suffix,primary_key,idx,extract_type)
    elif insert_type == '全表覆盖':
        dest_file += '_4' + kjb_suffix
        strategy = '[04]'
        generate_sda_xmldoc(table_name,sda_source_file4,useful_fields,
                            useful_types,insert_type,dest_file,strategy,
                            table_suffix,primary_key,idx,extract_type)
    elif insert_type == '增量覆盖':
        dest_file += '_5' + kjb_suffix
        strategy = '[05]'
        generate_sda_xmldoc(table_name,sda_source_file5,useful_fields,
                            useful_types,insert_type,dest_file,strategy,
                            table_suffix,primary_key,idx,extract_type)
    else:
        print(table_name + '数据加载方式无法识别!!!')
        pass

# 生成建表语句
def generate_table_create_sql(table_name
                              ,table_suffix
                              ,useful_fields
                              ,useful_types
                              ,useful_comments
                              ,useful_attrs
                              ,primary_key
                              ,idx):
    sql_file_name = current_folder_path + os.path.sep + table_name + sql_suffix
    district = district_suffix if current_layer == layer_sta else ''
    table_to_create = current_layer + underline + \
        project_name + underline + table_name + district + table_suffix
    with codecs.open(sql_file_name, 'w', encoding='utf-8') as f:
        create_table_str = 'DROP TABLE IF EXISTS ' + table_to_create + \
            ';\nCREATE TABLE ' + table_to_create + ' (\n'
        create_table_str += '    op_time INT,\n'
        if current_layer == layer_sda and table_suffix == '_h':
            create_table_str += '    sda_end_date INT,\n'
        if current_layer == layer_sda and project_name == 'crm':
            create_table_str += '    system_id char(5) COMMENT \'系统来源\',\n'
        comment = ''
        for i in range(len(useful_fields)):
            comment = ',' if useful_comments[i] is np.nan else ' COMMENT ' + '\'' + str(useful_comments[i]) + '\'' + ','
            if i == len(useful_fields) - 1:
                comment = comment[:-1] + ''
            can_be_null = ' NULL' if useful_attrs[i] == 'Y' else ' NOT NULL'
            create_table_str += '    ' + \
                str(useful_fields[i]) + ' ' + \
                str(useful_types[i]) + can_be_null + comment + '\n'
        create_table_str += ') ENGINE=InnoDB DEFAULT CHARSET=utf8;\n\n'
        f.write(create_table_str)

# 生成sda模板xml文件
def generate_sda_xmldoc(table_name
                        ,source_file
                        ,useful_fields
                        ,useful_types
                        ,insert_type
                        ,dest_file
                        ,strategy
                        ,table_suffix
                        ,primary_key
                        ,idx
                        ,extract_type):
    xmldoc, sda_table_name,sda_table_out_name,sta_table_in_name = \
        change_sda_basic_info(table_name,source_file,table_suffix)
    xmldoc = generate_sda_sql_str(xmldoc,useful_fields,useful_types,
                                  sda_table_out_name,sta_table_in_name,
                                  insert_type,primary_key,idx,extract_type)
    xmldoc = generate_sda_log_message(xmldoc,sda_table_name,sda_table_out_name,sta_table_in_name,strategy,insert_type)
    common.append_xml_header(dest_file,xmldoc)

# 修改sda模板中任务名，创建时间，修改时间等基本信息
def change_sda_basic_info(table_name,source_file,table_suffix):
    xmldoc = ElementTree.parse(source_file)
    job_name = xmldoc.find('name')
    created_date = xmldoc.find('created_date')
    modified_date = xmldoc.find('modified_date')
    sda_table_name = current_layer + underline + project_name + underline + table_name + table_suffix
    sda_table_out_name = sda_table_name
    sta_table_in_name = layer_sta + underline + project_name + underline + table_name + district_suffix
    job_name.text = sda_table_name
    created_date.text = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
    modified_date.text = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
    return xmldoc,sda_table_name,sda_table_out_name,sta_table_in_name

# 生成sda模板中SQL字符串
def generate_sda_sql_str(xmldoc,useful_fields,useful_types,
                                  sda_table_out_name,sta_table_in_name,
                                  insert_type,primary_key,idx,extract_type):
    # 由于crm有三个地区，所以sta源表需要处理
    if project_name == 'crm':
        sta_table_in_name_cd = sta_table_in_name + underline + sys_from_cd
        sta_table_in_name_bj = sta_table_in_name + underline + sys_from_bj
        sta_table_in_name_wh = sta_table_in_name + underline + sys_from_wh
        sta_tables_in = [sta_table_in_name_cd, sta_table_in_name_bj,sta_table_in_name_wh]
        systems = [sys_from_cd, sys_from_bj, sys_from_wh]
    else:
        sta_tables_in = [sta_table_in_name]
        systems = []

    if str(extract_type).lower() == 'alldata':
        if project_name == 'crm':
            [sql_str1, sql_str2, sql_str3] = \
                common.insert_all_table_data(sda_table_out_name,
                                             insert_type,
                                             sta_tables_in,
                                             useful_fields,
                                             useful_types,
                                             '',
                                             sys_from=systems,
                                             prj_name='crm')
            sqls = xmldoc.findall('entries/entry/sql')
            sqls[0].text = sql_str1
            sqls[1].text = sql_str2
            sqls[2].text = sql_str3
        else:
            sql_str = common.insert_all_table_data(sda_table_out_name,
                                                   insert_type,
                                                   [sta_table_in_name],
                                                   useful_fields,
                                                   useful_types,
                                                   '',
                                                   sys_from=[],
                                                   prj_name=project_name)[0]
            sql = xmldoc.find('entries/entry/sql')
            sql.text = sql_str
    else:
        if insert_type == '直接追加':
            generate_sda_sql_type1_str(xmldoc, useful_fields, useful_types,
                                       sda_table_out_name, sta_tables_in,
                                       systems, primary_key, idx)
        elif insert_type == '增量拉链' or insert_type == '主键拉链':
            generate_sda_sql_type2_str(xmldoc, useful_fields, useful_types,
                                       sda_table_out_name, sta_tables_in,
                                       systems, primary_key, idx)
        elif insert_type == '全量拉链' or insert_type == '全表拉链':
            generate_sda_sql_type3_str(xmldoc, useful_fields, useful_types,
                                       sda_table_out_name, sta_tables_in,
                                       systems, primary_key, idx)
        elif insert_type == '全表覆盖':
            generate_sda_sql_type4_str(xmldoc, useful_fields, useful_types,
                                       sda_table_out_name, sta_table_in_name,
                                       sta_tables_in, systems,
                                       primary_key, idx)
        elif insert_type == '增量覆盖':
            generate_sda_sql_type5_str(xmldoc, useful_fields, useful_types,
                                       sda_table_out_name, sta_table_in_name,
                                       sta_tables_in, systems,
                                       primary_key, idx)
        else:
            pass
    return xmldoc

# 生成直接追加数据SQL
def generate_sda_sql_type1_str(xmldoc
                               ,useful_fields
                               ,useful_types
                               ,sda_table_out_name
                               ,sta_tables_in
                               ,systems
                               ,primary_key
                               ,idx):
    sql_strs = []
    sql_strs_tmp = common.insert_type1_data(
        sda_table_out_name,
        sta_tables_in,
        useful_fields,
        useful_types,
        sys_from=systems,
        prj_name=project_name)
    sql_strs += sql_strs_tmp
    sql_str = 'DELETE\nFROM\n  ' + sda_table_out_name + \
        '\nWHERE op_time = ${op_time};\n'
    sql_strs += [sql_str]
    sqls = xmldoc.findall('entries/entry/sql')
    for i in range(len(sql_strs)):
        sqls[i].text = sql_strs[i]
    return xmldoc

# 生成增量拉链 SQL
def generate_sda_sql_type2_str(xmldoc
                               ,useful_fields
                               ,useful_types
                               ,sda_table_out_name
                               ,sta_tables_in
                               ,systems
                               ,primary_key
                               ,idx):
    sda_table_out_ins_name = sda_table_out_name + '_ins'
    sda_table_out_upd_name = sda_table_out_name + '_upd'
    sda_table_out_del_name = sda_table_out_name + '_del'

    sqls = xmldoc.findall('entries/entry/sql')

    sql_strs = []

    # STEP1
    sql_str1 = common.insert_type2_step1_data(
        sda_table_out_ins_name,
        sda_table_out_name,
        sta_tables_in,
        useful_fields,
        useful_types,
        primary_key,
        idx,
        systems,
        project_name
    )

    # STEP2
    sql_str2 = common.insert_type2_step2_data(
        sda_table_out_upd_name,
        sda_table_out_name,
        sta_tables_in,
        useful_fields,
        useful_types,
        primary_key,
        idx,
        systems,
        project_name
    )

    # STEP3
    sql_str3 = sqls[2].text
    sql_str3 = common.insert_type2_step3_data(
        sql_str3,
        sda_table_out_name,
        sta_tables_in,
        primary_key,
        idx
    )

    # STEP4
    sql_str4 = sqls[3].text
    sql_str4 = common.insert_type2_step4_data(
        sql_str4,
        sda_table_out_name
    )

    # STEP5
    sql_str5 = common.insert_type2_step5_data(
        useful_fields,
        sda_table_out_name,
        sda_table_out_ins_name,
        sda_table_out_del_name,
        sda_table_out_upd_name,
        primary_key,
        idx,
        project_name
    )
    sql_strs += [sql_str1] + [sql_str2] + [sql_str3] + [sql_str4] + [sql_str5]
    for i in range(len(sql_strs)):
        sqls[i].text = sql_strs[i]
    return xmldoc

# 生成全量拉链 SQL
def generate_sda_sql_type3_str(xmldoc
                               ,useful_fields
                               ,useful_types
                               ,sda_table_out_name
                               ,sta_tables_in
                               ,systems
                               ,primary_key
                               ,idx):
    xmldoc = generate_sda_sql_type2_str(xmldoc,
                                        useful_fields,
                                        useful_types,
                                        sda_table_out_name,
                                        sta_tables_in,
                                        systems,
                                        primary_key,
                                        idx)
    sqls = xmldoc.findall('entries/entry/sql')
    # STEP3
    sql_str = sqls[2].text
    sql_str = sql_str.replace('0 = 1', '1 = 1') # 增量不能识别删除，全量可识别删除
    sqls[2].text = sql_str
    return xmldoc

# 生成全表覆盖SQL
def generate_sda_sql_type4_str(xmldoc
                               ,useful_fields
                               ,useful_types
                               ,sda_table_out_name
                               ,sta_table_in_name
                               ,sta_tables_in
                               ,systems
                               ,primary_key
                               ,idx):
    sql_strs = []
    sql_str1 = 'TRUNCATE\nTABLE\n  ' + sda_table_out_name + ';\n'
    sql_strs += [sql_str1]
    if project_name == 'crm':
        sql_strs_tmp = common.insert_type4_data(
            sda_table_out_name,
            sta_tables_in,
            useful_fields,
            useful_types,
            '',
            sys_from=systems,
            prj_name='crm')
    else:
        sql_strs_tmp = common.insert_type4_data(
            sda_table_out_name,
            [sta_table_in_name],
            useful_fields,
            useful_types,
            '',
            sys_from=[],
            prj_name=project_name)[0]
    for strs in sql_strs_tmp:
        sql_strs.append(strs)
    sqls = xmldoc.findall('entries/entry/sql')
    for i in range(len(sql_strs)):
        sqls[i].text = sql_strs[i]
    return xmldoc

# 生成增量覆盖SQL
def generate_sda_sql_type5_str(xmldoc
                               ,useful_fields
                               ,useful_types
                               ,sda_table_out_name
                               ,sta_table_in_name
                               ,primary_key
                               ,idx):
    sql_str1 = 'DELETE\nFROM\n  ' + sda_table_out_name + '\nWHERE op_time >= ${op_time};\n'
    sql_str2 = 'INSERT INTO ' + sda_table_out_name +  '\nSELECT\n  a.op_time,\n'
    sql_str2 = common.sql_str_add_fields(sql_str2, useful_fields, useful_types)
    sql_str2 += 'FROM\n  ' + sta_table_in_name + ' a\n  LEFT JOIN ' + \
        sda_table_out_name + ' b\n   ON '
    sql_str2, any_key_indexes_a, any_key_indexes_b, length = \
        common.get_useful_key_index(primary_key, idx, sql_str2)
    sql_str2 += 'WHERE a.op_time = ${op_time}\n  AND '
    for i in range(length - 1):
        sql_str2 += '    ' + any_key_indexes_b[i] + ' IS NULL OR\n'
    sql_str2 += '    ' + any_key_indexes_b[length - 1] + ' IS NULL;\n'

    sql_str3 = 'UPDATE\n  ' + sda_table_out_name + ' a,\n  ' + \
        sta_table_in_name + ' b\nSET\n'
    sql_str3 = common.sql_str_add_fields(sql_str3, useful_fields, useful_types,
                                         table_alias1='b.',
                                         oper_type='JUDGE',
                                         comp_type='=',
                                         keys=primary_key,
                                         indexes=idx,
                                         condition='AND')
    sql_str3 += 'WHERE '
    sql_str3, any_key_indexes_a, any_key_indexes_b, length = \
        common.get_useful_key_index(primary_key, idx, sql_str3)
    sql_str3 += '    AND b.op_time = ${op_time}  AND\n'
    sql_str3 = common.sql_str_add_fields(sql_str3, useful_fields, useful_types,
                                         table_alias1='b.',
                                         oper_type='JUDGE',
                                         comp_type='<>',
                                         keys=primary_key,
                                         indexes=idx,
                                         condition='AND',
                                         end_str='')
    sql_str3 += ';'
    sqls = xmldoc.findall('entries/entry/sql')
    sqls[0].text = sql_str1
    sqls[1].text = sql_str2
    sqls[2].text = sql_str3
    return xmldoc

# 修改sta模板文件中属性值
def change_sta_attributes(project_connect_name,
                          table_name,
                          sta_source_file):
    xmldoc = ElementTree.parse(sta_source_file)
    name = xmldoc.find('info/name')
    to = xmldoc.find('order/hop/to')
    names = xmldoc.findall('step/name')
    table = xmldoc.find('step/table')
    hop_from = xmldoc.find('order/hop/from')
    hop_to = xmldoc.findall('order/hop/to')
    commit = xmldoc.find('step/commit')
    lookup = xmldoc.find('step/lookup')
    hop_from.text = project_name + '.' + table_name
    sta_table_name = current_layer + underline + project_name + underline + table_name + district_suffix

    hop_to[0].text = sta_table_name
    hop_to[2].text = hop_from.text
    name.text = sta_table_name
    to.text = sta_table_name
    names[0].text = hop_from.text
    names[1].text = sta_table_name
    table.text = sta_table_name
    commit.text = '10000'
    lookup.text = ''
    if project_connect_name in ('ehr','fin','dmk'):
        xmldoc = change_attributes_for_mssql(xmldoc)

    return xmldoc,sta_table_name

# 修改sta模板文件中属性值（当数据库是 mssql 时）
def change_attributes_for_mssql(xmldoc):
    indent0,indent1,indent2 = common.get_indents()
    con_types = xmldoc.findall('connection/type')
    con_attr_codes = xmldoc.findall('connection/attributes/attribute/code')
    con_attr_attributes = xmldoc.findall('connection/attributes/attribute/attribute')
    con_types[0].text = 'MSSQLNATIVE'
    con_attr_codes[0].text = 'EXTRA_OPTION_MSSQLNATIVE.instance'
    con_attr_attributes[0].text = '${'+project_connect_name+'_database_model}'

    attributes = xmldoc.findall('connection/attributes')
    attribute_1 = Element('attribute')
    attr_code_1 = Element('code')
    attr_attr_1 = Element('attribute')
    attr_code_1.text = 'MSSQLUseIntegratedSecurity'
    attr_attr_1.text = 'false'
    attribute_2 = Element('attribute')
    attr_code_2 = Element('code')
    attr_attr_2 = Element('attribute')
    attr_code_2.text = 'MSSQL_DOUBLE_DECIMAL_SEPARATOR'
    attr_attr_2.text = 'N'

    attribute_1.append(attr_code_1)
    attribute_1.append(attr_attr_1)
    attribute_2.append(attr_code_2)
    attribute_2.append(attr_attr_2)

    for child in attributes[0].findall('attribute'):
        if child[0].text == 'STREAM_RESULTS':
            attributes[0].remove(child)


    attributes[0][9].tail = indent2
    attr_code_1.tail = indent2
    attr_attr_1.tail = indent1
    attr_code_2.tail = indent2
    attr_attr_2.tail = indent1
    attribute_1.tail = indent1
    attribute_1.text = indent2
    attribute_2.tail = indent0
    attribute_2.text = indent2
    attributes[0].append(attribute_1)
    attributes[0].append(attribute_2)

    return xmldoc

# 修改sta模板中数据库连接信息
def change_sta_connection_info(xmldoc):
    connect_name = xmldoc.find('connection/name')
    connect_server = xmldoc.find('connection/server')
    connect_database = xmldoc.find('connection/database')
    connect_port = xmldoc.find('connection/port')
    connection_username = xmldoc.find('connection/username')
    connection_passwd = xmldoc.find('connection/password')
    connetion_attribute = xmldoc.findall('connection/attributes/attribute/attribute')[4]
    connetion_infos = xmldoc.findall('step/connection') # 获取每个步骤的数据库连接

    connect_name.text = 'conn_' + project_connect_name
    connect_server.text = '${' + project_connect_name + '_database_ip}'
    connect_database.text = '${' + project_connect_name + '_database_name}'
    connect_port.text = '${' + project_connect_name + '_database_port}'
    connection_username.text = '${' + project_connect_name + '_username}'
    connection_passwd.text = '${' + project_connect_name + '_passwd}'
    connetion_attribute.text = connect_port.text
    connetion_infos[0].text = connect_name.text

    return xmldoc


# 生成sta模板中表输入中sql字符串
def generate_sta_sql_str(xmldoc
                         ,useful_fields
                         ,useful_types
                         ,table_names
                         ,table_name
                         ,extract_type):
    sql_str = "select \n '${op_time}' as op_time,\n"
    # 生成sql脚本中间字段信息
    sql_str = common.sql_str_add_fields(sql_str,useful_fields,useful_types
                                        ,layer=layer_sta
                                        ,table_alias = ''
                                        ,table_alias1= ''
                                        ,oper_type= 'SELECT')
    sql_str += 'FROM ' + table_name + '\n'
    sql_str += 'WHERE\n'

    # 全量抽取(生成where条件)
    if str(extract_type).lower() == 'alldata':
        condition = str(conditions[list(table_names).index(table_name)])
        if condition.lower() == 'nan': # 如果没有增量识别条件
            sql_str += ' 1=1;'
        else:
            pattern = '(\w+)=?.*\n?(\w+)?' # 增量识别条件通过换行识别多个
            match_result = re.search(pattern,condition,re.M|re.I)
            if match_result:
                match_condtion_numbers = len(match_result.group().split('\n'))
                if project_connect_name not in ('fin','ehr','dmk'):
                    if match_condtion_numbers >0:
                        used_time1 = match_result.group(1)
                        # 在此根据源库中日期字段类型可能需要做单独处理
                        if project_name == 'crm':
                            if table_name == 'leads': # crm leads 表单独处理
                                used_time1 = 'FROM_UNIXTIME('+ used_time1 +')'
                        sql_str += "DATA_FORMAT(" + used_time1 + ", '%Y%m%d') <= '${op_time}';\n"
                        if match_condtion_numbers == 2:
                            used_time2 = match_result.group(2)
                            if project_name == 'crm':
                                if table_name == 'leads':
                                    used_time2 = 'FROM_UNIXTIME(' + used_time2 + ')'
                            sql_str = sql_str[:-2]
                            sql_str += '\nOR\n '
                            if project_name in('crm','car','zx_cis','zx_300','zx_getway','fk'):
                                sql_str += "DATE_FORMAT(" + used_time2 + \
                                           ", '%Y%m%d') <= '${op_time}';"
                else:
                    if match_condtion_numbers > 0:
                        sql_str += "DATE_FORMAT(" + match_result.group(1) + \
                                   ", '%Y%m%d') <= '${op_time}';\n"
                        if match_condtion_numbers == 2:
                            sql_str = sql_str[:-2] + sql_str[-1]
                            sql_str += '\nOR\n'
                            sql_str += "DATE_FORMAT(" + match_result.group(2) + \
                                       ", '%Y%m%d') <= '${op_time}';\n"
            else:
                sql_str += ' 1 = 1;'
    # 增量抽取(生成where条件)
    elif str(extract_type).strip().lower() == 'increment':
        condition = str(conditions[list(table_names).index(table_name)])
        if condition.lower() == 'nan':
            sql_str += ' 1 = 1;'
        else:
            pattern = '(\w+)=?.*\n?(\w+)?'
            match_result = re.search(pattern, condition, re.M | re.I)
            if match_result:
                match_condtion_numbers = len(match_result.group().split('\n'))
                if project_connect_name not in ('fin', 'ehr', 'dmk'):
                    if match_condtion_numbers > 0:
                        used_time1 = match_result.group(1)
                        # 在此根据源库中日期字段类型可能需要做单独处理
                        if project_name == 'crm':
                            if table_name == 'leads':
                                used_time1 = 'FROM_UNIXTIME(' + used_time1 + ')'
                            sql_str += "DATE_FORMAT(" + used_time1 +  ", '%Y-%m-%d %H:%i:%S') BETWEEN " + \
                                "DATE_ADD(STR_TO_DATE('${op_time}', '%Y%m%d'), INTERVAL - " + "8 HOUR) " \
                                "AND DATE_ADD(STR_TO_DATE('${op_time}', '%Y%m%d'), INTERVAL " + \
                                "'15:59:59' HOUR_SECOND);\n"
                        else:
                            sql_str += "DATE_FORMAT(" + used_time1 + ", '%Y%m%d') BETWEEN DATE_FORMAT(" + \
                                "'${op_time}', '%Y%m%d')\n\t\tAND " + \
                                "DATE_FORMAT('${op_time}" + \
                                "235959', '%Y%m%d%H%i%S');\n"

                        if match_condtion_numbers == 2:
                            used_time2 = match_result.group(2)
                            if table_name == 'leads':
                                used_time2 = 'FROM_UNIXTIME(' + used_time2 + ')'
                            sql_str = sql_str[:-2]
                            sql_str += '\nOR\n '
                            if project_name == 'crm':
                                sql_str += "DATE_FORMAT(" + used_time2 + ", '%Y-%m-%d %H:%i:%S') BETWEEN " + \
                                           "DATE_ADD(STR_TO_DATE('${op_time}', '%Y%m%d'), INTERVAL - 8 HOUR ) " \
                                           "AND DATE_ADD(STR_TO_DATE('${op_time}', '%Y%m%d'), INTERVAL " + \
                                           "'15:59:59' HOUR_SECOND);"
                            else:
                                sql_str += "DATE_FORMAT(" + used_time2 + ", '%Y%m%d') BETWEEN DATE_FORMAT('" + \
                                           "${op_time}', '%Y%m%d')\n\t\tAND DATE_FORMAT('${op_time}" + \
                                           "235959', '%Y%m%d%H%i%S');\n"
                else:
                    if match_condtion_numbers > 0:
                        sql_str += match_result.group(1) + " BETWEEN " + \
                                    "'${op_time}' AND CONVERT(varchar(100), " + \
                                    "'${op_time}', 23) + ' 23:59:59';\n"
                        if match_condtion_numbers == 2:
                            sql_str = sql_str[:-2] + sql_str[-1]
                            sql_str += '\nOR\n'
                            sql_str += ' ' + match_result.group(2) + \
                                        " BETWEEN '${op_time}' and CONVERT" + \
                                        "(varchar(100), '${op_time}', 23) + ' " + \
                                        "23:59:59';\n"
            else:
                sql_str += ' 1 = 1;'
    else:
        print("No such extract type")
    sql = xmldoc.find('step/sql')
    sql.text = sql_str
    return xmldoc

# 生成sta模板中表输出步骤字段映射
def generate_sta_table_fields(xmldoc, useful_fields):
    indent0, indent1, indent2 = common.get_indents()
    fields = xmldoc.find('step/fields')
    len_fields = len(useful_fields)
    for child in fields.findall('field'):
        if child.tag == 'field':
            fields.remove(child)

    for i in range(len_fields +1):
        field = Element('field')
        column = Element('column_name')
        stream = Element('stream_name')
        field.text = indent2
        column.tail = indent2
        stream.tail = indent1
        field.tail = indent1

        if i == 0:
            column.text = 'op_time'
            stream.text = 'op_time'
        else:
            if useful_fields[i-1].upper() in common.get_mysql_keywords() and common.apostrophe not in useful_fields[i-1]:
                useful_fields[i-1] = common.apostrophe + useful_fields[i-1] + common.apostrophe
            column.text = useful_fields[i-1]
            if common.apostrophe in useful_fields[i-1]:
                stream.text = useful_fields[i-1][1:-1]
            else:
                stream.text = useful_fields[i-1]
        field.append(column)
        field.append(stream)
        fields.append(field)

        all_fields = xmldoc.findall('step/fields/field')
        all_fields[len(all_fields)-2].tail = indent0
    return xmldoc

# 生成sta模板中日志版本信息
def generate_sta_log_message(xmldoc,table_name,sta_table_name):
    log_message = xmldoc.find('step/logmessage')
    create_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    log_message.text = '#***********************************************' + \
        '************************************\n# MoerLong EDW ETL script' + \
        ' -- Coding By Kettle\n# 任 务 名: ' + sta_table_name + '\n# 目 标 表: ' + \
        sta_table_name + '  类型一sta层输出表\n# 源 表 名: ' + table_name + \
        '  类型一sta层输入表\n# 加载策略: [03] (注,01:直接追加 02:增量拉链,' + \
        '03:全表拉链,04:全表覆盖,05:增量覆盖)\n# 运行周期: [1] (注, 1:天,' + \
        '2:星期,3:旬,4:月,5:季,6:半年,7:年，00:其他)\n# 脚本功能: 实现' + \
        project_name + '系统' + table_name + '表的sta层处理\n# 创建时间: ' + \
        create_date + '\n# 作    者: 彭德剑 pdj408@163.com\n# 修改记录: 1、\n#\n#************' + \
        '*****************************************************************' + \
        '******\n'
    return xmldoc

# 生成sda模板中日志版本信息
def generate_sda_log_message(
        xmldoc,
        sda_table_name,
        sda_table_out_name,
        sta_table_in_name,
        strategy,
        insert_type):
    log_message = xmldoc.find('entries/entry/logmessage')
    create_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    choose_insert_type = insert_type
    if choose_insert_type == '增量拉链' or choose_insert_type == '主键拉链':
        choose_insert_type = '增量拉链'
    elif choose_insert_type == '全量拉链' or choose_insert_type == '全表拉链':
        choose_insert_type = '全表拉链'
    else:
        pass

    log_message.text = '#************************************************' + \
        '***********************************\n# MoerLong EDW ETL ' + \
        'script -- Coding By Kettle\n# 任 务 名: ' + sda_table_name + \
        '\n# 目 标 表: ' + sda_table_out_name + '  类型一sda层输出表\n# 源 表 名: '
    if project_name == 'crm':
        log_message.text += sta_table_in_name[:-3] + 'cd, bj, wh'
    else:
        log_message.text += sta_table_in_name
    log_message.text += '  类型一sta层输入表\n# 加载策略: ' + strategy
    if project_name == 'crm':
        log_message.text += sta_table_in_name[:-3] + 'cd, bj, wh'
    else:
        log_message.text += sta_table_in_name
    log_message.text += '  类型一sta层输入表\n# 加载策略: ' + strategy + \
        ' (注,01:直接追加 02:增量拉链,03:全表拉链,04:全表覆盖,05:增量覆盖)\n# ' + \
        '运行周期: [1] (注, 1:天,2:星期,3:旬,4:月,5:季,6:半年,7:年，00:其他)' + \
        '\n# 脚本功能: 实现ETL加载模板类型一' + choose_insert_type + \
        '方式\n# 创建时间: ' + create_date + \
        '\n# 作    者: 彭德剑 pdj408@163.com\n# 修改记录: 1、\n#\n#************************' + \
        '***********************************************************\n'
    return xmldoc

if __name__ == '__main__':
    district_suffix = ''
    current_layer = ''
    underline = '_'
    two_space = '  '

    # 生成程序所在目录
    root = 'kettleProgram'
    # 生成的SQL文件名
    table_create_sql_str = '_table_create.sql'
    # excel 配置文件目录
    excel_config_dir = 'config_file'

    # 各文件后缀
    sql_suffix = '.sql'
    ktr_suffix = '.ktr'
    kjb_suffix = '.kjb'
    xlsx_suffix = '.xlsx'

    # 系统来源地
    sys_from_cd = 'cd'
    sys_from_bj = 'bj'
    sys_from_wh = 'wh'

    layer_sta = 'sta'
    layer_sda = 'sda'

    # 获取参数列表
    data_froms, project_names, districts, layers, extract_types \
        = common.get_input_info_tables()
    excel_file_name, project_name, project_connect_name, district_suffix, current_layer, data_from, extract_type \
        = get_basic_information(excel_config_dir,district_suffix,current_layer)

    # ETL文件模板
    source_folder = extract_type + os.sep
    if project_name == 'crm':
        source_folder += 'crm' + os.sep
    sta_source_file = source_folder + 'sta_crm_accounts_bj.ktr'
    sda_source_file1 = source_folder + 'sda_template_1.kjb'
    sda_source_file2 = source_folder + 'sda_template_2.kjb'
    sda_source_file3 = source_folder + 'sda_template_3.kjb'
    sda_source_file4 = source_folder + 'sda_template_4.kjb'
    sda_source_file5 = source_folder + 'sda_template_5.kjb'

    current_folder_path = project_connect_name + os.sep + extract_type + os.sep + current_layer
    common.delete_old_files_mk_folder(current_folder_path)

    table_names, primary_keys, indexes, conditions, table_types, insert_types, fields_df_useful \
        = get_table_info_from_excel(excel_file_name)

    generate_sql_xml_files(project_connect_name,table_names,table_types,insert_types
                           ,primary_keys,indexes,conditions,fields_df_useful,current_layer
                           ,extract_type)
    common.shuffle_files(project_name,current_layer,extract_type,root,district_suffix,project_connect_name)

