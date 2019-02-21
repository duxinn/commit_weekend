#!/user/bin/python3
# -*- coding:utf-8 -*-

import paramiko
import pymysql
import psycopg2

'''
此模块 操作 mysql数据库 du_op_app库下的 api_gitlog、 api_commits、 api_gitlogquit三张表

api+gitlog  api_gitlogquit 表结构如下
+-----------------+--------------+------+-----+---------+----------------+
| Field           | Type         | Null | Key | Default | Extra          |
+-----------------+--------------+------+-----+---------+----------------+
| id              | int(11)      | NO   | PRI | NULL    | auto_increment |
| commit_hash     | varchar(200) | NO   |     | NULL    |                |
| author_name     | varchar(200) | NO   |     | NULL    |                |
| author_email    | varchar(254) | NO   |     | NULL    |                |
| committer_name  | varchar(200) | NO   |     | NULL    |                |
| committer_email | varchar(254) | NO   |     | NULL    |                |
| committer_date  | datetime(6)  | NO   |     | NULL    |                |
| namespace       | varchar(200) | NO   |     | NULL    |                |
| project         | varchar(200) | NO   |     | NULL    |                |
| ref             | varchar(200) | NO   |     | NULL    |                |
| subject         | longtext     | NO   |     | NULL    |                |
| add_num         | bigint(20)   | NO   |     | NULL    |                |
| del_num         | bigint(20)   | NO   |     | NULL    |                |
| file_num        | bigint(20)   | NO   |     | NULL    |                |
+-----------------+--------------+------+-----+---------+----------------|
'''

# mysql -h 192.168.2.8 -uroot -pAeic3672@983cie CMS_SZLM
CONN_DU_OP = {'host': '192.168.2.8',
            'port': 3306,
            'user': 'root',
            'password': 'Aeic3672@983cie',
            'database': 'du_op_app',
            'charset': 'utf8'}


gitlib_path = '/var/opt/gitlab/git-data/repositories'
def commit_data_rewrite_api_gitlog():
    '''此函数操作 api_gitlog '''
    # data_list = []
    data_line = {}
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname='192.168.2.8', port=22, username='suiyang', password='Password1')

    # conn = pymysql.Connect(host='192.168.2.8', port=3306, user='root',
                           # password='Aeic3672@983cie', database='du_op_app', charset='utf8')
    conn = pymysql.connect(**CONN_DU_OP)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    # 在这三个指令之下，表格中的资料会完全消失，可是表格本身会继续存在
    cursor.execute("TRUNCATE TABLE api_gitlog")
    cursor.execute("TRUNCATE TABLE api_commits")
    cursor.execute("TRUNCATE TABLE api_gitlogquit")
    stdin, stdout, stderr = ssh.exec_command('cd %s && ls' % gitlib_path)

    # 读取 gitlib_path 路径下的 所有组
    namespaces = stdout.read()
    commit_num = 0
    # 遍历插入每个组、每个项目、每个分支、
    for namespace_name in namespaces.decode('utf-8').split('\n'):
        if commit_num > 1000:
            conn.commit()
            commit_num = 0
        if namespace_name:
            namespace_path = '%s/%s' % (gitlib_path, namespace_name)
            stdin, stdout, stderr = ssh.exec_command('cd %s && ls' % namespace_path)
            # 读取 组 下面的所有项目 
            projects = stdout.read()
            for project_name in projects.decode('utf-8').split('\n'):
                if project_name and ".wiki.git" not in project_name:
                    # 构建项目路径
                    project_path = '%s/%s/%s' % (gitlib_path,namespace_name, project_name)
                    stdin, stdout, stderr = ssh.exec_command('cd %s && git branch' % project_path)
                    # 读取 项目 下的所有分支
                    refs = stdout.read()
                    for ref_name in refs.decode('utf-8').split('\n'):
                        if ref_name:
                            # 构建分支名字
                            ref_name = ref_name.replace('*', '').strip()
                            stdin, stdout, stderr = ssh.exec_command(r"cd %s && git log %s --pretty=format:'%%H|%%an|%%ae|%%cn|%%ce|%%ci|%%s' --numstat" % \
                                (project_path, ref_name))
                            # cd 项目路径 && git log 分支名字  -->  每个分支 的 提交日志
                            data = stdout.read().decode('utf-8').split('\n')
                            add_num = 0
                            del_num = 0
                            file_num = 0
                            for da in data:
                                if "|" in da:
                                    da = da.split('|')
                                    data_line = {
                                        'namespace': namespace_name,  # 组名
                                        'project': project_name.replace('.git', ''),  # 项目名称
                                        'ref': ref_name, # 分支名称
                                        'commit_hash': da[0], # commit_hash
                                        'author_name': da[1], # 姓名
                                        'author_email': da[2], # 邮件地址
                                        'committer_name': da[3], # 提交姓名
                                        'committer_email': da[4], # 提交邮件
                                        'committer_date': da[5][0:18], # 提交日期
                                        'subject': da[6].replace('\'', ''), # 备注
                                    }
                                elif da and "|" not in da:
                                    da = da.split('\t')
                                    if da[0] != "-":
                                        add_num += int(da[0])
                                    if da[1] != "-":
                                        del_num += int(da[1])
                                    file_num += 1
                                elif not da:
                                    data_num = {
                                        'add_num': add_num,
                                        'del_num': del_num,
                                        'file_num': file_num
                                    }
                                    data_line.update(data_num)
                                    data_insert = (
                                        "INSERT INTO api_gitlog \
                                        (commit_hash,author_name,author_email,committer_name,committer_email,committer_date,\
                                        namespace,project,ref,subject,add_num,del_num,file_num) \
                                        VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
                                        (data_line['commit_hash'], data_line['author_name'], data_line['author_email'], \
                                            data_line['committer_name'], data_line['committer_email'], data_line['committer_date'],\
                                            data_line['namespace'], data_line['project'], data_line['ref'], data_line['subject'],\
                                             data_line['add_num'], data_line['del_num'], data_line['file_num'])
                                        )
                                    
                                    # 把 data_line 列表中
                                    # 组名、项目名称、分支名称、commit_hash、姓名、邮件地址、提交姓名、提交邮件、提交日期
                                    # 插入到 api_gitlog 里
                                    count = cursor.execute(data_insert)
                                    commit_num += 1
                                    add_num = 0
                                    del_num = 0
                                    file_num = 0
                                    data_line = {}
    # 提交 api_gitlog表 的插入操作
    conn.commit()
    cursor.close()

    cursor = conn.cursor()
    # 以下指令更新 api_gitlog表中 committer_name字段 的内容
    cursor.execute("update api_gitlog set committer_name ='代立健' where committer_name ='dailijian'")
    cursor.execute("update api_gitlog set committer_name ='荀建祥' where committer_name ='xunjianxiang'")
    cursor.execute("update api_gitlog set committer_name ='刘家兴' where committer_name ='liujx@shuzilm.cn'")
    cursor.execute("update api_gitlog set committer_name ='xewk' where committer_name ='babbage'")
    cursor.execute("update api_gitlog set committer_name ='xewk' where committer_name ='xierwake'")
    cursor.execute("update api_gitlog set committer_name ='王海廷' where committer_name ='wanghaiting'")
    cursor.execute("update api_gitlog set committer_name ='邓为强' where committer_name ='dwq'")
    cursor.execute("update api_gitlog set committer_name ='李作云' where committer_name ='李先生'")
    cursor.execute("update api_gitlog set committer_name ='李作云' where committer_name ='Lizyun'")
    cursor.execute("update api_gitlog set committer_name ='陈德' where committer_name ='boot'")
    cursor.execute("update api_gitlog set committer_name ='陈德' where committer_name ='chende@shuzilm.cn'")
    cursor.execute("update api_gitlog set committer_name ='杨玉奇' where committer_name ='yangyuqi@sina.com'")
    cursor.execute("update api_gitlog set committer_name ='杨玉奇' where committer_name ='yangyq@shuzilm.cn'")
    cursor.execute("update api_gitlog set committer_name ='杨玉奇' where committer_name ='zhangyq'")
    cursor.execute("update api_gitlog set committer_name ='杨玉奇' where committer_name ='yangyuqi'")
    cursor.execute("update api_gitlog set committer_name ='dandycheung' where committer_name ='Dandy Cheung'")
    cursor.execute("update api_gitlog set committer_name ='tianxc' where committer_name ='tianxuchun'")
    cursor.execute("update api_gitlog set committer_name ='tianxc' where committer_name ='tian xuchun'")
    cursor.execute("update api_gitlog set committer_name ='anxz' where committer_name ='安相璋'")
    cursor.execute("update api_gitlog set committer_name ='lipeigang' where committer_name ='lipg'")
    cursor.execute("update api_gitlog set committer_name ='lipeigang' where committer_name ='Li PeiGang'")
    cursor.execute("update api_gitlog set committer_name ='raoping' where committer_name ='raoping@shuzilm.cn'")
    cursor.execute("update api_gitlog set committer_name ='赵华飞' where committer_name ='zhf'")
    cursor.execute("update api_gitlog set committer_name ='赵华飞' where committer_name ='赵华飞'")
    cursor.execute("update api_gitlog set committer_name ='何成飞' where committer_name ='jackymelonhe'")
    cursor.execute("update api_gitlog set committer_name ='张聪' where committer_name ='killboyzc'")

    cursor.execute("update api_gitlog set committer_name ='张勇威' where committer_name ='zhangyw'")
    cursor.execute("update api_gitlog set committer_name ='王威振' where committer_name ='wangwz'")
    cursor.execute("update api_gitlog set committer_name ='王汝宁' where committer_name ='wangrn'")
    cursor.execute("update api_gitlog set committer_name ='陈德' where committer_name ='chende'")
    cursor.execute("update api_gitlog set committer_name ='方财良' where committer_name ='fangcl'")
    cursor.execute("update api_gitlog set committer_name ='荀建祥' where committer_name ='xunjx'")
    cursor.execute("update api_gitlog set committer_name ='代立健' where committer_name ='dailj'")
    cursor.execute("update api_gitlog set committer_name ='张聪' where committer_name ='zhangcong'")
    cursor.execute("update api_gitlog set committer_name ='陈龑' where committer_name ='chenyan'")
    cursor.execute("update api_gitlog set committer_name ='李凯' where committer_name ='likai'")
    cursor.execute("update api_gitlog set committer_name ='杨玉奇' where committer_name ='yangyq'")
    cursor.execute("update api_gitlog set committer_name ='刘耕华' where committer_name ='liugh'")
    cursor.execute("update api_gitlog set committer_name ='李作云' where committer_name ='lizy'")
    cursor.execute("update api_gitlog set committer_name ='周成杰' where committer_name ='zhoucj'")
    cursor.execute("update api_gitlog set committer_name ='高健' where committer_name ='gaojian'")
    cursor.execute("update api_gitlog set committer_name ='陈敏' where committer_name ='chenmin'")
    cursor.execute("update api_gitlog set committer_name ='王海廷' where committer_name ='wanght'")
    cursor.execute("update api_gitlog set committer_name ='李朋波' where committer_name ='lipb'")
    cursor.execute("update api_gitlog set committer_name ='王帅' where committer_name ='wangshuai'")
    cursor.execute("update api_gitlog set committer_name ='赵繁旗' where committer_name ='zhaofq'")
    cursor.execute("update api_gitlog set committer_name ='何成飞' where committer_name ='hecf'")
    cursor.execute("update api_gitlog set committer_name ='刘伟' where committer_name ='liuwei'")
    cursor.execute("update api_gitlog set committer_name ='杨旗' where committer_name ='yangqi'")
    cursor.execute("update api_gitlog set committer_name ='张迁' where committer_name ='zhangqian'")
    cursor.execute("update api_gitlog set committer_name ='侯克佩' where committer_name ='houkp'")
    cursor.execute("update api_gitlog set committer_name ='段亚俊' where committer_name ='duanyj'")
    cursor.execute("update api_gitlog set committer_name ='姚少琛' where committer_name ='yaosc'")
    cursor.execute("update api_gitlog set committer_name ='邓键' where committer_name ='dengjian'")
    cursor.execute("update api_gitlog set committer_name ='于俊娇' where committer_name ='yujj'")
    cursor.execute("update api_gitlog set committer_name ='李凯' where committer_name ='likai'")

    conn.commit()
    cursor.close()
    # 最终 committer_name 字段中的内容如下
        # committer_name |
        # +----------------+
        # | dailj          |
        # | xunjx          |
        # | dengwq         |
        # | wangwz         |
        # | zhangyw        |
        # | dandycheung    |
        # | chende         |
        # | wangrn         |
        # | Administrator  |
        # | liuwei         |
        # | wanght         |
        # | yangqi         |
        # | houkp          |
        # | wangshuai      |
        # | fangcl         |
        # | xewk           |
        # | zhangxd        |
        # | zhangcong      |
        # | yangyq         |
        # | suiyang        |
        # | chenmin        |
        # | zhoucj         |
        # | lizy           |
        # | liujx          |
        # | liugh 

    # conn = pymysql.connect(**CONN_DU_OP)
    # cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = 'select name from CMS_SZLM.szlm_users;'
    cursor.execute(sql)
    name_szlm_users = cursor.fetchall()
    name_szlm_users_list = []
    for i in name_szlm_users:
        if i['name'] not in name_szlm_users_list:
            name_szlm_users_list.append(i['name'])

    sql = 'select distinct committer_name from api_gitlog;'
    cursor.execute(sql)
    name_api_gitlog = cursor.fetchall()
    name_api_gitlog_list = []
    for i in name_api_gitlog:
        if i['committer_name'] not in name_api_gitlog_list:
            name_api_gitlog_list.append(i['committer_name'])

    # 如果在 name_api_gitlog 表里 不在 name_szlm_users 里 就 是离职的人
    quit_name_list_temp = []
    for i in name_api_gitlog_list:
        if i not in name_szlm_users_list:
            quit_name_list_temp.append(i)

    quit_name_list = ['刘家兴','赵华飞','liujx','zhangxd','xewk','suiyang','Cailiang','YANG,ZhiGang','YANG Zhigang','lansj','WangChen','269333685@qq.com','secpersu','shuzilm','tianxc','anxz','lipeigang','chenling','songguang','zhaohf','raoping','jiazhongqiang','System Administrator','jerryyyq','root','YANG','Andy841','Ghost','bruce']
    for i in quit_name_list_temp:
        if i not in quit_name_list:
            quit_name_list.append(i)

    quit_name_tuple = tuple(quit_name_list)
    # 把 离职的人的记录 保存到 api_gitlogquit 表中
    cursor.execute("Insert into api_gitlogquit select * from api_gitlog where committer_name in %s" % str(quit_name_tuple))

    # 把 离职的人的记录 从 api_gitlog 中删除
    cursor.execute("delete from api_gitlog where committer_name in %s" % str(quit_name_tuple))

    conn.commit()
    cursor.close()
    conn.close()
    ssh.close()

def commit_data_rewrite_api_commits():

    '''
    此函数从 postgres 数据库中 gitlabhq_production库中 ci_pipelines表 和 projects表 中查出所有数据
    对应写入MySQL数据库中 du_op_app库中 api_commits表 中
    
    ci_pipelines 表结构如下
    gitlabhq_production=> \d ci_pipelines;
                                              Table "public.ci_pipelines"
            Column        |            Type             |                         Modifiers                         
    ----------------------+-----------------------------+-----------------------------------------------------------
     id                   | integer                     | not null default nextval('ci_pipelines_id_seq'::regclass)
     ref                  | character varying           | 
     sha                  | character varying           | 
     before_sha           | character varying           | 
     created_at           | timestamp without time zone | 
     updated_at           | timestamp without time zone | 
     tag                  | boolean                     | default false
     yaml_errors          | text                        | 
     committed_at         | timestamp without time zone | 
     project_id           | integer                     | 
     status               | character varying           | 
     started_at           | timestamp without time zone | 
     finished_at          | timestamp without time zone | 
     duration             | integer                     | 
     user_id              | integer                     | 
     lock_version         | integer                     | 
     auto_canceled_by_id  | integer                     | 
     pipeline_schedule_id | integer                     | 
     source               | integer                     | 
     protected            | boolean                     | 
     config_source        | integer                     | 
     failure_reason       | integer                     |
    
    api_gitlog 和 api_gitlogquit 表结构如下
    +-----------------+--------------+------+-----+---------+----------------+
    | Field           | Type         | Null | Key | Default | Extra          |
    +-----------------+--------------+------+-----+---------+----------------+
    | id              | int(11)      | NO   | PRI | NULL    | auto_increment |
    | commit_hash     | varchar(200) | NO   |     | NULL    |                |
    | author_name     | varchar(200) | NO   |     | NULL    |                |
    | author_email    | varchar(254) | NO   |     | NULL    |                |
    | committer_name  | varchar(200) | NO   |     | NULL    |                |
    | committer_email | varchar(254) | NO   |     | NULL    |                |
    | committer_date  | datetime(6)  | NO   |     | NULL    |                |
    | namespace       | varchar(200) | NO   |     | NULL    |                |
    | project         | varchar(200) | NO   |     | NULL    |                |
    | ref             | varchar(200) | NO   |     | NULL    |                |
    | subject         | longtext     | NO   |     | NULL    |                |
    | add_num         | bigint(20)   | NO   |     | NULL    |                |
    | del_num         | bigint(20)   | NO   |     | NULL    |                |
    | file_num        | bigint(20)   | NO   |     | NULL    |                |
    +-----------------+--------------+------+-----+---------+----------------|

    api_commits表结构如下
    | Field         | Type         | Null | Key | Default | Extra          |
    +---------------+--------------+------+-----+---------+----------------+
  0 | id            | int(11)      | NO   | PRI | NULL    | auto_increment | 
  1 | project_id    | bigint(20)   | YES  |     | NULL    |                | 
  2 | ref           | varchar(255) | YES  |     | NULL    |                | 
  3 | sha           | varchar(255) | YES  |     | NULL    |                | 
  4 | before_sha    | varchar(255) | YES  |     | NULL    |                | 
  5 | push_data     | longtext     | YES  |     | NULL    |                | 
  6 | created_at    | datetime(6)  | YES  |     | NULL    |                | 
  7 | updated_at    | datetime(6)  | YES  |     | NULL    |                | 
  8 | tag           | tinyint(1)   | NO   |     | NULL    |                | 
  9 | yaml_errors   | longtext     | YES  |     | NULL    |                | 
 10 | committed_at  | datetime(6)  | YES  |     | NULL    |                | 
 11 | gl_project_id | longtext     | YES  |     | NULL    |                | 
 12 | status        | varchar(255) | YES  |     | NULL    |                | 
 13 | started_at    | datetime(6)  | YES  |     | NULL    |                | 
 14 | finished_at   | datetime(6)  | YES  |     | NULL    |                | 
 15 | duration      | bigint(20)   | YES  |     | NULL    |                | 
 16 | user_id       | bigint(20)   | YES  |     | NULL    |                | 
 17 | lock_version  | bigint(20)   | YES  |     | NULL    |                | 
    +---------------+--------------+------+-----+---------+----------------+
    18个字段
    '''
    conn = psycopg2.connect(host="127.0.0.1", port=5432, user="gitlab", password="", database="gitlabhq_production")
    cur = conn.cursor()
    sql = "select ci_pipelines.id, ref, sha, before_sha, ci_pipelines.created_at, ci_pipelines.updated_at, tag, yaml_errors, committed_at, project_id, status, started_at, finished_at, duration, user_id, lock_version, auto_canceled_by_id, pipeline_schedule_id, source, protected, config_source, ci_pipelines.failure_reason, projects.name \
    from ci_pipelines right join \
    projects on project_id = projects.id;"
    # select projects.name, ref from ci_pipelines right join projects on project_id = projects.id where projects.name = 'zeus'
    cur.execute(sql)
    s = cur.fetchall()
    conn.close()

    conn = pymysql.Connect(host='192.168.2.8', port=3306, user='root', password='Aeic3672@983cie', database='du_op_app', charset='utf8')
    cursor = conn.cursor()

    for i in s:
        i = list(i)
        if i[0] in (None, 'None'):
            continue
        if i[1] in (None, 'None'):
            i[1] = 'null'
        if i[2] in (None, 'None'):
            i[2] = "'%s'" % i[2]
        if i[3] in (None, 'None'):
            i[3] = 'null'
        if i[4] in (None, 'None'):
            i[4] = 'null'
        if i[5] in (None, 'None'):
            i[5] = 'null'
        if i[6] in (None, 'None'):
            i[6] = 'null'
        if i[7] in (None, 'None'):
            i[7] = 'null'
        if i[8] in (None, 'None'): # yaml_errors字段
            i[8] = "null"
        if i[9] in (None, 'None'): # committed_at字段
            i[9] = "'%s'" % i[9]
        if i[10] in (None, 'None'):
            i[10] = 'null'
        if i[11] in (None, 'None'): # status字段
            i[11] = "null"
        else:
            i[11] = "'%s'" % i[11]
        if i[12] in (None, 'None'):
            i[12] = 'null'
        if i[13] in (None, 'None'): # finished_at字段
            i[13] = "null"
        if i[14] in (None, 'None'):
            i[14] = 'null'
        if i[15] in (None, 'None'): # user_id字段
            i[15] = "null"
        if i[16] in (None, 'None'):
            i[16] = 'null'
        if i[17] in (None, 'None'):
            i[17] = 'null'
        if i[18] in (None, 'None'):
            i[18] = 'null'
        if i[19] in (None, 'None'):
            i[19] = 'null'
        if i[20] in (None, 'None'):
            i[20] = 'null'
        if i[21] in (None, 'None'):
            i[21] = 'null'
        if i[22] in (None, 'None'):
            i[22] = 'null'
    # s，即从postgresql 数据库 sql 语句 查出的字段及序号如下
    #  0       id
    #  1       ref
    #  2       sha
    #  3       before_sha
    #  4       created_at
    #  5       updated_at
    #  6       tag
    #  7       yaml_errors
    #  8       committed_at
    #  9       project_id
    # 10       status
    # 11       started_at
    # 12       finished_at
    # 13       duration
    # 14       user_id
    # 15       lock_version
    # 16       auto_canceled_by_id
    # 17       pipeline_schedule_id
    # 18       source
    # 19       protected
    # 20       config_source
    # 21       failure_reason
    # 22       projects.name

    # 以下插入语句，将psycopg2数据 gitlabhq_production库中 ci_pipelines表 和projects表 查出的所有内容 写入 api_commits表中

        # 18 个 %S
        # 以下两行为 api_commits 表插入的字段
        # id   project_id  ref      sha  before_sha push_data created_at updated_at tag  yaml_errors 
        # committed_at  gl_project_id status started_at  finished_at  duration   user_id  lock_version
        insert = "insert into api_commits values('%s',%s,'%s','%s','%s','%s','%s','%s',%s,'%s',%s,'%s','%s',%s,'%s',%s,'%s',%s)" %\
         (i[0], i[9], i[1], i[2], i[3], '', i[4], i[5], i[6], i[7], i[8], i[22], i[10], i[11], i[12], i[13], i[14], i[15])
        # 以下4行 为i的位置和对应的值
        # (i[0],  i[9],    i[1],  i[2], i[3],     '',      i[4],       i[5],     i[6],   i[7],
        #  id   project_id  ref   sha  before_sha '',   created_at  updated_at   tag    yaml_errors
        #  i[8],           i[22],        i[10],   i[11],          i[12],        i[13],      i[14],     i[15])
        #  committed_at  projects.name   status  started_at      finished_at  duration      user_id     lock_vision
        cursor.execute(insert)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    commit_data_rewrite_api_gitlog()
    commit_data_rewrite_api_commits()
