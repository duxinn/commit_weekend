#!/user/bin/python
# -*- coding:utf-8 -*-
import time
import datetime
import pymysql
import os
import smtplib
import requests
from email.mime.text import MIMEText
from email.header import Header

DBConfig = {
    'host': '192.168.2.8',
    'port': 3306,
    'user': 'root',
    'passwd': 'xxxx',
    'db': 'du_op_app',
    'charset': 'utf8'
}

mail_host = "mail.shuzilm.cn"
mail_sender = "it@shuzilm.cn"
mail_username = "it@shuzilm.cn"
mail_password = "xxxx"

# 钉钉企业 ID
DING_CORPID = "xxxx"
# 钉钉企业密钥
DING_CORPSECRET = "xxxx"

technical_department = [12993126, 74448429, 48804840, 48806835, 48815834, 48828847, 48879846]


#                       技术中心     技术部     业务保障部 云端       终端       前端      大数据


def email_list_gen():
    '''此函数用于 生成 收件人邮件列表,返回技术中心和精准广告技术部的人员邮箱，数据类型为 list。
    '''
    conn = pymysql.connect(host='192.168.2.8', port=3306, user='root', passwd='xxxx', db='CMS_SZLM', charset='utf8')
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute("select userid, department, name from szlm_users_info;")
    userid_department_name = cursor.fetchall()
    # [{'userid': '1022522344671751', 'department': '[48879846]', 'name': '刘伟'},
    technical_usrid = set()
    for i in userid_department_name:
        if i.get('department'):
            # 如果有部门信息
            for j in eval(i.get('department')):
                # 如果部门的值在 technical_department 里
                if j in technical_department:
                    technical_usrid.add(i['userid'])
    technical_usrid_t = str(tuple(technical_usrid))
    sql = 'select email from szlm_users where uid in ' + technical_usrid_t + ';'
    cursor.execute(sql)
    email_list_query = cursor.fetchall()
    email_list = []
    for l in email_list_query:
        email_list.append(l.get('email'))
    # l = ['liuwei@shuzilm.cn', 'yangyq@shuzilm.cn', 'fangcl@shuzilm.cn', 'zhoucj@shuzilm.cn', 'lizy@shuzilm.cn', 'yangqi@shuzilm.cn', 'zhangyw@shuzilm.cn', 'chende@shuzilm.cn', 'liangwj@shuzilm.cn', 'wanght@shuzilm.cn', 'wangwz@shuzilm.cn', 'dailj@shuzilm.cn', 'xunjx@shuzilm.cn', 'dengwq@shuzilm.cn', 'zhangyp@shuzilm.cn', 'lipb@shuzilm.cn', 'wangshuai@shuzilm.cn', 'wangrn@shuzilm.cn', 'liugh@shuzilm.cn', 'chenmin@shuzilm.cn', 'zhangcong@shuzilm.cn', 'zhangqian@shuzilm.cn', 'houkp@shuzilm.cn', 'linjr@shuzilm.cn', 'duanyj@shuzilm.cn', 'hecf@shuzilm.cn', 'zhaofq@shuzilm.cn', 'yaosc@shuzilm.cn', 'yujj@shuzilm.cn', 'dengjian@shuzilm.cn', 'chenyan@shuzilm.cn', 'gaojian@shuzilm.cn', 'likai@shuzilm.cn']
    return email_list


# 发送邮件
def send_email_information(email_list, content, title="提交代码周报"):
    # 实例化一封邮件
    msg = MIMEText(content, _subtype='html', _charset='utf-8')
    msg['Subject'] = title
    msg['From'] = mail_sender

    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(mail_username, mail_password)
        for add in email_list:
            msg['To'] = Header(add, 'utf-8')
            server.sendmail(mail_sender, add, msg.as_string())
        # 以下两行代码用于运维测试用
        # msg['To'] = Header('chenmin@shuzilm.cn','utf-8')
        # server.sendmail(mail_sender, 'chenmin@shuzilm.cn', msg.as_string())
        server.close()
        return 1
    except Exception as e:
        print(str(e))
        return 0


def commit_count_html():
    lines = []
    conn = pymysql.connect(**DBConfig)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = "select committer_name N,MAX(DATE_FORMAT(committer_date,'%Y-%m-%d')) D from api_gitlog where committer_name not in ('王海廷', '邓为强','dengwq','dandycheung','Cailiang','Administrator') group by N order by D desc;"
    cursor.execute(sql)
    # 提交代码活跃统计
    data = cursor.fetchall()
    all_data = {}
    for i in data:
        sql = "select commit_hash H,DATE_FORMAT(committer_date,'%Y-%m-%d') K,add_num N,del_num D,file_num F from api_gitlog where committer_name='" + \
              i['N'] + \
              "' and DATE_SUB(CURDATE(),INTERVAL 7 DAY) < DATE_FORMAT(committer_date,'%Y-%m-%d')"
        cursor.execute(sql)
        everyone = cursor.fetchall()
        hash_list = []
        date_list = []
        data_list = []
        if everyone:
            all_data.setdefault(i['N'], everyone)
            for ii in everyone:
                if ii['H'] not in hash_list:
                    hash_list.append(ii["H"])
                    if ii['K'] not in date_list:
                        date_list.append(ii["K"])
                        l = [ii["K"], ii['N'], 1]
                        data_list.append(l)
                    else:
                        for iii in data_list:
                            if ii['K'] == iii[0]:
                                iii[1] += ii['N']
                                iii[2] += 1
        for iii in data_list:
            i.setdefault(iii[0], ('%s | %s' % (iii[2], iii[1])))
    sql = "select j.committer_name,j.namespace,j.project,j.ref,DATE_FORMAT(j.committer_date,'%Y-%m-%d %H:%i:%S') D, j.subject FROM \
    (SELECT f.project,f.ref,MAX(f.committer_date) date from api_gitlog f \
    JOIN \
    (select c.project,c.ref from \
    (select * from (select project,ref from api_gitlog GROUP BY project,ref) a \
    LEFT JOIN \
    (select gl_project_id,ref re from api_commits where created_at>'2018-04-01 09:46:43.408599' GROUP BY gl_project_id,re) b \
    on a.project=b.gl_project_id and a.ref=b.re ) c where c.re is null) d \
    on f.project=d.project and f.ref=d.ref GROUP BY f.project,f.ref) h \
    JOIN \
    api_gitlog j \
    on h.project=j.project and h.ref=j.ref and h.date=j.committer_date \
    where '2018-05-15' < DATE_FORMAT(j.committer_date,'%Y-%m-%d')  \
    and j.namespace like 'du-%'\
    and j.ref='master' \
    and j.project not in ('gitbook','du-docs','warehouse_2.0','devops_git_webhook', 'warehouse_python') \
    order by j.committer_date desc"
    # 笛卡尔积 api_gitlog 和 api_commits
    cursor.execute(sql)

    # 未提交代码到 Bitbucket 仓库
    data1 = cursor.fetchall()

    sql = """
select committer_name,namespace,project,ref,committer_date D,`subject` from api_gitlog
where commit_hash in \
(select sha from (select gl_project_id,ref,MAX(finished_at) date from api_commits GROUP BY gl_project_id,ref) a JOIN api_commits b on a.gl_project_id=b.gl_project_id and a.ref=b.ref and a.date=b.finished_at where `status`='failed')
and project not in ('du_ftp_tools','liuw_test','androidTest2','btest','lbadvisor','book')
and ref = 'master'
"""
    cursor.execute(sql)

    # 未成功提交代码到 Bitbucket 仓库
    data2 = cursor.fetchall()
    cursor.close()
    conn.close()

    now = time.strftime("%Y-%m-%d", time.localtime())

    lines.append('<html lang="zh-cn"><head><meta charset="utf-8"/>')
    lines.append('<style>h1{text-align:center} h2{text-align:center} \
        .table-1 {border-collapse:collapse;}'
                 '.table-1 td {padding: 3px 10px;border:1px solid #ddd;text-align:center;}'
                 ' .title{background-color: #0088CC;color: #ffffff;}</style>')
    lines.append('</head><body><table class="table-1"  align="center">')
    lines.append('<h1>提交代码统计周报</h1>')
    lines.append('<h1>%s</h1>' % now)
    lines.append('<h2>提交代码活跃统计</h2>')
    lines.append("<tr class='title'>")
    lines.append('<td>姓名</td>')
    for i in range(6, 0, -1):
        day = time.strftime("%Y-%m-%d", time.strptime(str(datetime.datetime.now() +
                                                          datetime.timedelta(days=-i))[:18], "%Y-%m-%d %H:%M:%S"))
        lines.append('<td>%s</td>' % day)
    lines.append('<td>%s</td>' % now)
    lines.append('<td>总计</td>')
    lines.append('<td>最后提交时间</td>')
    lines.append('</tr>')
    for i in data:
        if (data.index(i) % 2) == 0:
            lines.append('<tr bgcolor="#F2F2F2">')
        else:
            lines.append('<tr>')
        lines.append('<td>%s</td>' % i['N'])
        for n in range(6, 0, -1):
            day = time.strftime("%Y-%m-%d",
                                time.strptime(str(datetime.datetime.now() + datetime.timedelta(days=-n))[:18],
                                              "%Y-%m-%d %H:%M:%S"))
            content = i.get(day, "-")
            title_list = []
            title_ha = []
            if content == "-":
                lines.append('<td>%s</td>' % content)
            else:
                for ii in all_data[i['N']]:
                    if ii['K'] == day and ii['H'] not in title_ha:
                        title_str = "添加：%s&nbsp;&nbsp;&#09删除：%s&nbsp;&nbsp;&#09修改文件：%s" % (
                            ii['N'], ii['D'], ii['F'])
                        title_ha.append(ii['H'])
                        title_list.append(title_str)
                title_str = "&#xd;".join(title_list)
                lines.append("<td title='%s'>%s</td>" % (title_str, content))

        content = i.get(now, "-")
        title_list = []
        title_ha = []
        if content == "-":
            lines.append('<td>%s</td>' % content)
        else:
            for ii in all_data[i['N']]:
                if ii['K'] == now and ii['H'] not in title_ha:
                    title_str = "添加：%s&nbsp;&nbsp;&#09删除：%s&nbsp;&nbsp;&#09修改文件：%s" % (
                        ii['N'], ii['D'], ii['F'])
                    title_ha.append(ii['H'])
                    title_list.append(title_str)
            title_str = "&#xd;".join(title_list)
            lines.append("<td title='%s'>%s</td>" % (title_str, content))

        commit_all = 0
        commit_add = 0
        commit_str = "-"
        for key, value in i.items():
            if key[0] == "2":
                commit_str = value.split("|")
                commit_all += int(commit_str[0])
                commit_add += int(commit_str[1])
        if commit_all != 0:
            commit_str = str(commit_all) + "|" + str(commit_add)
        lines.append('<td>%s</td>' % commit_str)

        d = datetime.datetime.strptime(i['D'], "%Y-%m-%d")
        if datetime.datetime.now() + datetime.timedelta(days=-3) > d > datetime.datetime.now() + datetime.timedelta(
                days=-7):
            lines.append("<td style='color:#DAA520'>%s</td>" % i['D'])
        elif datetime.datetime.now() + datetime.timedelta(days=-7) > d:
            lines.append("<td style='color:red'>%s</td>" % i['D'])
        else:
            lines.append("<td style='color:#00BB00'>%s</td>" % i['D'])
        lines.append('</tr>')
    lines.append('</table>')
    # data2 --> 未成功提交代码到 Bitbucket 仓库
    lines.append('<h2>未成功提交代码到 Bitbucket 仓库</h2>')
    lines.append('<table class="table-1"  align="center">')
    lines.append("<tr class='title'>")
    lines.append('<td>姓名</td>')
    lines.append('<td>组名</td>')
    lines.append('<td>项目</td>')
    lines.append('<td>分支</td>')
    lines.append('<td>日期</td>')
    lines.append('<td>备注</td>')
    lines.append('</tr>')
    for i in data2:
        if (data2.index(i) % 2) == 0:
            lines.append('<tr bgcolor="#F2F2F2">')
        else:
            lines.append('<tr>')
        lines.append('<td>%s</td>' % i['committer_name'])
        lines.append('<td>%s</td>' % i['namespace'])
        lines.append('<td>%s</td>' % i['project'])
        lines.append('<td>%s</td>' % i['ref'])
        lines.append('<td>%s</td>' % i['D'])
        lines.append('<td>%s</td>' % i['subject'])
        lines.append('</tr>')
    lines.append('</table>')
    # data1 --> 未提交代码到 Bitbucket 仓库
    lines.append('<h2>未提交代码到 Bitbucket 仓库</h2>')
    lines.append('<table class="table-1"  align="center">')
    lines.append("<tr class='title'>")
    lines.append('<td>姓名</td>')
    lines.append('<td>组名</td>')
    lines.append('<td>项目</td>')
    lines.append('<td>分支</td>')
    lines.append('<td>日期</td>')
    lines.append('<td>备注</td>')
    # lines.append('<td>失败原因</td>')
    lines.append('</tr>')
    for i in data1:
        if (data1.index(i) % 2) == 0:
            lines.append('<tr bgcolor="#F2F2F2">')
        else:
            lines.append('<tr>')
        lines.append('<td>%s</td>' % i['committer_name'])
        lines.append('<td>%s</td>' % i['namespace'])
        lines.append('<td>%s</td>' % i['project'])
        lines.append('<td>%s</td>' % i['ref'])
        lines.append('<td>%s</td>' % i['D'])
        lines.append('<td>%s</td>' % i['subject'])
        lines.append('</tr>')
    lines.append('</table>')
    lines.append('</body></html>')
    lines.append('</table>')
    data = "".join(lines)
    return data


if __name__ == '__main__':

    # [12993126,74448429,48804840,48806835,48815834,48828847,48879846]
    # 技术中心  技术部   业务保障部  云端       终端      前端   大数据
    command = "python3 /srv/app/du_op_weekly/commit_data.py"
    os.system(command)
    # 生成收件人列表
    email_list = email_list_gen()
    data = commit_count_html()
    # 原函数：send_email_information(email_list, content, title="提交代码周报")
    if send_email_information(email_list, data):
        print("邮件发送成功")
    else:
        print("邮件发送失败")
