# -*- coding:utf-8 -*-
import time
import datetime
import pymysql
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header

mail_host = "mail.shuzilm.cn"
mail_sender = "it@shuzilm.cn"
mail_username = "it@shuzilm.cn"
mail_password = "xxxx"
email_list = ['xxxx@xxxx.cn', 'xxxx@xxxx.cn']


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


if __name__ == '__main__':
    temp = send_email_information(email_list, 'test')
    print(temp)
