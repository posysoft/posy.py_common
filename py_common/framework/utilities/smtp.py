# coding:utf-8
# Created by lilx on 2015/3/4
__author__ = 'lilx'

import smtplib
from email.mime.text import MIMEText


class Email(object):
    """
    email
    """
    # Default options
    default = {
        'host': '',
        'port': 25,
        'username': '',
        'password': '',
        'from': '',
        'to': [],
        'use_tls': True
    }

    def __init__(self, options, logger=None):
        self.options = self.default
        self.options.update(options)
        self.log = logger

    def add_recipients(self, recipients):
        for r in recipients:
            if r not in self.options['to']:
                self.options['to'].append(r)

    def send_simple_mail(self, Content, Subject, To=None, From=None):
        msg = MIMEText(Content, _charset='utf-8')
        msg['Subject'] = Subject
        msg['from'] = From if From else self.options['from']
        msg['to'] = ';'.join(To if To else self.options['to'])
        self.send_mail(msg.as_string(), To)

    def send_mail(self, message, recipients=None):
        if not recipients:
            recipients = self.options['to']
        smtp = smtplib.SMTP()
        try:
            smtp.connect(self.options['host'], self.options['port'])
            if self.options['use_tls']:
                smtp.starttls()
            smtp.login(self.options['username'], self.options['password'])
            smtp.sendmail(self.options['from'], recipients, message)
            if self.log:
                self.log.info('Send Message to: %s' % ', '.join(recipients))
        finally:
            smtp.quit()


if __name__ == '__main__':
    mail = Email({
        'username': '',
        'password': '',
        'to': ['']
    })
    msg = MIMEText('test', _charset='utf-8')
    msg['Subject'] = u'测试'
    msg['from'] = mail.options['from']
    msg['to'] = ';'.join(mail.options['to'])
    mail.send_mail(msg.as_string())