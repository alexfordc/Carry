from django.db import models


# Create your models here.


class Clj(models.Model):
    """ 超链接 """
    name = models.CharField(max_length=30)
    addres = models.URLField(max_length=100)

    def __unicode__(self):
        return self.name


class Users(models.Model):
    """ 用户 """
    CHICO_ZT = [[0, '未启用'], [1, '启用']]
    CHICO_QX = [[1, '普通用户'], [2, '内部用户'], [3,'管理员']]
    name = models.CharField('用户名', max_length=20, unique=True)
    password = models.CharField('密码', max_length=40)
    phone = models.CharField('手机号', max_length=11)
    email = models.EmailField('邮箱', max_length=36, null=True, blank=True)
    enabled = models.IntegerField('状态', choices=CHICO_ZT, default=0)  # 1：启用，0：未启用
    jurisdiction = models.IntegerField('用户权限', choices=CHICO_QX, default=1)  # 1：普通用户，2：内部用户，3：管理员

    def __unicode__(self):
        return self.name


class WorkLog(models.Model):
    """ 工作日志 """
    belonged = models.ForeignKey('Users')  # 所属用户
    startDate = models.DateField('填写日期', auto_now_add=True)
    date = models.DateField('所属工作日期')
    title = models.CharField('标题', max_length=50)
    body = models.TextField('详细内容', max_length=300)

    class Meta:
        """ 以所属工作日期逆序 """
        ordering = ('-date',)

    def __unicode__(self):
        return self.belonged


class TradingAccount(models.Model):
    """ 交易账户 """
    belonged = models.ForeignKey('Users')  # 所属用户
    host = models.CharField('账户', max_length=40)
    port = models.IntegerField('端口')
    license = models.CharField('许可证', max_length=30)
    appid = models.CharField('ApppID', max_length=20)
    userid = models.CharField('用户ID', max_length=20)

    def __unicode__(self):
        return self.belonged


'''
class Transaction_data(models.Model):
    date=models.DateTimeField()
    open=models.FloatField()
    high=models.FloatField()
    low=models.FloatField()
    close=models.FloatField()
    amout=models.IntegerField()
    vol=models.FloatField()
    code=models.CharField(max_length=12)
    createDate=models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'transaction_data'  # 自定义表名称为mytable
        ordering = ['date']
'''
