from django.db import models

# Create your models here.


class Clj(models.Model):
    name=models.CharField(max_length=30)
    addres=models.URLField(max_length=100)

    def __unicode__(self):
        return self.name

class Users(models.Model):
    name = models.CharField(max_length=20)
    password = models.CharField(max_length=20)
    enabled = models.CharField(max_length=3)

    def __unicode__(self):
        return self.name

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
