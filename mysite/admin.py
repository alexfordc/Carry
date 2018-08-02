from django.contrib import admin
from .models import Clj, Users, WorkLog, TradingAccount


# Register your models here.

class CljAdmin(admin.ModelAdmin):
    list_display = ('name', 'addres')


class UsersAdmin(admin.ModelAdmin):
    list_display = ('name', 'password', 'phone', 'email', 'enabled', 'jurisdiction')


class WorkLogAdmin(admin.ModelAdmin):
    list_display = ('belonged', 'startDate', 'date', 'title', 'body')


class TradingAccountAdmin(admin.ModelAdmin):
    list_display = ('belonged', 'host')


admin.site.register(Clj, CljAdmin)
admin.site.register(Users, UsersAdmin)
admin.site.register(WorkLog, WorkLogAdmin)
admin.site.register(TradingAccount,TradingAccountAdmin)
