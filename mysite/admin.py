from django.contrib import admin
from .models import Clj, Users, WorkLog, TradingAccount, SimulationAccount, InfoPublic, InfoBody


# Register your models here.

class CljAdmin(admin.ModelAdmin):
    list_display = ('name', 'addres')


class UsersAdmin(admin.ModelAdmin):
    list_display = ('name', 'password', 'phone', 'email', 'enabled', 'jurisdiction', 'creationTime')


class WorkLogAdmin(admin.ModelAdmin):
    list_display = ('belonged', 'startDate', 'date', 'title', 'body')


class TradingAccountAdmin(admin.ModelAdmin):
    list_display = ('belonged', 'host', 'enabled')


class SimulationAccountAdmin(admin.ModelAdmin):
    list_display = ('belonged', 'host', 'enabled')


class InfoPublicAdmin(admin.ModelAdmin):
    list_display = ('belonged', 'startDate', 'title')


class InfoBodyAdmin(admin.ModelAdmin):
    list_display = ('belonged', 'belongedTitle', 'startDate', 'body')


admin.site.register(Clj, CljAdmin)
admin.site.register(Users, UsersAdmin)
admin.site.register(WorkLog, WorkLogAdmin)
admin.site.register(TradingAccount, TradingAccountAdmin)
admin.site.register(SimulationAccount, SimulationAccountAdmin)
admin.site.register(InfoPublic, InfoPublicAdmin)
admin.site.register(InfoBody, InfoBodyAdmin)
