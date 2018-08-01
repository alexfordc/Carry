from django.contrib import admin
from .models import Clj,Users

# Register your models here.

class CljAdmin(admin.ModelAdmin):
    list_display=('name','addres')

class UsersAdmin(admin.ModelAdmin):
    list_display = ('name','password','enabled')

admin.site.register(Clj,CljAdmin)
admin.site.register(Users,UsersAdmin)