from django import forms
from mysite import models
from captcha.fields import CaptchaField

from django.core.exceptions import ValidationError
from django.forms import fields
from django.forms import widgets
from django.core.validators import RegexValidator


class UsersForm(forms.ModelForm):
    passwordr = fields.CharField(max_length=40)
    captcha = CaptchaField()

    class Meta:
        model = models.Users
        fields = ['name', 'password', 'phone', 'email']

    def __init__(self, *args, **kwargs):
        super(UsersForm, self).__init__(*args, **kwargs)
        self.fields['name'].label = ''
        self.fields['name'].widget = forms.TextInput(attrs={"placeholder": "用户名"})
        self.fields['password'].label = ''
        self.fields['password'].widget = forms.PasswordInput(attrs={"placeholder": "密码"})
        self.fields['passwordr'].label = ''  # ''确认密码'
        self.fields['passwordr'].widget = forms.PasswordInput(attrs={"placeholder": "再次输入密码"})
        self.fields['phone'].label = ''  # ''手机'
        self.fields['phone'].widget = forms.TextInput(attrs={"placeholder": "手机号码"})
        self.fields['email'].label = ''  # ''邮箱'
        self.fields['email'].widget = forms.EmailInput(attrs={"placeholder": "邮箱（非必填）"})
        self.fields['captcha'].label = ''  # '验证码'


# class UsersForm(forms.ModelForm):
#     name = forms.CharField(widget=forms.TextInput(attrs={"class": "form-control","placeholder": "请输入用户名"}))
#     password = forms.CharField(widget=forms.PasswordInput(attrs={"class": "form-control","placeholder": "请输入密码"}))
#     passwordr = forms.CharField(widget=forms.PasswordInput(attrs={"class": "form-control","placeholder": "确认密码"}))
#     phone = forms.CharField(widget=forms.TextInput(attrs={"class": "form-control","placeholder": "请输入手机号码"}))
#     email = forms.EmailField(widget=forms.TextInput(attrs={"class": "form-control","placeholder": "请输入邮箱账号"}))
#     #captcha = CaptchaField()


"""
username = forms.EmailField(widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "请输入邮箱账号", "value": "", "required": "required",}),
                              max_length=50,error_messages={"required": "用户名不能为空",})
password = forms.CharField(widget=forms.PasswordInput(attrs={"class": "form-control", "placeholder": "请输入密码", "value": "", "required": "required",}),
                          min_length=8, max_length=50,error_messages={"required": "密码不能为空",})


class RegisterForm(forms.Form):
    username = fields.CharField(
        required=True,
        widget=widgets.TextInput(attrs={'class': "form-control",'placeholder': '用户名为8-12个字符'}),
        min_length=6,
        max_length=12,
        strip=True,
        error_messages={'required': '标题不能为空',
                        'min_length': '用户名最少为6个字符',
                        'max_length': '用户名最不超过为20个字符'},
    )
    email = fields.EmailField(
        required=True,
        widget=widgets.TextInput(attrs={'class': "form-control",'placeholder': '请输入邮箱'}),
        strip=True,
        error_messages={'required': '邮箱不能为空',
                        'invalid':'请输入正确的邮箱格式'},
        )
    pwd = fields.CharField(
        widget=widgets.PasswordInput(attrs={'class': "form-control",'placeholder': '请输入密码，必须包含数字,字母,特殊字符'},render_value=True),
        required=True,
        min_length=6,
        max_length=12,
        strip=True,
        validators=[
            # 下面的正则内容一目了然，我就不注释了
            RegexValidator(r'((?=.*\d))^.{6,12}$', '必须包含数字'),
            RegexValidator(r'((?=.*[a-zA-Z]))^.{6,12}$', '必须包含字母'),
            RegexValidator(r'((?=.*[^a-zA-Z0-9]))^.{6,12}$', '必须包含特殊字符'),
            RegexValidator(r'^.(\S){6,10}$', '密码不能包含空白字符'),
        ], #用于对密码的正则验证
        error_messages={'required': '密码不能为空!',
                        'min_length': '密码最少为6个字符',
                        'max_length': '密码最多不超过为12个字符!',},
    )
    pwd_again = fields.CharField(
        #render_value会对于PasswordInput，错误是否清空密码输入框内容，默认为清除，我改为不清楚
        widget=widgets.PasswordInput(attrs={'class': "form-control",'placeholder': '请再次输入密码!'},render_value=True),
        required=True,
        strip=True,
        error_messages={'required': '请再次输入密码!',}

    )

    def clean_username(self):
        # 对username的扩展验证，查找用户是否已经存在
        username = self.cleaned_data.get('username')
        users = models.User.objects.filter(username=username).count()
        if users:
            raise ValidationError('用户已经存在！')
        return username

    def clean_email(self):
        # 对email的扩展验证，查找用户是否已经存在
        email = self.cleaned_data.get('email')
        email_count = models.User.objects.filter(email=email).count() #从数据库中查找是否用户已经存在
        if email_count:
            raise ValidationError('该邮箱已经注册！')
        return email

    def _clean_new_password2(self): #查看两次密码是否一致
        password1 = self.cleaned_data.get('pwd')
        password2 = self.cleaned_data.get('pwd_again')
        if password1 and password2:
            if password1 != password2:
                # self.error_dict['pwd_again'] = '两次密码不匹配'
                raise ValidationError('两次密码不匹配！')

    def clean(self):
        #是基于form对象的验证，字段全部验证通过会调用clean函数进行验证
        self._clean_new_password2() #简单的调用而已
"""
