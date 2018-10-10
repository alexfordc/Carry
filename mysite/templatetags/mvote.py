from django import template
from django.utils.safestring import SafeData, mark_safe
from django.utils.html import escape
from django.utils.text import normalize_newlines

register = template.Library()


@register.filter(name='names')
def names(d, i):
    """ 返回用户名，如果没
    有对应的名称则返回空字符串 """
    if isinstance(d, dict):
        name = d.get(i)
        return name if name else i
    return ''


@register.filter(name='name_option')
def name_option(d, i):
    """ 返回用户名，如果没
    有对应的名称则返回原ID """
    if isinstance(d, dict):
        name = d.get(int(i) if isinstance(i, str) and i.isdigit() else i)
        return name if name else i
    return i


@register.filter(name='linebreaksbrs')
def linebreaksbrs(value, autoescape=True):
    """ 处理换行、空格 """
    autoescape = autoescape and not isinstance(value, SafeData)
    value = normalize_newlines(value)
    if autoescape:
        value = escape(value)
    try:
        return mark_safe(value.replace('\r\n', '<br/>').replace('\n', '<br/>')
                         .replace('\t', '&nbsp;&nbsp;&nbsp;&nbsp;').replace(' ', '&nbsp;'))
    except:
        return value
