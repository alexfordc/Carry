from django import template

register = template.Library()


@register.filter(name='names')
def names(d, i):
    """ 返回用户名，如果没
    有对应的名称则返回空字符串 """
    if isinstance(d,dict):
        name = d.get(i)
        return name if name else i
    return ''


@register.filter(name='name_option')
def name_option(d, i):
    """ 返回用户名，如果没
    有对应的名称则返回原ID """
    if isinstance(d,dict):
        name = d.get(i)
        return name if name else i
    return i
