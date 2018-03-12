from django import template

register=template.Library()

@register.filter(name='names')
def names(d,i):
    name=d.get(i)
    if not name:
        name='暂无'
    return name
