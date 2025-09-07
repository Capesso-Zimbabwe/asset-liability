from django import template

register = template.Library()

@register.filter
def getattribute(obj, attr):
    """Gets an attribute of an object dynamically from a string name"""
    return getattr(obj, attr) 