from django import template

register = template.Library()

@register.filter
def lookup(dictionary, key):
    """
    Custom template filter to look up dictionary values by key.
    Usage: {{ dict|lookup:key }}
    """
    if dictionary is None:
        return None
    return dictionary.get(key, 0)

@register.filter
def multiply(value, arg):
    """
    Multiply filter for template calculations.
    Usage: {{ value|multiply:arg }}
    """
    try:
        return float(value) * float(arg)
    except (ValueError, TypeError):
        return 0

@register.filter
def divide(value, arg):
    """
    Divide filter for template calculations.
    Usage: {{ value|divide:arg }}
    """
    try:
        if float(arg) == 0:
            return 0
        return float(value) / float(arg)
    except (ValueError, TypeError):
        return 0

@register.filter
def percentage(value, total):
    """
    Calculate percentage.
    Usage: {{ value|percentage:total }}
    """
    try:
        if float(total) == 0:
            return 0
        return (float(value) / float(total)) * 100
    except (ValueError, TypeError):
        return 0

@register.filter
def currency(value):
    """
    Format currency values.
    Usage: {{ value|currency }}
    """
    try:
        return "{:,.2f}".format(float(value))
    except (ValueError, TypeError):
        return "0.00" 