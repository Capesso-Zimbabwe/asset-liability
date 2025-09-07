from django import template

register = template.Library()

@register.filter
def get_field(form, field_name):
    """Get a form field by name"""
    return form[field_name]