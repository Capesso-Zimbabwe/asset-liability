from django import template

register = template.Library()

@register.filter
def field_type(field):
    """
    Return the field type of a form field
    """
    return field.field.widget.__class__.__name__.lower()

@register.filter
def is_checkbox(field):
    """
    Check if field is a checkbox
    """
    return field_type(field) == 'checkbox'

@register.filter
def add_classes(field, classes):
    """
    Add CSS classes to form field
    """
    return field.as_widget(attrs={"class": classes}) 