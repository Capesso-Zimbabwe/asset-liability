# forms.py

from django import forms
from django.core.validators import FileExtensionValidator
from .models import  HQLASection, CashInflowSection, CashOutflowSection, HQLAItem, CashInflowItem, CashOutflowItem, ASFSection, ASFItem, RSFSection, RSFItem

# Only allow Excel (.xls, .xlsx) and CSV (.csv) files
FILE_VALIDATOR = FileExtensionValidator(allowed_extensions=['xls', 'xlsx', 'csv'])

class HQLAUploadForm(forms.Form):
    file = forms.FileField(
        label="Select HQLA file",
        validators=[FILE_VALIDATOR],
        help_text="Upload an Excel (.xls, .xlsx) or CSV (.csv) file containing HQLA data."
    )

class CashInflowUploadForm(forms.Form):
    file = forms.FileField(
        label="Select Cash Inflow file",
        validators=[FILE_VALIDATOR],
        help_text="Upload an Excel (.xls, .xlsx) or CSV (.csv) file containing cash inflow data."
    )

class CashOutflowUploadForm(forms.Form):
    file = forms.FileField(
        label="Select Cash Outflow file",
        validators=[FILE_VALIDATOR],
        help_text="Upload an Excel (.xls, .xlsx) or CSV (.csv) file containing cash outflow data."
    )

class RequiredStableUploadForm(forms.Form):
    file = forms.FileField(
        label="Select Required Stable File",
        validators=[FILE_VALIDATOR],
        help_text="Upload an Excel (.xls, .xlsx) or CSV (.csv) file containing cash outflow data."
    )

class AvailableStableUploadForm(forms.Form):
    file = forms.FileField(
        label="Select Cash Outflow file",
        validators=[FILE_VALIDATOR],
        help_text="Upload an Excel (.xls, .xlsx) or CSV (.csv) file containing cash outflow data."
    )


class HQLASectionForm(forms.ModelForm):
    class Meta:
        model = HQLASection
        fields = ["section_name", "level", "weight", "parent"]

class CashInflowSectionForm(forms.ModelForm):
    class Meta:
        model = CashInflowSection
        fields = ["section_name", "weight", "parent"]

class CashOutflowSectionForm(forms.ModelForm):
    # runoff_rate instead of weight
    class Meta:
        model = CashOutflowSection
        fields = ["section_name", "runoff_rate", "parent"]

# LCR Items — names only (no weight here)
class HQLAItemForm(forms.ModelForm):
    class Meta:
        model = HQLAItem
        fields = ["section", "item_name"]

class CashInflowItemForm(forms.ModelForm):
    class Meta:
        model = CashInflowItem
        fields = ["section", "item_name"]

class CashOutflowItemForm(forms.ModelForm):
    class Meta:
        model = CashOutflowItem
        fields = ["section", "item_name"]

# NSFR — weights on Items, not Sections
class ASFSectionForm(forms.ModelForm):
    class Meta:
        model = ASFSection
        fields = ["section_name", "parent"]

class ASFItemForm(forms.ModelForm):
    class Meta:
        model = ASFItem
        fields = ["section", "item_name", "weight"]

class RSFSectionForm(forms.ModelForm):
    class Meta:
        model = RSFSection
        fields = ["section_name", "parent"]

class RSFItemForm(forms.ModelForm):
    class Meta:
        model = RSFItem
        fields = ["section", "item_name", "weight"]