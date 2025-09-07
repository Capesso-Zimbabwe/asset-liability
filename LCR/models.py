from django.db import models
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.fields import GenericForeignKey


class HQLASection(models.Model):
    LEVEL_CHOICES = [
        ('L1', 'Level 1'),
        ('L2A', 'Level 2A'),
        ('L2B', 'Level 2B'),
    ]

    section_id    = models.AutoField(primary_key=True)
    section_name  = models.CharField(max_length=100)
    level         = models.CharField(max_length=3, choices=LEVEL_CHOICES)
    display_order = models.PositiveIntegerField()
    weight        = models.DecimalField(max_digits=5, decimal_places=2)
    parent        = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='subsections'
    )

    def __str__(self):
        return self.section_name


class HQLAItem(models.Model):
    item_id       = models.AutoField(primary_key=True)
    section       = models.ForeignKey(
        HQLASection,
        on_delete=models.CASCADE,
        related_name='items'
    )
    item_name     = models.CharField(max_length=100)
    display_order = models.PositiveIntegerField()

    def __str__(self):
        return self.item_name

    class Meta:
        unique_together = (
            'section',
            'item_name',
        )


class HQLATable(models.Model):
    id                      = models.AutoField(primary_key=True)
    item                    = models.ForeignKey(
        HQLAItem,
        on_delete=models.CASCADE,
        related_name='data'
    )
    name                    = models.CharField(max_length=100)
    currency                = models.CharField(max_length=10)
    amount                  = models.DecimalField(max_digits=20, decimal_places=2)
    adjusted_amount         = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    aggregate_nominal_amount= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    adjusted_nominal_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    reporting_date          = models.DateField()

    def __str__(self):
        return f"{self.name} ({self.currency})"
    
    class Meta:
        unique_together = (
            'reporting_date',
            'name',
            'currency',
            'item'
        )




class CashOutflowSection(models.Model):
    section_id    = models.AutoField(primary_key=True)
    section_name  = models.CharField(max_length=100)
    category      = models.CharField(max_length=100)
    runoff_rate   = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    display_order = models.PositiveIntegerField()
    parent        = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='subsections'
    )

    def __str__(self):
        return self.section_name


class CashOutflowItem(models.Model):
    item_id       = models.AutoField(primary_key=True)
    section       = models.ForeignKey(
        CashOutflowSection,
        on_delete=models.CASCADE,
        related_name='items'
    )
    item_name     = models.CharField(max_length=100)
    display_order = models.PositiveIntegerField()

    def __str__(self):
        return self.item_name

    class Meta:
        unique_together = (
            'section',
            'item_name',
        )


class CashOutflowTable(models.Model):
    id                      = models.AutoField(primary_key=True)
    item                    = models.ForeignKey(
        CashOutflowItem,
        on_delete=models.CASCADE,
        related_name='data'
    )
    name                    = models.CharField(max_length=100)
    currency                = models.CharField(max_length=10)
    amount                  = models.DecimalField(max_digits=20, decimal_places=2)
    adjusted_amount         = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    aggregate_nominal_amount= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    adjusted_nominal_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    reporting_date          = models.DateField()

    def __str__(self):
        return f"{self.name} ({self.currency})"
    
    class Meta:
        unique_together = (
            'reporting_date',
            'name',
            'currency',
            'item'
        )




class CashInflowSection(models.Model):

    section_id    = models.AutoField(primary_key=True)
    section_name  = models.CharField(max_length=100)
    category      = models.CharField(max_length=100)
    weight        = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    display_order = models.PositiveIntegerField()
    parent        = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='subsections'
    )

    def __str__(self):
        return self.section_name


class CashInflowItem(models.Model):

    item_id       = models.AutoField(primary_key=True)
    section       = models.ForeignKey(
        CashInflowSection,
        on_delete=models.CASCADE,
        related_name='items'
    )
    item_name     = models.CharField(max_length=100)
    display_order = models.PositiveIntegerField()

    def __str__(self):
        return self.item_name

    class Meta:
        unique_together = (
            'section',
            'item_name',
        )

class CashInflowTable(models.Model):

    id                      = models.AutoField(primary_key=True)
    item                    = models.ForeignKey(
        CashInflowItem,
        on_delete=models.CASCADE,
        related_name='data'
    )
    name                    = models.CharField(max_length=100)
    currency                = models.CharField(max_length=10)
    amount                  = models.DecimalField(max_digits=20, decimal_places=2)
    adjusted_amount         = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    aggregate_nominal_amount= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    adjusted_nominal_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    reporting_date          = models.DateField()

    def __str__(self):
        return f"{self.name} ({self.currency})"

    class Meta:
        unique_together = (
            'reporting_date',
            'name',
            'currency',
            'item'
        )

class CurrencyAdjustmentSummary(models.Model):
    """
    Holds the per-date, per-currency totals of raw and adjusted amounts,
    as well as which table the data came from.
    """
    HQLA      = 'HQLA'
    INFLOW    = 'INFLOW'
    OUTFLOW   = 'OUTFLOW'
    TYPE_CHOICES = [
        (HQLA,    'HQLA'),
        (INFLOW,  'Cash Inflow'),
        (OUTFLOW, 'Cash Outflow'),
    ]

    reporting_date         = models.DateField()
    currency               = models.CharField(max_length=10)
    record_type            = models.CharField(
                                max_length=8,
                                choices=TYPE_CHOICES,
                                help_text="Source table for these totals"
                            )
    total_amount           = models.DecimalField(
                                max_digits=20,
                                decimal_places=2,
                                help_text="Sum of raw amounts"
                            )
    total_adjusted_amount  = models.DecimalField(
                                max_digits=20,
                                decimal_places=2,
                                help_text="Sum of adjusted amounts"
                            )
    created_at             = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('reporting_date', 'currency', 'record_type')
        ordering = ['reporting_date', 'record_type', 'currency']

    def __str__(self):
        return (
            f"{self.reporting_date} │ {self.record_type} │ {self.currency} "
            f"= {self.total_adjusted_amount}"
        )
    


class LCRRecord(models.Model):
    # Record type constants
    HQLA    = 'HQLA'
    INFLOW  = 'INFLOW'
    OUTFLOW = 'OUTFLOW'
    ASF     = 'ASF'     # Available Stable Funding
    RSF     = 'RSF'     # Required Stable Funding

    RECORD_TYPE_CHOICES = [
        (HQLA,   'HQLA'),
        (INFLOW, 'Cash Inflow'),
        (OUTFLOW,'Cash Outflow'),
        (ASF,    'ASF'),
        (RSF,    'RSF'),
    ]

    reporting_date        = models.DateField()
    currency              = models.CharField(max_length=3)
    amount_before_weights = models.DecimalField(max_digits=20, decimal_places=2)
    adjusted_amount       = models.DecimalField(max_digits=20, decimal_places=2)

    # --- generic FK to the source “Item” (CashInflowItem, CashOutflowItem, HQLAItem, ASFItem, RSFItem) ---
    item_content_type     = models.ForeignKey(
                                ContentType,
                                on_delete=models.CASCADE,
                                related_name='lcr_item_records'
                            )
    item_object_id        = models.PositiveIntegerField()
    item                  = GenericForeignKey('item_content_type', 'item_object_id')
    item_name             = models.CharField(
                                max_length=200,
                                help_text="Snapshot of the item's name at the time of LCR",
                                null=True,
                                blank=True
                            )

    # --- generic FK to the source “Section” (…Section variants for HQLA/Inflows/Outflows/ASF/RSF) ---
    section_content_type  = models.ForeignKey(
                                ContentType,
                                on_delete=models.CASCADE,
                                related_name='lcr_section_records'
                            )
    section_object_id     = models.PositiveIntegerField()
    section               = GenericForeignKey('section_content_type', 'section_object_id')
    section_name          = models.CharField(
                                max_length=200,
                                help_text="Snapshot of the section's name at the time of LCR",
                                null=True,
                                blank=True
                            )

    # widen to handle all choices (e.g., 'Available Stable Funding' label is long, but value is short)
    record_type           = models.CharField(max_length=16, choices=RECORD_TYPE_CHOICES)

    created_at            = models.DateTimeField(auto_now_add=True)
    updated_at            = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-reporting_date', 'currency']
        unique_together = [
            ('reporting_date', 'currency', 'record_type',
             'item_content_type', 'item_object_id')
        ]

    def __str__(self):
        return f"{self.reporting_date} | {self.currency} | {self.record_type}"
    



class LCRRun(models.Model):
    reporting_date = models.DateField()
    run_name       = models.CharField(max_length=32, unique=True)
    created_at     = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=50, blank=True)

    def __str__(self):
        return self.run_name
    



    

class ASFSection(models.Model):
    section_id    = models.AutoField(primary_key=True)
    section_name  = models.CharField(max_length=100,null=True, blank=True)
    display_order = models.PositiveIntegerField()
    parent        = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='subsections'
    )

    def __str__(self):
        return self.section_name



# 2) ASFItem: now holds the weight


class ASFItem(models.Model):
    item_id       = models.AutoField(primary_key=True)
    section       = models.ForeignKey(
        ASFSection,
        on_delete=models.CASCADE,
        related_name='items'
    )
    item_name     = models.CharField(max_length=100)
    display_order = models.PositiveIntegerField()
    # <-- moved weight here:
    weight        = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)

    class Meta:
        unique_together = ('section', 'item_name')

    def __str__(self):
        return f"{self.section.section_name} / {self.item_name}"



# 3) ASFTable: actual data rows


class ASFTable(models.Model):
    id               = models.AutoField(primary_key=True)
    item             = models.ForeignKey(
        ASFItem,
        on_delete=models.CASCADE,
        related_name='data'
    )
    name             = models.CharField(max_length=100)
    currency         = models.CharField(max_length=10)
    amount           = models.DecimalField(max_digits=20, decimal_places=2)
    adjusted_amount  = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    reporting_date   = models.DateField()

    class Meta:
        unique_together = ('reporting_date', 'name', 'currency', 'item')

    def __str__(self):
        return f"{self.name} ({self.currency})"
    
class RSFSection(models.Model):
    section_id    = models.AutoField(primary_key=True)
    section_name  = models.CharField(max_length=100)
    display_order = models.PositiveIntegerField()
    parent        = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='subsections'
    )

    def __str__(self):
        return self.section_name



# 2) RSFItem: holds the run-off rate


class RSFItem(models.Model):
    item_id       = models.AutoField(primary_key=True)
    section       = models.ForeignKey(
        RSFSection,
        on_delete=models.CASCADE,
        related_name='items'
    )
    item_name     = models.CharField(max_length=100)
    display_order = models.PositiveIntegerField()
    # <-- the RSF run-off rate here
    weight        = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)

    class Meta:
        unique_together = ('section', 'item_name')

    def __str__(self):
        return f"{self.section.section_name} / {self.item_name}"



# 3) RSFTable: your raw cash-outflow rows


class RSFTable(models.Model):
    id               = models.AutoField(primary_key=True)
    item             = models.ForeignKey(
        RSFItem,
        on_delete=models.CASCADE,
        related_name='data'
    )
    name             = models.CharField(max_length=100)
    currency         = models.CharField(max_length=10)
    amount           = models.DecimalField(max_digits=20, decimal_places=2)
    adjusted_amount  = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    reporting_date   = models.DateField()

    class Meta:
        unique_together = ('reporting_date', 'name', 'currency', 'item')

    def __str__(self):
        return f"{self.name} ({self.currency})"


