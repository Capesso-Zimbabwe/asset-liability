from django.utils import timezone
from django.db import models
from django.forms import ValidationError

# Create your models here.

class FSI_Loans_Processing(models.Model):
    fic_mis_date = models.DateField(null=True)
    v_account_number = models.CharField(max_length=255, unique=True, null=False)
    v_cust_ref_code = models.CharField(max_length=50, null=True)
    v_prod_code = models.CharField(max_length=50, null=True)
    n_curr_interest_rate = models.DecimalField(max_digits=5, decimal_places=2, null=True, help_text="Fixed interest rate for the loan")    
    # The changing interest rate (e.g., LIBOR or SOFR)
    n_interest_changing_rate = models.DecimalField(max_digits=5, decimal_places=4, null=True, help_text="Changing interest rate value, e.g., LIBOR rate at a specific time")   
    v_interest_freq_unit = models.CharField(max_length=50, null=True)
    v_interest_payment_type = models.CharField(max_length=50, null=True)
    v_day_count_ind= models.CharField(max_length=7,default='30/365', help_text="This column stores the accrual basis code for interest accrual calculation.")
    # New fields for variable rate and fees   
    v_management_fee_rate = models.DecimalField(max_digits=5, decimal_places=2, null=True, help_text="Annual management fee rate, e.g., 1%")
    n_wht_percent= models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_effective_interest_rate = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    n_accrued_interest = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    d_start_date = models.DateField(null=True)
    d_last_payment_date = models.DateField(null=True)
    d_next_payment_date = models.DateField(null=True)
    d_maturity_date = models.DateField(null=True)
    v_amrt_repayment_type = models.CharField(max_length=50, null=True)
    v_amrt_term_unit = models.CharField(max_length=50, null=True)
    n_eop_curr_prin_bal = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_eop_int_bal = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_eop_bal = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_curr_payment_recd= models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_collateral_amount = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_acct_risk_score = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    v_ccy_code = models.CharField(max_length=10, null=True)
    v_loan_type = models.CharField(max_length=50, null=True)
    m_fees = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    v_m_fees_term_unit=models.CharField(max_length=1, null=True)
    v_lob_code = models.CharField(max_length=50, null=True)
    v_lv_code = models.CharField(max_length=50, null=True)
    v_country_id = models.CharField(max_length=50, null=True)
    v_credit_score = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    v_collateral_type = models.CharField(max_length=50, null=True)
    v_loan_desc = models.CharField(max_length=255, null=True)
    v_account_classification_cd = models.CharField(max_length=50, null=True)
    v_gaap_code = models.CharField(max_length=50, null=True)
    v_branch_code = models.CharField(max_length=50, null=True)
    v_facility_code = models.CharField( blank=True, max_length=50,null=True)


    class Meta:
        db_table = 'Fsi_ALM_Processing'



class stg_party_master(models.Model):
    fic_mis_date = models.DateField()
    v_party_id = models.CharField(max_length=50, unique=True) 
    v_partner_name = models.CharField(max_length=50)
    v_party_type = models.CharField(max_length=50, null=True )
    v_party_type_code = models.CharField(max_length=50, null=True)
    v_party_type_name = models.CharField(max_length=50, null=True)
    
    class Meta:
        db_table = 'stg_party_master'


class stg_payment_schedule(models.Model):
    fic_mis_date = models.DateField(null=False)
    v_account_number = models.CharField(max_length=50, null=False)
    d_payment_date = models.DateField(null=False)
    n_principal_payment_amt = models.DecimalField(max_digits=22, decimal_places=3, null=True)
    n_interest_payment_amt = models.DecimalField(max_digits=22, decimal_places=3, null=True)
    n_amount = models.DecimalField(max_digits=22, decimal_places=3, null=True)
    v_payment_type_cd = models.CharField(max_length=20, null=True)  # Payment type code
    class Meta:
        db_table = 'stg_payment_schedule'



class FSI_Expected_Cashflow(models.Model):
    fic_mis_date = models.DateField()
    v_account_number = models.CharField(max_length=50)
    v_loan_type = models.CharField(max_length=50)
    v_cust_ref_code=models.CharField(max_length=50, null=True)
    v_party_type_code = models.CharField(max_length=50, null=True)
    n_cash_flow_bucket = models.IntegerField() 
    d_cash_flow_date = models.DateField()
    n_principal_payment = models.DecimalField(max_digits=20, decimal_places=2)
    n_interest_payment = models.DecimalField(max_digits=20, decimal_places=2)
    n_cash_flow_amount = models.DecimalField(max_digits=20, decimal_places=2)
    n_balance = models.DecimalField(max_digits=20, decimal_places=2)
    n_accrued_interest = models.DecimalField(max_digits=22, decimal_places=3, null=True, blank=True)  # Accrued interest
    n_exposure_at_default = models.DecimalField(max_digits=22, decimal_places=3, null=True, blank=True) 
    v_cash_flow_type = models.CharField(max_length=10)
    management_fee_added = models.DecimalField(max_digits=20, decimal_places=2)
    v_ccy_code = models.CharField(max_length=3)

    class Meta:
        db_table = 'FSI_Expected_Cashflow'
        unique_together = ('fic_mis_date', 'v_account_number', 'd_cash_flow_date')


class Fsi_Interest_Method(models.Model):
    # Define choices for the interest method
    INTEREST_METHOD_CHOICES = [('Simple', 'Simple Interest'), ('Compound', 'Compound Interest'),('Amortized', 'Amortized Interest'),('Floating', 'Floating/Variable Interest'),]
    
    v_interest_method = models.CharField( max_length=50, choices=INTEREST_METHOD_CHOICES,unique=True)
    description = models.TextField(blank=True)  # Optional description for documentation
  

    class Meta:
        db_table = 'Fsi_Interest_Method'


class Stg_Product_Master(models.Model):  # Class name with underscores
    v_prod_code = models.CharField(max_length=20, null=False)  # VARCHAR2(20)
    fic_mis_date = models.DateField(null=False)  # DATE
    v_prod_name = models.CharField(max_length=255, null=True, blank=True)  # VARCHAR2(255)
    v_prod_type = models.CharField(max_length=255, null=True, blank=True)  # VARCHAR2(20)
    v_prod_group_desc = models.CharField(max_length=255, null=True, blank=True)  # VARCHAR2(255)
    f_prod_rate_sensitivity = models.CharField(max_length=1, null=True, blank=True)  # VARCHAR2(1)
    v_common_coa_code = models.CharField(max_length=20, null=True, blank=True)  # VARCHAR2(20)
    v_balance_sheet_category = models.CharField(max_length=20, null=True, blank=True)  # VARCHAR2(20)
    v_balance_sheet_category_desc = models.CharField(max_length=255, null=True, blank=True)  # VARCHAR2(255)
    v_prod_type_desc = models.CharField(max_length=255, null=True, blank=True)  # VARCHAR2(255)
    v_load_type = models.CharField(max_length=20, null=True, blank=True)  # VARCHAR2(20)
    v_lob_code = models.CharField(max_length=20, null=True, blank=True)  # VARCHAR2(20)
    v_prod_desc = models.CharField(max_length=255, null=True, blank=True)  # VARCHAR2(255)

    class Meta:
        db_table = 'Stg_Product_Master'  # Explicitly set the table name



class Stg_Common_Coa_Master(models.Model):  # Class with underscores in the name
    v_common_coa_code = models.CharField(max_length=20, null=True, blank=True)  # VARCHAR2(20 CHAR)
    v_common_coa_name = models.CharField(max_length=150, null=True, blank=True)  # VARCHAR2(150 CHAR)
    v_common_coa_description = models.CharField(max_length=60, null=True, blank=True)  # VARCHAR2(60 CHAR)
    v_accrual_basis_code = models.CharField(max_length=10, null=True, blank=True)  # VARCHAR2(10 CHAR)
    v_account_type = models.CharField(max_length=20, null=True, blank=True)  # VARCHAR2(20 CHAR)
    fic_mis_date = models.DateField(null=True, blank=True)  # DATE
    v_rollup_signage_code = models.CharField(max_length=5, null=True, blank=True)  # VARCHAR2(5 CHAR)
    d_start_date = models.DateField(null=True, blank=True)  # DATE
    d_end_date = models.DateField(null=True, blank=True)  # DATE

    class Meta:
        db_table = 'Stg_COMMON_COA_MASTER'  # Explicitly set the table name


class Arranged_cashflows(models.Model):
    fic_mis_date = models.DateField(null=False)
    v_account_number = models.CharField(max_length=255, null=False)
    v_prod_code = models.CharField(max_length=50, null=False)
    v_loan_type = models.CharField(max_length=50, null=True)
    v_party_type_code = models.CharField(max_length=50, null=True)
    v_cash_flow_type = models.CharField(max_length=50, null=True, blank=True)  # Example field for cash flow type
    n_cash_flow_bucket = models.IntegerField() 
    d_cashflow_date = models.DateField()  # New field to store the cashflow date
    n_total_cash_flow_amount = models.DecimalField(max_digits=20, decimal_places=2)
    n_total_principal_payment = models.DecimalField(max_digits=20, decimal_places=2)
    n_total_interest_payment = models.DecimalField(max_digits=20, decimal_places=2)
    n_total_balance = models.DecimalField(max_digits=20, decimal_places=2)
    v_ccy_code = models.CharField(max_length=10)
    record_count = models.IntegerField(default=0)


    class Meta:
        db_table = 'Fsi_Arranged_cashflows'




# New model for pattern entries (bucket number and percentage)
# models.py  (or behavioural_patterns.py)

from decimal import Decimal
from django.core.exceptions import ValidationError
from django.db import models


class BehavioralPattern(models.Model):
    """
    Header row – one per product type (LOANS, DEPOSITS, …).
    """
    v_prod_type  = models.CharField(
        max_length=255,
        unique=True,                       # product type written only once
        help_text="Product type exactly as it appears in Report tables",
    )
    description  = models.TextField(blank=True)

    class Meta:
        db_table  = "Behavioral_Pattern"
        ordering  = ["v_prod_type"]

    def __str__(self):
        return self.v_prod_type

    # handy helper for loaders
    def bucket_map(self) -> dict[int, Decimal]:
        """
        Returns {bucket_number: Decimal(percentage)} for this pattern.
        """
        return {
            row.bucket_number: row.percentage
            for row in self.splits.all()
        }

    def clean(self):
        # ensure the splits sum to 100 %
        total = sum(s.percentage for s in self.splits.all())
        if total != 100:
            raise ValidationError(
                f"Sum of bucket percentages for {self.v_prod_type} "
                f"must equal 100 % (is {total})"
            )


class BehavioralPatternSplit(models.Model):
    """
    Detail rows – one per bucket for a given BehavioralPattern.
    """
    pattern       = models.ForeignKey(
        BehavioralPattern,
        related_name="splits",
        on_delete=models.CASCADE,
    )
    bucket_number = models.PositiveIntegerField()
    percentage    = models.DecimalField(max_digits=6, decimal_places=3)
    note          = models.CharField(max_length=255, blank=True)

    class Meta:
        db_table        = "Behavioral_Pattern_Split"
        unique_together = ("pattern", "bucket_number")
        ordering        = ["pattern", "bucket_number"]

    def __str__(self):
        return (
            f"{self.pattern.v_prod_type} – bucket {self.bucket_number}: "
            f"{self.percentage}%"
        )

    def clean(self):
        if not (0 <= self.percentage <= 100):
            raise ValidationError("Percentage must be between 0 and 100.")




class TimeBucketMaster(models.Model):
    process_name = models.CharField(max_length=100)  # Name of the process (e.g., "Process X")
    bucket_number = models.IntegerField()  # Bucket number (1 to N)
    start_date = models.DateField()  # Start date of the time bucket
    end_date = models.DateField()  # End date of the time bucket
    created_at = models.DateTimeField(auto_now_add=True)  # Timestamp of bucket creation
    fic_mis_date = models.DateField(null=True)  # Anchor date for which buckets were generated

    class Meta:
        db_table = 'Time_Bucket_Master'
        unique_together = ('process_name', 'fic_mis_date', 'bucket_number')



class TimeBucketDefinition(models.Model):
    name = models.CharField(max_length=100)  # Name of the time bucket set
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=100, default="System")
    last_changed_at = models.DateTimeField(auto_now=True)
    last_changed_by = models.CharField(max_length=100, default="System")

    class Meta:
        db_table = 'Time_Bucket_Definition'


class TimeBuckets(models.Model):
    serial_number = models.IntegerField()  # Bucket number (e.g., 1, 2, 3)
    start_date = models.DateField()  # Start date for this time bucket
    end_date = models.DateField()  # End date for this time bucket
    frequency = models.IntegerField()  # Frequency as an integer (e.g., 7 days, 3 months)
    multiplier = models.CharField(max_length=20)  # Days, Months, Years


    class Meta:
        db_table = 'Time_buckets'
        ordering = ['serial_number']  # Order buckets by their number


class ProductFilter(models.Model):
    field_name = models.CharField(max_length=50)  # Name of the field to filter by
    condition = models.CharField(max_length=50)  # Type of condition (equals, contains, etc.)
    value = models.CharField(max_length=255)  # Value to filter with
    created_by = models.CharField(max_length=50, default='System')
    created_at = models.DateTimeField(default=timezone.now)
    modified_by = models.CharField(max_length=50, default='System')
    modified_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'alm_Product_Filter'


class Process(models.Model):
    name = models.CharField(max_length=100)  # Name of the process (e.g., 'contractual', 'forecast', etc.
    description = models.TextField(null=True, blank=True)  # Optional description for the process
    uses_behavioral_patterns = models.BooleanField(default=False)
    filters = models.ManyToManyField(ProductFilter, related_name='processes')
    execution_date = models.DateTimeField(null=True, blank=True)  # Optional field to track last execution date
    status = models.CharField(max_length=20, default='Pending')  # Track status (e.g., 'Pending', 'Completed')
    created_by = models.CharField(max_length=50, default='System')
    created_at = models.DateTimeField(default=timezone.now)
    modified_by = models.CharField(max_length=50, default='System')
    modified_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'alm_Process'


class ExecutionHistory(models.Model):
    """Model to track financial function executions"""
    fic_mis_date = models.DateField()  # Financial MIS date for the execution
    run_number = models.IntegerField(default=1)  # Incremental run number for the same date
    process_name = models.CharField(max_length=100,null=True, blank=True)  # Name of the specific function/process
    status = models.CharField(max_length=20, default='Running')  # Track status (e.g., 'Running', 'Success', 'Failed')
    start_time = models.DateTimeField()  # When the process started
    end_time = models.DateTimeField(null=True, blank=True)  # When the process ended
    execution_time = models.FloatField(null=True, blank=True)  # Time taken in seconds
    error_message = models.TextField(null=True, blank=True)  # Error message if process failed
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'alm_ExecutionHistory'
        ordering = ['-fic_mis_date', '-run_number', 'start_time']
        unique_together = ['fic_mis_date', 'run_number', 'process_name']


class Aggregated_Acc_CashflowByBuckets(models.Model):
    fic_mis_date = models.DateField()  # The base date from product_level_cashflows
    process_name = models.CharField(max_length=100)  # Process name to identify different cashflow processes
    v_account_number = models.CharField(max_length=50)  # Account number being aggregated
    v_prod_code = models.CharField(max_length=50)
    v_party_type_code = models.CharField(max_length=50, null=True)
  # Product code to identify the product
    v_loan_type = models.CharField(max_length=50, null=True)
    v_ccy_code = models.CharField(max_length=10, null=True, blank=True)  # Optional currency code
    financial_element = models.CharField(max_length=50)  # Either 'n_total_cash_flow_amount', 'n_total_principal_payment', or 'n_total_interest_payment'
     # Foreign Key to TimeBucketMaster
    time_bucket_master = models.ForeignKey(TimeBucketMaster, on_delete=models.CASCADE, null=True)
    bucket_1 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 1
    bucket_2 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 2
    bucket_3 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_4 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_5 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_6 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_7 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_8 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_9 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_10 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_11 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_12 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_13= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_14 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_15= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_16 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_17 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_18 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_19= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_20= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_21 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_22 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_23= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_24 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_25 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_26 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_27 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_28 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_29 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_30 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_31 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_32 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_33 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_34 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_35 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_36 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_37 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_38 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_39 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_40 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_41 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_42 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_43 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_44 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_45 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_46 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_47 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_48 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_49 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_50 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 50

    class Meta:
        db_table = 'Fsi_Aggregated_Acc_Cashflow'



class Aggregated_Prod_Cashflow_Base(models.Model):
    fic_mis_date = models.DateField()  # The base date from product_level_cashflows
    process_name = models.CharField(max_length=100)  # Process name to identify different cashflow processes
    v_loan_type = models.CharField(max_length=50, null=True)
    v_party_type_code = models.CharField(max_length=50, null=True)
    v_prod_code = models.CharField(max_length=50)  # Product code  being aggregated
    v_ccy_code = models.CharField(max_length=10, null=False, blank=False)  #  currency code
    financial_element = models.CharField(max_length=50)  # Either 'n_total_cash_flow_amount', 'n_total_principal_payment', or 'n_total_interest_payment'
    # Foreign Key to AggregatedCashflowByBucket
    cashflow_by_bucket = models.ForeignKey(Aggregated_Acc_CashflowByBuckets, on_delete=models.CASCADE, null=True)

    # Foreign Key to TimeBucketMaster
    time_bucket_master = models.ForeignKey(TimeBucketMaster, on_delete=models.CASCADE, null=True)

    bucket_1 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 1
    bucket_2 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 2
    bucket_3 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 3
    bucket_4 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 4
    bucket_5 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 5
    bucket_6 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 6
    bucket_7 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 7
    bucket_8 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 8
    bucket_9 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 9
    bucket_10 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 10
    bucket_11 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 11
    bucket_12 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 12
    bucket_13= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 13
    bucket_14 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 14
    bucket_15= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 15
    bucket_16 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 16
    bucket_17 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 17
    bucket_18 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 18
    bucket_19= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 19
    bucket_20= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 20
    bucket_21 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 21
    bucket_22 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 22
    bucket_23= models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 23
    bucket_24 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 24
    bucket_25 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 25
    bucket_26 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 26
    bucket_27 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 27
    bucket_28 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 28
    bucket_29 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 29
    bucket_30 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 30
    bucket_31 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 31
    bucket_32 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 32
    bucket_33 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 33
    bucket_34 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 34
    bucket_35 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 35
    bucket_36 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 36
    bucket_37 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 37
    bucket_38 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 38
    bucket_39 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 39
    bucket_40 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 40
    bucket_41 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 41
    bucket_42 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 42
    bucket_43 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 43
    bucket_44 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 44
    bucket_45 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 45
    bucket_46 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 46
    bucket_47 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 47
    bucket_48 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 48
    bucket_49 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 49
    bucket_50 = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)  # Sum for bucket 50

    class Meta:
        db_table = 'Fsi_Aggregated_Prod_Cashflow'




##################################################################################################

from django.db import models


class Report_Contractual_Base(models.Model):
    fic_mis_date = models.DateField()
    process_name = models.CharField(max_length=100)
    v_loan_type = models.CharField(max_length=50, null=True)
    v_party_type_code = models.CharField(max_length=50, null=True)
    v_prod_code = models.CharField(max_length=50)
    v_product_name = models.CharField(max_length=255, null=True, blank=True)
    v_product_splits = models.CharField(max_length=255, null=True, blank=True)
    v_prod_type = models.CharField(max_length=255)
    v_ccy_code = models.CharField(max_length=10)
    financial_element = models.CharField(max_length=50)
    
    time_bucket_master = models.ForeignKey(
        'TimeBucketMaster',
        on_delete=models.CASCADE,
        null=True,
    )
    cashflow_by_bucket = models.ForeignKey(
        'Aggregated_Prod_Cashflow_Base',  # or whatever the FK points to
        on_delete=models.CASCADE,
        null=True,
    )
    account_type = models.CharField(max_length=20)
    v_prod_type_desc = models.CharField(max_length=255, null=True, blank=True)
    flow_type = models.CharField(max_length=255, null=True, blank=True)
    n_adjusted_cash_flow_amount = models.DecimalField(
        max_digits=20,
        decimal_places=2,
        null=True,
        blank=True,
    )

    # Dynamic bucket-range columns will be added later by a sync function.
    # This makes them accessible as attributes (e.g. bucket_1_20250101_20250331).
    def __getattr__(self, item):
        if item.startswith("bucket_"):
            return self.__dict__.get(item)
        raise AttributeError(item)

    class Meta:
        db_table = 'Report_Contractual_Base'



class LCRRun(models.Model):
    reporting_date = models.DateField()
    run_name       = models.CharField(max_length=32, unique=True)
    created_at     = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.run_name



    




class Stg_Exchange_Rate(models.Model): 
    fic_mis_date = models.DateField()
    v_from_ccy_code = models.CharField(max_length=3)
    v_to_ccy_code = models.CharField(max_length=3)
    n_exchange_rate = models.DecimalField(max_digits=15, decimal_places=6)




    class Meta:
        db_table = "Stg_Exchange_Rate" 
        unique_together = ('fic_mis_date', 'v_from_ccy_code', 'v_to_ccy_code')
        ordering = ['fic_mis_date']


from django.db import models


class ProductBalance(models.Model):
    """
    Stores daily MIS (Management Information System) balances
    for each product.
    """

    fic_mis_date = models.DateField(help_text="Reporting date")
    v_prod_code = models.CharField(max_length=30, help_text="Internal product code")
    v_prod_type = models.CharField(max_length=50, help_text="Product category / type")
    v_prod_name = models.CharField(max_length=200, help_text="Human-readable product name")
    n_balance = models.DecimalField(
        max_digits=40,
        decimal_places=10,
        help_text="Closing balance for the day",
    )
    v_ccy_code = models.CharField(max_length=3)


    class Meta:
        db_table = "product_balance"      # optional: custom table name
        verbose_name = "Product Balance"
        verbose_name_plural = "Product Balances"
        unique_together = ("fic_mis_date", "v_prod_code")  # optional: prevent duplicates
        ordering = ["-fic_mis_date", "v_prod_code"]

    def __str__(self):
        return f"{self.fic_mis_date} • {self.v_prod_code} • {self.n_balance}"
