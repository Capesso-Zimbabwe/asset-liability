
from django.db import models
from django.core.validators import MinValueValidator
from decimal import Decimal



class LoanContract(models.Model):
    # Required fields (no null)
    v_account_number = models.CharField(
        max_length=50,
        help_text="Unique account identifier"
    )
    fic_mis_date = models.DateField(
        help_text="Financial Information Control MIS date"
    )
    v_prod_code = models.CharField(
        max_length=50,
        help_text="Product code identifier"
    )

    # Optional fields (can be null)
    v_cust_ref_code = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        help_text="Customer reference code"
    )
    d_book_date = models.DateField(
        null=True,
        blank=True,
        help_text="Date when the loan was booked"
    )
    d_maturity_date = models.DateField(
        null=True,
        blank=True,
        help_text="Loan maturity date"
    )
    d_next_payment_date = models.DateField(
        null=True,
        blank=True,
        help_text="Next scheduled payment date"
    )
    d_last_payment_date = models.DateField(
        null=True,
        blank=True,
        help_text="Last payment date"
    )
    d_value_date = models.DateField(
        null=True,
        blank=True,
        help_text="Value date of the loan"
    )
    n_eop_bal = models.DecimalField(
        max_digits=20,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="End of period balance",
        validators=[MinValueValidator(Decimal('0.00'))]
    )
    n_curr_interest_rate = models.DecimalField(
        max_digits=7,
        decimal_places=4,
        null=True,
        blank=True,
        help_text="Current interest rate for the loan",
        validators=[MinValueValidator(Decimal('0.00'))]
    )
    v_interest_type = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        help_text="Type of interest (e.g., Fixed, Variable)"
    )
    v_amrt_repayment_type = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        help_text="Amortization repayment type"
    )
    n_amrt_term = models.IntegerField(
        null=True,
        blank=True,
        help_text="Amortization term"
    )
    v_amrt_term_unit = models.CharField(
        max_length=20,
        null=True,
        blank=True,
        default='D',
        help_text="Unit for amortization term (e.g., D=Days, M=Months, Y=Years)"
    )
    v_amrt_type_cd = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        help_text="Amortization type code"
    )
    n_remain_no_of_pmts = models.IntegerField(
        null=True,
        blank=True,
        help_text="Remaining number of payments"
    )
    n_interest_freq = models.IntegerField(
        null=True,
        blank=True,
        help_text="Interest payment frequency"
    )
    v_interest_freq_unit = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        help_text="Unit for interest frequency (e.g., Monthly, Quarterly)"
    )
    v_day_count_ind = models.CharField(
        max_length=7,
        null=True,
        blank=True,
        default='30/365',
        help_text="Day count indicator for interest calculation (e.g., 30/365)"
    )
    v_ccy_code = models.CharField(
        max_length=10,
        null=True,
        blank=True,
        help_text="Currency code"
    )
    n_curr_payment_recd = models.DecimalField(
        max_digits=20,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Current payment received",
        validators=[MinValueValidator(Decimal('0.00'))]
    )
    v_instrument_type_cd = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        default="",
        help_text="Instrument type code"
    )
    v_amrt_freq = models.CharField(
        max_length=1,
        null=True,
        blank=True,
        help_text="Amortization frequency (e.g., M=Monthly, Q=Quarterly)"
    )

    class Meta:
        db_table = 'Stg_Loan_Contracts'
        constraints = [
            models.UniqueConstraint(
                fields=['v_account_number', 'fic_mis_date'],
                name='uq_%(app_label)s_%(class)s_acc_ficmis',
            ),
        ]


        
class OverdraftContract(models.Model):
    v_account_number = models.CharField(max_length=50)
    fic_mis_date = models.DateField()
    v_cust_ref_code = models.CharField(max_length=50, null=True,blank=True)
    v_prod_code = models.CharField(max_length=50)
    d_book_date = models.DateField(null=True,blank=True)
    d_maturity_date = models.DateField(null=True,blank=True)
    d_next_payment_date = models.DateField(null=True,blank=True)
    d_last_payment_date = models.DateField(null=True,blank=True)
    d_value_date = models.DateField(null=True, blank=True)
    n_eop_bal = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_curr_interest_rate = models.DecimalField(max_digits=5, decimal_places=2, null=True,blank=True, help_text="Fixed interest rate for the loan")    
    v_interest_type = models.CharField(max_length=50, null=True, blank=True)
    v_amrt_repayment_type = models.CharField(max_length=50, null=True,blank=True)
    n_amrt_term = models.IntegerField(null=True, blank=True)
    v_amrt_term_unit = models.CharField(max_length=20, default='D', null=True, blank=True)
    v_amrt_type_cd = models.CharField(max_length=50, null=True, blank=True)
    n_remain_no_of_pmts = models.IntegerField(null=True, blank=True)
    n_interest_freq = models.IntegerField(null=True, blank=True)
    v_interest_freq_unit = models.CharField(max_length=50, null=True, blank=True)
    v_day_count_ind= models.CharField(max_length=7,null=True,blank=True,default='30/365', help_text="This column stores the accrual basis code for interest accrual calculation.")
    v_ccy_code = models.CharField(max_length=10, null=True)
    n_curr_payment_recd= models.DecimalField(max_digits=10, decimal_places=2,null=True,blank=True)
    v_instrument_type_cd = models.CharField(max_length=50,null=True,blank=True, default="")
    v_amrt_freq = models.CharField(max_length=1,null=True,blank=True)


    class Meta:
        db_table = 'Stg_Overdraft_Contracts'
        constraints = [
            models.UniqueConstraint(
                fields=['v_account_number', 'fic_mis_date'],
                name='uq_%(app_label)s_%(class)s_acc_ficmis',
            ),
        ]
               
        
class LoanPaymentSchedule(models.Model):
    v_account_number = models.CharField(max_length=50)
    fic_mis_date = models.DateField()
    d_next_payment_date = models.DateField()
    v_instrument_type_cd = models.CharField(max_length=50)
    n_amount = models.DecimalField(max_digits=15, decimal_places=2)
    n_leg_type = models.IntegerField(null=True,blank=True)
    v_cal_option_display_cd = models.CharField(max_length=10, blank=True, null=True)
    v_holiday_display_cd = models.CharField(max_length=10, blank=True, null=True)
    n_principal_payment_amnt = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    n_interest_payment_amt = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    n_rate = models.DecimalField(max_digits=7, decimal_places=2,null=True,blank=True)
    n_notional_balance = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    n_level=models.CharField(max_length=10)

    class Meta:
        db_table = 'Stg_Loan_Payment_Schedules'
    


class CASA(models.Model):
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    D_ACCT_OPEN_DATE = models.DateField()
    D_MATURITY_DATE = models.DateField(null=True, blank=True)
    D_LAST_PAYMENT_DATE = models.DateField()
    N_EOP_BALANCE_SAVINGS = models.DecimalField(max_digits=15, decimal_places=2)
    N_CURRENT_INT_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    V_INTEREST_TYPE = models.CharField(max_length=50)
    N_INTEREST_FREQ = models.IntegerField()
    V_INTEREST_FREQ_UNIT = models.CharField(max_length=20)
    V_DAY_COUNT_IND = models.CharField(max_length=20)
    N_AMRT_TERM = models.IntegerField(null=True, blank=True)
    V_AMRT_TERM_UNIT = models.CharField(max_length=20, null=True, blank=True)
    V_AMRT_TYPE = models.CharField(max_length=20)
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50, default="")
    

    class Meta:
        db_table = 'Stg_Casa'


    

class Investment(models.Model):
    v_account_number = models.CharField(max_length=50)
    fic_mis_date = models.DateField()
    v_cust_ref_code = models.CharField(max_length=50, null=True,blank=True)
    v_prod_code = models.CharField(max_length=50)
    d_book_date = models.DateField(null=True,blank=True)
    d_maturity_date = models.DateField(null=True,blank=True)
    d_next_payment_date = models.DateField(null=True,blank=True)
    d_last_payment_date = models.DateField(null=True,blank=True)
    d_value_date = models.DateField(null=True, blank=True)
    n_eop_bal = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_accr_int_amt = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    n_curr_interest_rate = models.DecimalField(max_digits=5, decimal_places=2, null=True, help_text="Fixed interest rate for the loan")    
    v_interest_type = models.CharField(max_length=50, null=True, blank=True)
    v_amrt_repayment_type = models.CharField(max_length=50, null=True,blank=True)
    n_amrt_term = models.IntegerField(null=True, blank=True)
    v_amrt_term_unit = models.CharField(max_length=20, default='D', null=True, blank=True)
    v_amrt_type_cd = models.CharField(max_length=50, null=True, blank=True)
    n_remain_no_of_pmts = models.IntegerField(null=True, blank=True)
    n_interest_freq = models.IntegerField(null=True, blank=True)
    v_interest_freq_unit = models.CharField(max_length=50, null=True,blank=True)
    v_day_count_ind= models.CharField(max_length=7,default='30/365',null=True,blank=True, help_text="This column stores the accrual basis code for interest accrual calculation.")
    v_ccy_code = models.CharField(max_length=10, null=True)
    n_curr_payment_recd= models.DecimalField(max_digits=10, decimal_places=2, null=True,blank=True)
    v_instrument_type_cd = models.CharField(max_length=50, default="",null=True,blank=True)
    v_amrt_freq = models.CharField(max_length=10, null=True,blank=True)


    class Meta:
        db_table = 'Stg_Investments'
        constraints = [
            models.UniqueConstraint(
                fields=['v_account_number', 'fic_mis_date'],
                name='uq_%(app_label)s_%(class)s_acc_ficmis',
            ),
        ]

class Guarantee(models.Model):
    V_CONTRACT_CODE = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    D_MATURITY_DATE = models.DateField()
    D_VALUE_DATE = models.DateField()
    D_ORIGINATION_DATE = models.DateField()
    N_GUARANTEE_AMT = models.DecimalField(max_digits=15, decimal_places=2)
    N_GUARANTEE_VALUE = models.DecimalField(max_digits=15, decimal_places=2)
    V_DAY_COUNT_IND = models.CharField(max_length=20)
    V_DEVOLVEMENT_STATU_CD = models.CharField(max_length=20)
    V_REPAYMENT_TYPE = models.CharField(max_length=50)
    N_DEVOLMENT_AMT = models.DecimalField(max_digits=15, decimal_places=2)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50)

    class Meta:
        db_table = 'Stg_Guarantee'

        

class Borrowing(models.Model):
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    D_BOOK_DATE = models.DateField()
    D_INTEREST_DATE = models.DateField()
    D_LAST_PAYMENT_DATE = models.DateField()
    D_LAST_REPRICE_DATE = models.DateField()
    D_MATURITY_DATE = models.DateField()
    D_ORIG_NEXT_PAYMENT_DATE = models.DateField()
    D_REPRICING_DATE = models.DateField()
    D_REVISED_MATURITY_DATE = models.DateField()
    D_START_DATE = models.DateField()
    D_VALUE_DATE = models.DateField()
    N_AMRT_TERM = models.IntegerField()
    N_EOP_BAL = models.DecimalField(max_digits=15, decimal_places=2)
    N_EOP_BOOK_BAL = models.DecimalField(max_digits=15, decimal_places=2)
    N_EFFECTIVE_INTEREST_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    N_INTEREST_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    N_LRD_BALANCE = models.DecimalField(max_digits=15, decimal_places=2)
    N_REMAIN_NO_OF_PMTS = models.IntegerField()
    N_REM_TENOR = models.IntegerField()
    N_REPRICE_FREQ = models.IntegerField()
    N_TENOR = models.IntegerField()
    V_AMRT_TERM_UNIT = models.CharField(max_length=20)
    V_CCY_CODE = models.CharField(max_length=3)
    V_INSTRUMENT_CODE = models.CharField(max_length=50)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50)
    V_INT_PAYMENT_FREQUENCY_UNIT = models.CharField(max_length=20)
    V_INTEREST_METHOD = models.CharField(max_length=50)
    V_INTEREST_TIMING_TYPE = models.CharField(max_length=50)
    V_INTEREST_TYPE = models.CharField(max_length=50)
    V_LV_CODE = models.CharField(max_length=20)
    V_NEG_AMRT_EQ_UNIT = models.CharField(max_length=20)
    V_PMT_CHG_FREQ_UNIT = models.CharField(max_length=20)
    V_PROD_CODE = models.CharField(max_length=50)
    V_REPAYMENT_TYPE = models.CharField(max_length=50)
    V_REPRICE_FREQ_UNIT = models.CharField(max_length=20)
    V_TENOR_UNIT = models.CharField(max_length=20)
    V_PRODUCT_TYPE = models.CharField(max_length=50)
    V_DAY_COUNT_IND = models.CharField(max_length=20, default="")
    N_INT_PAYMENT_FREQUENCY = models.IntegerField(default=0)

    class Meta:
        db_table = 'Stg_Borrowings'


class Card(models.Model):
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    D_ACCT_OPEN_DATE = models.DateField()
    D_ACCT_MATURITY_DATE = models.DateField()
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    D_NEXT_PAYMENT_DATE = models.DateField()
    V_PMT_FREQ_UNIT = models.CharField(max_length=20)
    V_LV_CODE = models.CharField(max_length=20)
    D_LAST_PAYMENT_DATE = models.DateField()
    N_EOP_BAL = models.DecimalField(max_digits=15, decimal_places=2)
    N_CURR_INTEREST_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    V_INTEREST_TYPE = models.CharField(max_length=50)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50)
    N_PMT_FREQ = models.IntegerField()
    N_ORIG_TERM = models.IntegerField()
    V_ORIG_TERM_UNIT = models.CharField(max_length=20)
    V_AMTR_TYPE = models.CharField(max_length=20)
    V_DAY_COUNT_IND = models.CharField(max_length=20)
    V_GAAP_CODE = models.CharField(max_length=20)
    N_EOP_PMT_AMT = models.DecimalField(max_digits=15, decimal_places=2)
    

    class Meta:
        db_table = 'Stg_Cards'
        

    

class FirstDayProduct(models.Model):
    """
    Model to handle First Day Products for FX Gap and ZWG Gap reports.
    These are products that are automatically assigned to Day 1 liquidity bucket.
    """
    fic_mis_date = models.DateField(null=True)
    v_prod_code = models.CharField(max_length=50, null=True)
    v_ccy_code = models.CharField(max_length=10, null=True)
    v_prod_name = models.CharField(max_length=200, null=True,blank=True)
    v_account_number = models.CharField(max_length=255, null=False)
    n_eop_bal = models.DecimalField(max_digits=40, decimal_places=2, null=True)


    class Meta:
        db_table = 'Stg_First_Day_Bucket'
        constraints = [
            models.UniqueConstraint(
                fields=['v_account_number', 'fic_mis_date'],
                name='uq_%(app_label)s_%(class)s_acc_ficmis',
            ),
        ]


class CreditLine(models.Model):
    fic_mis_date = models.DateField(null=True)
    v_prod_code = models.CharField(max_length=50, null=True)
    v_account_number = models.CharField(max_length=255,  null=False)
    n_eop_bal = models.DecimalField(max_digits=20, decimal_places=5, null=True)
    v_ccy_code = models.CharField(max_length=10, null=True)
    v_interest_rate = models.DecimalField(max_digits=7, decimal_places=5,null=True,blank=True)
    v_amrt_freq = models.CharField(max_length=1, null=True,blank=True)
    v_days_to_maturity = models.IntegerField(null=True,blank=True)
    d_book_date = models.DateField(null=True,blank=True)
    v_repayment_type = models.CharField(max_length=20,null=True,blank=True)
    d_maturity_date = models.DateField(null=True,blank=True)
    d_next_payment_date = models.DateField(null=True,blank=True)
    v_facility_code = models.CharField(null=True, blank=True, max_length=50)


    class Meta:
        db_table = 'Stg_Credit_Line'
        constraints = [
            models.UniqueConstraint(
                fields=['v_account_number', 'fic_mis_date'],
                name='uq_%(app_label)s_%(class)s_acc_ficmis',
            ),
        ]


