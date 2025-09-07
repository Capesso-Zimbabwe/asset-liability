from django.db import models

class BaseModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        abstract = True

class Investments(BaseModel):
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    N_EOP_BAL = models.DecimalField(max_digits=15, decimal_places=2)
    N_INTEREST_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    D_MATURITY_DATE = models.DateField(null=True, blank=True)
    D_START_DATE = models.DateField(null=True, blank=True)
    D_NEXT_PAYMENT_DATE = models.DateField(null=True, blank=True)
    D_LAST_PAYMENT_DATE = models.DateField(null=True, blank=True)
    N_AMRT_TERM = models.IntegerField(null=True, blank=True)
    V_AMRT_TERM_UNIT = models.CharField(max_length=20, null=True, blank=True)
    V_REPAYMENT_TYPE = models.CharField(max_length=50)
    V_INTEREST_TYPE = models.CharField(max_length=50, null=True, blank=True)
    N_INT_PAYMENT_FREQUENCY = models.IntegerField(null=True, blank=True)
    V_INT_PAYMENT_FREQ_UNIT = models.CharField(max_length=20, null=True, blank=True)
    V_DAY_COUNT_IND = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50)
    N_ACCR_INT_AMT = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)


    class Meta:
        unique_together = ['V_ACCOUNT_NUMBER', 'FIC_MIS_DATE']

    def __str__(self):
        return f"{self.V_ACCOUNT_NUMBER} - {self.FIC_MIS_DATE}"

class Guarantees(BaseModel):
    V_CONTRACT_CODE = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    N_GUARANTEE_AMT = models.DecimalField(max_digits=15, decimal_places=2)
    D_BOOK_DATE = models.DateField()

    class Meta:
        unique_together = ['V_CONTRACT_CODE', 'FIC_MIS_DATE']

    def __str__(self):
        return f"{self.V_CONTRACT_CODE} - {self.FIC_MIS_DATE}"

class LoanContracts(BaseModel):
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    N_EOP_BAL = models.DecimalField(max_digits=15, decimal_places=2)
    N_CURR_INTEREST_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    D_BOOK_DATE = models.DateField()
    D_MATURITY_DATE = models.DateField()
    D_LAST_PAYMENT_DATE = models.DateField(null=True, blank=True)
    D_NEXT_PAYMENT_DATE = models.DateField(null=True, blank=True)
    N_AMRT_TERM = models.IntegerField(null=True, blank=True)
    V_AMRT_TERM_UNIT = models.CharField(max_length=20, blank=True, null=True)
    V_INTEREST_TYPE = models.CharField(max_length=50, blank=True, null=True)
    V_REPAYMENT_TYPE = models.CharField(max_length=50)
    V_DAY_COUNT_IND = models.CharField(max_length=20, null=True, blank=True)
    N_REMAIN_NO_OF_PMTS = models.IntegerField(null=True, blank=True)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50)
    V_AMRT_FREQ = models.CharField(max_length=1)

    class Meta:
        unique_together = ['V_ACCOUNT_NUMBER', 'FIC_MIS_DATE']

    def __str__(self):
        return f"{self.V_ACCOUNT_NUMBER} - {self.FIC_MIS_DATE}"

class Borrowings(BaseModel):
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    N_EOP_BAL = models.DecimalField(max_digits=15, decimal_places=2)
    N_INTEREST_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    V_INTEREST_TYPE = models.CharField(max_length=50)
    V_INTEREST_METHOD = models.CharField(max_length=50)
    V_DAY_COUNT_IND = models.DecimalField(max_digits=5, decimal_places=5)
    D_START_DATE = models.DateField()
    D_BOOK_DATE = models.DateField()
    D_MATURITY_DATE = models.DateField()
    D_LAST_PAYMENT_DATE = models.DateField()
    D_REPRICING_DATE = models.DateField()
    D_VALUE_DATE = models.DateField()
    N_AMRT_TERM = models.IntegerField()
    V_AMRT_TERM_UNIT = models.CharField(max_length=20)
    N_REMAIN_NO_OF_PMTS = models.IntegerField()
    V_REPAYMENT_TYPE = models.CharField(max_length=50)
    N_LRD_BALANCE = models.DecimalField(max_digits=15, decimal_places=2)
    V_INT_PAYMENT_FREQUENCY_UNIT = models.CharField(max_length=20)
    N_INT_PAYMENT_FREQUENCY = models.IntegerField()
    V_INTEREST_TIMING_TYPE = models.CharField(max_length=50)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50)

    class Meta:
        unique_together = ['V_ACCOUNT_NUMBER', 'FIC_MIS_DATE']

    def __str__(self):
        return f"{self.V_ACCOUNT_NUMBER} - {self.FIC_MIS_DATE}"

class Cards(BaseModel):
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    N_EOP_BAL = models.DecimalField(max_digits=15, decimal_places=2)
    N_CURR_INTEREST_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    D_NEXT_PAYMENT_DATE = models.DateField()
    V_INTEREST_TYPE = models.CharField(max_length=50)
    V_DAY_COUNT_IND = models.CharField(max_length=20)

    class Meta:
        unique_together = ['V_ACCOUNT_NUMBER', 'FIC_MIS_DATE']

    def __str__(self):
        return f"{self.V_ACCOUNT_NUMBER} - {self.FIC_MIS_DATE}"

class Casa(BaseModel):
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    FIC_MIS_DATE = models.DateField()
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    N_EOP_BALANCE_SAVINGS = models.DecimalField(max_digits=15, decimal_places=2)
    N_CURRENT_INT_RATE = models.DecimalField(max_digits=5, decimal_places=2)
    V_INTEREST_TYPE = models.CharField(max_length=50)
    N_INTEREST_FREQ = models.IntegerField()
    V_INTEREST_FREQ_UNIT = models.CharField(max_length=20)
    V_DAY_COUNT_IND = models.CharField(max_length=20)
    D_ACCT_OPEN_DATE = models.DateField()
    D_LAST_PAYMENT_DATE = models.DateField()
    D_MATURITY_DATE = models.DateField()
    N_AMRT_TERM = models.IntegerField()
    V_AMRT_TERM_UNIT = models.CharField(max_length=20)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50)

    class Meta:
        unique_together = ['V_ACCOUNT_NUMBER', 'FIC_MIS_DATE']

    def __str__(self):
        return f"{self.V_ACCOUNT_NUMBER} - {self.FIC_MIS_DATE}"

class FirstDayProduct(BaseModel):
    """
    Model to handle First Day Products for FX Gap and ZWG Gap reports in FSI module.
    These are products that are automatically assigned to Day 1 liquidity bucket.
    """
    V_PROD_CODE = models.CharField(max_length=50)
    V_CCY_CODE = models.CharField(max_length=3)
    FIC_MIS_DATE = models.DateField()
    V_PROD_NAME = models.CharField(max_length=200)
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    N_EOP_BAL = models.DecimalField(max_digits=15, decimal_places=2)
    V_INSTRUMENT_TYPE_CD = models.CharField(max_length=50, null=True, blank=True)
    V_INTEREST_TYPE = models.CharField(max_length=50, null=True, blank=True)
    N_INTEREST_RATE = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    V_DAY_COUNT_IND = models.CharField(max_length=20, null=True, blank=True)

    class Meta:
        unique_together = ['V_ACCOUNT_NUMBER', 'FIC_MIS_DATE']
        indexes = [
            models.Index(fields=['V_PROD_CODE']),
            models.Index(fields=['FIC_MIS_DATE']),
            models.Index(fields=['V_CCY_CODE']),
            models.Index(fields=['V_INSTRUMENT_TYPE_CD']),
        ]

    def __str__(self):
        return f"{self.V_PROD_CODE} - {self.FIC_MIS_DATE}"

class FsiCreditLine(BaseModel):
    V_PROD_CODE = models.CharField(max_length=50)
    V_ACCOUNT_NUMBER = models.CharField(max_length=50)
    N_EOP_BAL = models.DecimalField(max_digits=20, decimal_places=5)
    V_CCY_CODE = models.CharField(max_length=3)
    N_INTEREST_RATE = models.DecimalField(max_digits=7, decimal_places=5)
    V_AMRT_FREQ = models.CharField(max_length=1)
    V_DAYS_TO_MATURITY = models.IntegerField()
    FIC_MIS_DATE = models.DateField()
    D_BOOK_DATE = models.DateField()
    V_REPAYMENT_TYPE = models.CharField(max_length=20)
    D_MATURITY_DATE = models.DateField()
    D_NEXT_PAYMENT_DATE = models.DateField()
    V_FACILITY_CODE = models.CharField(null=True, blank=True, max_length=50)

    class Meta:
        unique_together = [
            ("V_PROD_CODE", "V_ACCOUNT_NUMBER", "FIC_MIS_DATE")
        ]

    def __str__(self):
        return f"{self.V_PROD_CODE} - {self.V_ACCOUNT_NUMBER} - {self.FIC_MIS_DATE}"