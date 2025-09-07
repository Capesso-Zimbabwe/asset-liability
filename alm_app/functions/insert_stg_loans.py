from django.db import transaction
from django.utils.timezone import now

from core.staging.models import LoanContract
from ..models import FSI_Loans_Processing  # adjust import path

def insert_stg_loans(fic_mis_date):
    """
    Move rows for fic_mis_date from Stg_Loan_Contracts -> Fsi_ALM_Processing.
    Upserts on v_account_number. Sets v_loan_type='loans '.
    Returns (created_count, updated_count).
    """

    src_qs = LoanContract.objects.filter(fic_mis_date=fic_mis_date)

    if not src_qs.exists():
        return 0, 0

    # Preload existing targets for upsert logic
    existing = {
        obj.v_account_number: obj
        for obj in FSI_Loans_Processing.objects.filter(
            v_account_number__in=src_qs.values_list("v_account_number", flat=True)
        )
    }

    to_create = []
    to_update = []

    for src in src_qs:
        data = {
            "fic_mis_date": src.fic_mis_date,
            "v_account_number": src.v_account_number,
            "v_cust_ref_code": src.v_cust_ref_code,
            "v_prod_code": src.v_prod_code,
            "n_curr_interest_rate": src.n_curr_interest_rate,
            "v_interest_freq_unit": src.v_interest_freq_unit,
            "v_interest_payment_type": src.v_interest_type,  # adjust if different meaning
            "v_day_count_ind": src.v_day_count_ind or "30/365",
            "d_start_date": src.d_book_date or src.d_value_date,
            "d_last_payment_date": src.d_last_payment_date,
            "d_next_payment_date": src.d_next_payment_date,
            "d_maturity_date": src.d_maturity_date,
            "v_amrt_repayment_type": src.v_amrt_repayment_type,
            "v_amrt_term_unit": src.v_amrt_term_unit,
            "n_eop_bal": src.n_eop_bal,
            "n_curr_payment_recd": src.n_curr_payment_recd,
            "v_ccy_code": src.v_ccy_code,
            # Target-only fields set to defaults / NULLs
            "n_interest_changing_rate": None,
            "v_management_fee_rate": None,
            "n_wht_percent": None,
            "n_effective_interest_rate": None,
            "n_accrued_interest": None,
            "n_eop_curr_prin_bal": None,
            "n_eop_int_bal": None,
            "n_collateral_amount": None,
            "n_acct_risk_score": None,
            "v_loan_type": "loans ",          # << required by you
            "m_fees": None,
            "v_m_fees_term_unit": None,
            "v_lob_code": None,
            "v_lv_code": None,
            "v_country_id": None,
            "v_credit_score": None,
            "v_collateral_type": None,
            "v_loan_desc": None,
            "v_account_classification_cd": None,
            "v_gaap_code": None,
            "v_branch_code": None,
            "v_facility_code": None,
        }

        tgt = existing.get(src.v_account_number)
        if tgt:
            # update fields on the existing object
            for k, v in data.items():
                setattr(tgt, k, v)
            to_update.append(tgt)
        else:
            to_create.append(FSI_Loans_Processing(**data))

    with transaction.atomic():
        created = FSI_Loans_Processing.objects.bulk_create(
            to_create, ignore_conflicts=True, batch_size=1000
        )
        if to_update:
            # list of fields to update (exclude PK, unique)
            update_fields = [f for f in data.keys() if f != "v_account_number"]
            FSI_Loans_Processing.objects.bulk_update(
                to_update, fields=update_fields, batch_size=1000
            )

    return len(created), len(to_update)
