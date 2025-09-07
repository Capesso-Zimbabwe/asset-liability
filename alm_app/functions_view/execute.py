# execute/views.py
from __future__ import annotations

import logging
from datetime import datetime
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.db.models import Max
from django.utils import timezone

from ..functions.repo_alignment import align_buckets_to_balance
from ..pipeline.pipeline import Pipeline, PipelineStep
from ..services.execution_service import ExecutionService
from ..models import ExecutionHistory

# Import domain functions
from ..functions.cashflow_first_day import cashflow_first_day
from ..functions.cashflow_credit import cashflow_credit_line
from ..functions.cashflows_loans import balance_cashflows_to_target, cashflow_loan_contracts
from ..functions.cashflow_overdrafts import cashflow_overdrafts
from ..functions.cashflow_investments import cashflow_investments
from ..functions.cashflow_acc_aggr import calculate_time_buckets_and_spread
from ..functions.cashflow_prod_aggr import aggregate_by_prod_code
from ..functions.report_loader import (
    create_report_contractual_table,
    report_contractual_load,
)
from ..functions.report_contractual_cons_loader import load_report_contractual_cons
from ..functions.report_behavioural_loader import load_report_behavioural
from ..functions.report_rate_sensitive_loader import load_report_rate_sensitive

logger = logging.getLogger(__name__)

# Define the pipeline steps and their dependencies
TOTAL_PIPELINE_STEPS = [
    "Process First Day Cashflows",
    "Process Credit Line Cashflows",
    "Process Loan Cashflows",
    "Balance Cashflows to Target",
    "Process Overdraft Cashflows",
    "Process Investment Cashflows",
    "Process First Day Cashflows (Re-run)",
    "Calculate Time Buckets and Spread",
    "Aggregate by Product Code",
    "Create Report Contractual Table",
    "Load Contractual Report",
    "Align Buckets to Balance",
    "Load Contractual Consolidated Report",
    "Load Behavioural Report",
    "Load Rate-Sensitive Report"
]

# Define the steps with their functions and dependencies
all_steps = [
    ("Process First Day Cashflows", cashflow_first_day, []),
    ("Process Credit Line Cashflows", cashflow_credit_line, ["Process First Day Cashflows"]),
    ("Process Loan Cashflows", cashflow_loan_contracts, ["Process Credit Line Cashflows"]),
    ("Balance Cashflows to Target", balance_cashflows_to_target, ["Process Loan Cashflows"]),
    ("Process Overdraft Cashflows", cashflow_overdrafts, ["Balance Cashflows to Target"]),
    ("Process Investment Cashflows", cashflow_investments, ["Process Overdraft Cashflows"]),
    ("Process First Day Cashflows (Re-run)", cashflow_first_day, ["Process Investment Cashflows"]),
    ("Calculate Time Buckets and Spread", calculate_time_buckets_and_spread, ["Process First Day Cashflows (Re-run)"]),
    ("Aggregate by Product Code", aggregate_by_prod_code, ["Calculate Time Buckets and Spread"]),
    ("Create Report Contractual Table", create_report_contractual_table, ["Aggregate by Product Code"]),
    ("Load Contractual Report", report_contractual_load, ["Create Report Contractual Table"]),
    ("Align Buckets to Balance", align_buckets_to_balance, ["Load Contractual Report"]),
    ("Load Contractual Consolidated Report", load_report_contractual_cons, ["Align Buckets to Balance"]),
    ("Load Behavioural Report", load_report_behavioural, ["Load Contractual Consolidated Report"]),
    ("Load Rate-Sensitive Report", load_report_rate_sensitive, ["Load Behavioural Report"])
]

@login_required
def execute_view(request):
    """Form endpoint + AJAX endpoint."""
    if request.method == "POST":
        fic_mis_date_str: str = request.POST.get("fic_mis_date", "")
        try:
            fic_mis_date = datetime.strptime(fic_mis_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return render(
                request,
                "execute/execute.html",
                {"error": "Invalid date format. Please use YYYY-MM-DD."},
            )

        existing_executions = ExecutionHistory.objects.filter(fic_mis_date=fic_mis_date)
        next_run = existing_executions.count() + 1

        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            force_new_execution_str = request.POST.get("force_new_execution", "false")
            force_new = force_new_execution_str.lower() == "true"
            is_execute_again = "execute_again" in request.POST or force_new

            if is_execute_again:
                return execute_functions(
                    request,
                    fic_mis_date,
                    force_new_execution=True,
                    specified_run_number=next_run,
                )
            return execute_functions(request, fic_mis_date, force_new_execution=True)

        return render(
            request,
            "execute/execute.html",
            {
                "fic_mis_date": fic_mis_date_str,
                "next_run": next_run,
                "ready_to_execute": True,
            },
        )

    return render(request, "execute/execute.html")

def execute_functions(
    request,
    fic_mis_date,
    *,
    force_new_execution: bool = False,
    specified_run_number: int | None = None,
    continue_from_step: str | None = None,
) -> JsonResponse:
    """Execute pipeline steps in sequence with proper tracking and error handling."""
    # Initialize services
    execution_service = ExecutionService(fic_mis_date)
    
    if continue_from_step:
        # When continuing, use the same run number
        run_number = specified_run_number
        
        # Get existing executions and their statuses
        existing_executions = (ExecutionHistory.objects
                             .filter(fic_mis_date=fic_mis_date, 
                                   run_number=run_number)
                             .order_by('start_time'))
        
        # Create status map for existing executions
        execution_status_map = {exe.process_name: exe.status for exe in existing_executions}
        
        # Find the index of the continue point
        try:
            continue_idx = TOTAL_PIPELINE_STEPS.index(continue_from_step)
        except ValueError:
            return JsonResponse({
                'error': f'Invalid continuation step: {continue_from_step}'
            }, status=400)
            
        # Create list of remaining steps
        remaining_steps = []
        for step_name, step_func, dependencies in all_steps[continue_idx:]:
            # Include step if:
            # 1. It hasn't been executed yet
            # 2. It's the stopped/failed step we're continuing from
            # 3. It comes after the stopped/failed step
            status = execution_status_map.get(step_name)
            if not status or status in ['Failed', 'Stopped'] or step_name == continue_from_step:
                remaining_steps.append((step_name, step_func, dependencies))

        # Ensure all steps exist in the history
        for step_name in TOTAL_PIPELINE_STEPS:
            if step_name not in execution_status_map:
                # Create new entry for steps that don't exist
                ExecutionHistory.objects.create(
                    fic_mis_date=fic_mis_date,
                    run_number=run_number,
                    process_name=step_name,
                    status='Pending' if step_name in [s[0] for s in remaining_steps] else 'Skipped',
                    start_time=timezone.now()
                )
            elif step_name in [s[0] for s in remaining_steps]:
                # Update status for steps that will be executed
                ExecutionHistory.objects.filter(
                    fic_mis_date=fic_mis_date,
                    run_number=run_number,
                    process_name=step_name,
                    status__in=['Failed', 'Stopped']
                ).update(
                    status='Pending',
                    error_message=None,
                    end_time=None,
                    execution_time=None
                )
    else:
        # For new executions
        run_number = execution_service.get_next_run_number(specified_run_number)
        remaining_steps = all_steps
        
        if force_new_execution:
            execution_service.clear_existing_run(run_number)

    # Initialize pipeline with remaining steps
    pipeline = Pipeline(fic_mis_date)
    
    # Add remaining steps to pipeline
    for step_name, step_func, dependencies in remaining_steps:
        pipeline.add_step(PipelineStep(
            name=step_name,
            function=step_func,
            depends_on=dependencies
        ))

    try:
        # Execute pipeline and track results
        for step in pipeline.steps:
            # Get existing execution for this step if any
            existing_execution = None
            if continue_from_step:
                existing_execution = ExecutionHistory.objects.filter(
                    fic_mis_date=fic_mis_date,
                    run_number=run_number,
                    process_name=step.name
                ).first()

            # If step was already successful, skip it
            if existing_execution and existing_execution.status == 'Success':
                continue

            # If step exists but wasn't successful, update its status to Running
            if existing_execution:
                existing_execution.status = 'Running'
                existing_execution.start_time = timezone.now()
                existing_execution.end_time = None
                existing_execution.error_message = None
                existing_execution.execution_time = None
                existing_execution.save()
                hist = existing_execution
            else:
                # Create new running status
                hist = execution_service.create_running_status(step.name, run_number)
            
            # Execute step and get result
            result = pipeline._execute_step(step)
            
            # Update execution history
            execution_service.update_execution_status(hist, result)
            
            # Break if step failed
            if result.status == "Failed":
                # Mark remaining steps as Pending
                ExecutionHistory.objects.filter(
                    fic_mis_date=fic_mis_date,
                    run_number=run_number,
                    status='Pending'
                ).update(
                    status='Pending',
                    error_message='Previous step failed'
                )
                break

        # Get all executions for this run, including skipped ones
        all_executions = (ExecutionHistory.objects
                         .filter(fic_mis_date=fic_mis_date, run_number=run_number)
                         .order_by('start_time'))

        # Format and return response with all steps
        response_data = execution_service.format_response(
            run_number,
            pipeline._results,
            pipeline.total_execution_time
        )
        
        # Add all executions to the response
        response_data['all_steps'] = list(all_executions.values(
            'process_name', 'status', 'execution_time', 'error_message'
        ))

        return JsonResponse(response_data)

    except Exception as e:
        logger.exception("Pipeline execution failed")
        # Mark remaining steps as Pending on error
        ExecutionHistory.objects.filter(
            fic_mis_date=fic_mis_date,
            run_number=run_number,
            status='Pending'
        ).update(
            status='Pending',
            error_message=str(e)
        )
        return JsonResponse(
            {
                "fic_mis_date": fic_mis_date.strftime("%Y-%m-%d"),
                "run_number": run_number,
                "error": str(e),
                "completed": False
            },
            status=500
        )

@login_required
def execution_history(request):
    """Render execution history page."""
    dates = (
        ExecutionHistory.objects.values("fic_mis_date")
        .distinct()
        .order_by("-fic_mis_date")
    )
    history = []

    for d in dates:
        fic_mis_date = d["fic_mis_date"]
        runs = (
            ExecutionHistory.objects.filter(fic_mis_date=fic_mis_date)
            .values("run_number")
            .distinct()
            .order_by("-run_number")
        )

        date_runs = []
        for r in runs:
            run_number = r["run_number"]
            executions = ExecutionHistory.objects.filter(
                fic_mis_date=fic_mis_date,
                run_number=run_number
            ).order_by("start_time")

            overall_status = "Success"
            total_seconds = 0.0
            for exe in executions:
                if exe.status == "Failed":
                    overall_status = "Failed"
                if exe.execution_time:
                    total_seconds += exe.execution_time

            date_runs.append({
                    "run_number": run_number,
                    "executions": executions,
                    "overall_status": overall_status,
                    "total_time": round(total_seconds, 2),
                    "start_time": executions.first().start_time if executions else None,
                    "end_time": executions.last().end_time if executions else None,
            })

        history.append({"fic_mis_date": fic_mis_date, "runs": date_runs})

    return render(request, "execute/history.html", {"history": history})

def execution_status_api(request):
    """API endpoint to get execution status."""
    date = request.GET.get("fic_mis_date")
    run = request.GET.get("run") or request.GET.get("run_number")

    if not date or not run:
        return HttpResponseBadRequest("fic_mis_date and run (or run_number) are required")

    qs = (ExecutionHistory.objects
          .filter(fic_mis_date=date, run_number=run)
          .values("process_name", "status", "execution_time", "error_message"))

    return JsonResponse({
        "fic_mis_date": date,
        "run_number": run,
        "executions": list(qs)
    })
