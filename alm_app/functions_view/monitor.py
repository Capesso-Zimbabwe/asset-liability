from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.http import JsonResponse
from django.db.models import Max, Q, Count
from django.utils import timezone
from ..models import ExecutionHistory
from ..pipeline.pipeline import Pipeline, PipelineStep
from .execute import execute_functions, TOTAL_PIPELINE_STEPS  # Import TOTAL_PIPELINE_STEPS
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

@login_required
def monitor_view(request):
    """
    View to monitor current and recent pipeline executions.
    Shows active and recent runs across all users.
    """
    context = {}
    
    # Handle execution selection
    selected_date = request.GET.get('fic_mis_date')
    selected_run = request.GET.get('run_number')

    if selected_date and selected_run:
        # Try to load specific execution
        try:
            executions = (ExecutionHistory.objects
                        .filter(fic_mis_date=selected_date, run_number=selected_run)
                        .order_by('start_time'))
            
            if executions.exists():
                # Found the execution - show its details
                context.update(calculate_execution_progress(executions))
                context['viewing_execution'] = True
                return render(request, 'execute/monitor.html', context)
            else:
                # Execution not found - show error and recent executions
                context['search_error'] = f"No execution found for date {selected_date} and run #{selected_run}"
        except Exception as e:
            context['search_error'] = f"Error loading execution: {str(e)}"
    
    # No specific execution selected or error occurred - show recent executions
    try:
        # Get recent executions for the home page
        recent_executions = (ExecutionHistory.objects
                           .values('fic_mis_date', 'run_number')
                           .annotate(
                               latest_status=Max('status'),
                               total_steps=Count('id'),
                               completed_steps=Count('id', filter=Q(status='Success')),
                               has_failure=Count('id', filter=Q(status='Failed')),
                               has_stopped=Count('id', filter=Q(status='Stopped')),
                               start_time=Max('start_time')
                           )
                           .distinct()
                           .order_by('-start_time')[:10])

        if not recent_executions.exists():
            context['no_executions'] = True
            return render(request, 'execute/monitor.html', context)

        # Process recent executions
        for exe in recent_executions:
            if exe['has_failure'] > 0:
                exe['status'] = 'Failed'
            elif exe['has_stopped'] > 0:
                exe['status'] = 'Stopped'
            elif exe['completed_steps'] == exe['total_steps']:
                exe['status'] = 'Success'
            else:
                exe['status'] = 'Running'

            # Calculate progress percentage
            exe['progress'] = (exe['completed_steps'] / exe['total_steps']) * 100 if exe['total_steps'] > 0 else 0

        context.update({
            'recent_executions': recent_executions,
            'showing_recent': True
        })

    except Exception as e:
        context['error'] = f"Error loading recent executions: {str(e)}"
        context['no_executions'] = True

    return render(request, 'execute/monitor.html', context)

def get_recent_executions():
    """Get recent executions for the home page view."""
    recent_executions = (ExecutionHistory.objects
                        .values('fic_mis_date', 'run_number')
                        .annotate(
                            latest_status=Max('status'),
                            total_steps=Count('id'),
                            completed_steps=Count('id', filter=Q(status='Success')),
                            has_failure=Count('id', filter=Q(status='Failed')),
                            has_stopped=Count('id', filter=Q(status='Stopped'))
                        )
                        .distinct()
                        .order_by('-fic_mis_date', '-run_number')[:10])

    # Enhance recent executions with status
    for exe in recent_executions:
        if exe['has_failure'] > 0:
            exe['status'] = 'Failed'
        elif exe['has_stopped'] > 0:
            exe['status'] = 'Stopped'
        elif exe['completed_steps'] == exe['total_steps']:
            exe['status'] = 'Success'
        else:
            exe['status'] = 'Running'

    return {
        'recent_executions': recent_executions,
        'showing_recent': True
    }

def calculate_execution_progress(executions):
    """Calculate progress for a specific execution."""
    # Get the first execution to get date and run number
    first_execution = executions.first()
    fic_mis_date = first_execution.fic_mis_date
    run_number = first_execution.run_number

    # Calculate progress
    total_steps = len(TOTAL_PIPELINE_STEPS)
    completed_steps = 0
    failed_steps = 0
    running_steps = 0
    stopped_steps = 0
    
    # Create a mapping of completed steps
    step_status_map = {exe.process_name: exe.status for exe in executions}
    
    # Count steps in order and find current/failed/stopped step
    current_step = None
    last_executed_step = None
    for step_name in TOTAL_PIPELINE_STEPS:
        status = step_status_map.get(step_name)
        if status == 'Success':
            completed_steps += 1
            last_executed_step = step_name
        elif status == 'Failed':
            failed_steps += 1
            current_step = step_name
            break
        elif status == 'Running':
            running_steps += 1
            current_step = step_name
            break
        elif status == 'Stopped':
            stopped_steps += 1
            current_step = step_name
            break
        elif not status:  # Step not yet executed
            current_step = step_name
            break

    # Calculate progress percentage
    progress = (completed_steps / total_steps) * 100

    # Calculate total execution time
    total_time = sum(exe.execution_time or 0 for exe in executions)

    # Determine execution status
    if failed_steps > 0:
        status = 'Failed'
        resume_from = current_step
    elif stopped_steps > 0:
        status = 'Stopped'
        resume_from = current_step
    elif running_steps > 0:
        status = 'Running'
        resume_from = None
    elif completed_steps == total_steps:
        status = 'Success'
        resume_from = None
    else:
        status = 'Pending'
        resume_from = last_executed_step

    return {
        'fic_mis_date': fic_mis_date,
        'run_number': run_number,
        'executions': executions,
        'progress': round(progress, 1),
        'total_time': round(total_time, 2),
        'status': status,
        'can_continue': status in ['Failed', 'Stopped'],
        'current_step': current_step,
        'resume_from': resume_from,
        'is_running': running_steps > 0,
        'total_steps': total_steps,
        'completed_steps': completed_steps,
        'next_step': TOTAL_PIPELINE_STEPS[completed_steps] if completed_steps < total_steps else None
    }

@login_required
def get_execution_status(request):
    """API endpoint to get current execution status."""
    try:
        fic_mis_date = request.GET.get('fic_mis_date')
        run_number = request.GET.get('run_number')

        if not fic_mis_date or not run_number:
            # Get the most recent or active execution
            active = ExecutionHistory.objects.filter(status='Running').first()
            if active:
                fic_mis_date = active.fic_mis_date
                run_number = active.run_number
            else:
                latest = ExecutionHistory.objects.order_by('-start_time').first()
                if latest:
                    fic_mis_date = latest.fic_mis_date
                    run_number = latest.run_number
                else:
                    return JsonResponse({'error': 'No executions found'}, status=404)

        executions = (ExecutionHistory.objects
                     .filter(fic_mis_date=fic_mis_date, run_number=run_number)
                     .order_by('start_time'))

        if not executions.exists():
            return JsonResponse({'error': 'Execution not found'}, status=404)

        # Calculate progress similar to monitor_view
        completed_steps = sum(1 for exe in executions if exe.status == 'Success')
        progress = (completed_steps / len(TOTAL_PIPELINE_STEPS)) * 100

        return JsonResponse({
            'fic_mis_date': fic_mis_date,
            'run_number': run_number,
            'status': executions.filter(status='Running').exists() and 'Running' or
                     executions.filter(status='Failed').exists() and 'Failed' or
                     executions.filter(status='Stopped').exists() and 'Stopped' or
                     completed_steps == len(TOTAL_PIPELINE_STEPS) and 'Success' or 'Pending',
            'progress': round(progress, 1),
            'completed_steps': completed_steps,
            'total_steps': len(TOTAL_PIPELINE_STEPS),
            'current_step': next((exe.process_name for exe in executions if exe.status in ['Running', 'Failed', 'Stopped']), None),
            'executions': list(executions.values('process_name', 'status', 'execution_time', 'error_message'))
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def continue_execution(request):
    """Continue execution from the last failed or stopped step."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        # Parse JSON data from request body
        data = json.loads(request.body)
        fic_mis_date = data.get('fic_mis_date')
        run_number = data.get('run_number')

        if not fic_mis_date or not run_number:
            return JsonResponse({'error': 'Missing required parameters'}, status=400)

        # Find the execution to continue
        executions = (ExecutionHistory.objects
                     .filter(
                         fic_mis_date=fic_mis_date,
                         run_number=run_number
                     )
                     .order_by('start_time'))

        if not executions.exists():
            return JsonResponse({'error': 'Execution not found'}, status=404)

        # Find the point to continue from
        continue_from = None
        last_success = None
        
        # First check for failed or stopped step
        for exe in executions:
            if exe.status == 'Success':
                last_success = exe.process_name
            elif exe.status in ['Failed', 'Stopped']:
                continue_from = exe.process_name
                break

        if not continue_from and last_success:
            # If no failed/stopped step found, continue from after the last successful step
            try:
                current_idx = TOTAL_PIPELINE_STEPS.index(last_success)
                if current_idx < len(TOTAL_PIPELINE_STEPS) - 1:
                    continue_from = TOTAL_PIPELINE_STEPS[current_idx + 1]
            except ValueError:
                return JsonResponse({'error': 'Could not determine next step'}, status=400)
            
        if not continue_from:
            return JsonResponse({'error': 'No point to continue from'}, status=400)

        # Clear any failed or stopped statuses for this run
        ExecutionHistory.objects.filter(
            fic_mis_date=fic_mis_date,
            run_number=run_number,
            status__in=['Failed', 'Stopped']
        ).update(
            status='Pending',
            end_time=None,
            error_message=None
        )

        # Continue execution with same run number
        from .execute import execute_functions
        response = execute_functions(
            request,
            datetime.strptime(fic_mis_date, '%Y-%m-%d').date(),
            specified_run_number=run_number,
            continue_from_step=continue_from
        )

        # Parse the response
        response_data = json.loads(response.content)
        
        return JsonResponse({
            'success': True,
            'message': f'Continuing from: {continue_from}',
            'run_number': run_number,  # Same run number
            'continue_from': continue_from
        })

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        logger.exception("Error in continue_execution")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def stop_execution(request):
    """Stop the current execution by marking running steps as 'Stopped'."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        # Parse JSON data from request body
        data = json.loads(request.body)
        fic_mis_date = data.get('fic_mis_date')
        run_number = data.get('run_number')

        if not fic_mis_date or not run_number:
            return JsonResponse({'error': 'Missing required parameters'}, status=400)

        # Find running executions for this run
        running_executions = ExecutionHistory.objects.filter(
            fic_mis_date=fic_mis_date,
            run_number=run_number,
            status='Running'
        )

        if not running_executions.exists():
            return JsonResponse({'error': 'No running execution found'}, status=404)

        # Get the current step before stopping
        current_step = running_executions.first().process_name

        # Mark all running steps as stopped
        running_executions.update(
            status='Stopped',
            end_time=timezone.now()
        )

        return JsonResponse({
            'success': True,
            'message': current_step,
            'stopped_at': current_step
        })

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        logger.exception("Error in stop_execution")
        return JsonResponse({'error': str(e)}, status=500)
