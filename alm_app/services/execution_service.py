from datetime import datetime
from typing import List, Optional
from django.utils import timezone
from django.db.models import Max

from ..models import ExecutionHistory
from ..pipeline.pipeline import PipelineResult

class ExecutionService:
    def __init__(self, fic_mis_date: datetime.date):
        self.fic_mis_date = fic_mis_date

    def get_next_run_number(self, specified_run_number: Optional[int] = None) -> int:
        """Get the next run number for the given date."""
        if specified_run_number is not None:
            return specified_run_number
            
        return (
            ExecutionHistory.objects.filter(fic_mis_date=self.fic_mis_date)
            .aggregate(max_run=Max("run_number"))
            .get("max_run", 0) or 0
        ) + 1

    def clear_existing_run(self, run_number: int):
        """Clear any existing execution history for this run."""
        ExecutionHistory.objects.filter(
            fic_mis_date=self.fic_mis_date,
            run_number=run_number
        ).delete()

    def create_running_status(self, process_name: str, run_number: int) -> ExecutionHistory:
        """Create a new execution history entry with 'Running' status."""
        return ExecutionHistory.objects.create(
            fic_mis_date=self.fic_mis_date,
            run_number=run_number,
            process_name=process_name,
            status="Running",
            start_time=timezone.now()
        )

    def update_execution_status(self, history: ExecutionHistory, result: PipelineResult):
        """Update execution history with the result."""
        history.status = result.status
        history.end_time = timezone.now()
        history.execution_time = result.execution_time
        if result.error_message:
            history.error_message = result.error_message
        history.save()

    def format_response(self, run_number: int, results: List[PipelineResult], total_time: float) -> dict:
        """Format the execution results for API response."""
        executions = [
            {
                "process_name": result.step_name,
                "run_number": run_number,
                "status": result.status,
                "execution_time": round(result.execution_time, 2),
                "error_message": result.error_message if result.error_message else None
            }
            for result in results
        ]

        return {
            "fic_mis_date": self.fic_mis_date.strftime("%Y-%m-%d"),
            "run_number": run_number,
            "executions": executions,
            "total_execution_time": round(total_time, 2),
            "completed": all(r.status == "Success" for r in results)
        } 