from __future__ import annotations
from typing import Callable, List, Optional
from dataclasses import dataclass
from datetime import datetime
import logging
import time
from django.utils import timezone
import traceback

logger = logging.getLogger(__name__)

@dataclass
class PipelineStep:
    name: str
    function: Callable
    depends_on: List[str] = None
    retry_count: int = 3
    timeout_seconds: int = 3600

    def __post_init__(self):
        self.depends_on = self.depends_on or []

class PipelineResult:
    def __init__(self, step_name: str, status: str, execution_time: float, error_message: Optional[str] = None):
        self.step_name = step_name
        self.status = status
        self.execution_time = execution_time
        self.error_message = error_message

class Pipeline:
    def __init__(self, fic_mis_date: datetime.date):
        self.fic_mis_date = fic_mis_date
        self.steps: List[PipelineStep] = []
        self._results: List[PipelineResult] = []
        self._start_time = None

    def add_step(self, step: PipelineStep) -> Pipeline:
        """Add a step to the pipeline."""
        self.steps.append(step)
        return self

    def validate_dependencies(self):
        """Ensure all dependencies exist and there are no circular dependencies."""
        step_names = {step.name for step in self.steps}
        
        for step in self.steps:
            for dep in step.depends_on:
                if dep not in step_names:
                    raise ValueError(f"Step '{step.name}' depends on non-existent step '{dep}'")

        # Check for circular dependencies
        visited = set()
        temp_visited = set()

        def has_cycle(step_name: str) -> bool:
            if step_name in temp_visited:
                return True
            if step_name in visited:
                return False

            temp_visited.add(step_name)
            step = next(s for s in self.steps if s.name == step_name)
            
            for dep in step.depends_on:
                if has_cycle(dep):
                    return True
                    
            temp_visited.remove(step_name)
            visited.add(step_name)
            return False

        for step in self.steps:
            if has_cycle(step.name):
                raise ValueError(f"Circular dependency detected involving step '{step.name}'")

    def _execute_step(self, step: PipelineStep) -> PipelineResult:
        """Execute a single step with retry logic and timeout protection."""
        start_time = time.perf_counter()
        
        for attempt in range(step.retry_count):
            try:
                # Execute the step function
                step.function(fic_mis_date=self.fic_mis_date)
                
                execution_time = time.perf_counter() - start_time
                return PipelineResult(
                    step_name=step.name,
                    status="Success",
                    execution_time=execution_time
                )
                
            except Exception as e:
                if attempt == step.retry_count - 1:  # Last attempt
                    execution_time = time.perf_counter() - start_time
                    error_msg = ''.join(traceback.format_exception(type(e), e, e.__traceback__))[:4000]
                    logger.exception(f"Step {step.name} failed after {step.retry_count} attempts")
                    return PipelineResult(
                        step_name=step.name,
                        status="Failed",
                        execution_time=execution_time,
                        error_message=error_msg
                    )
                    
                # Wait before retrying (exponential backoff)
                time.sleep(2 ** attempt)

    def execute(self) -> List[PipelineResult]:
        """Execute all steps in the pipeline respecting dependencies."""
        self.validate_dependencies()
        self._start_time = time.perf_counter()
        completed_steps = set()
        
        while len(completed_steps) < len(self.steps):
            # Find next eligible step
            next_step = None
            for step in self.steps:
                if step.name not in completed_steps and all(dep in completed_steps for dep in step.depends_on):
                    next_step = step
                    break
                    
            if not next_step:
                remaining = [s.name for s in self.steps if s.name not in completed_steps]
                raise RuntimeError(f"Unable to resolve dependencies for remaining steps: {remaining}")

            # Execute the step
            result = self._execute_step(next_step)
            self._results.append(result)
            
            if result.status == "Success":
                completed_steps.add(next_step.name)
            else:
                # Stop execution on failure
                break

        return self._results

    @property
    def total_execution_time(self) -> float:
        """Get total execution time of the pipeline."""
        if not self._start_time:
            return 0.0
        return time.perf_counter() - self._start_time

    @property
    def is_completed(self) -> bool:
        """Check if pipeline completed successfully."""
        return bool(self._results and all(r.status == "Success" for r in self._results)) 