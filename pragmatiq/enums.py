from enum import Enum


class TaskState(Enum):
    """Enumeration of possible states for a task in the PragmatiQ system.

    Defines the lifecycle states a task can be in, used for tracking and monitoring task execution.

    Attributes:
        CANCELLED (str): Task was cancelled before completion, value is "cancelled".
        COMPLETED (str): Task finished successfully, value is "completed".
        FAILED (str): Task encountered an error and failed, value is "failed".
        PENDING (str): Task is queued and awaiting execution, value is "pending".
        RUNNING (str): Task is currently executing, value is "running".
    """

    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"
    RUNNING = "running"
