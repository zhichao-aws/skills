"""
Thread pool utility for parallel execution of tasks.
This module provides a reusable thread pool implementation for executing
any function in parallel, with progress tracking.
"""

import concurrent.futures
from functools import partial
from typing import List, Callable, Any, Optional, TypeVar, Generic
from tqdm import tqdm

T = TypeVar("T")
R = TypeVar("R")


class ThreadPool:
    """
    A thread pool for executing tasks in parallel.

    This class provides methods to execute a function on multiple inputs in parallel,
    with optional progress tracking.
    """

    def __init__(self, max_workers: Optional[int] = None):
        """
        Initialize a ThreadPool.

        Args:
            max_workers: Maximum number of worker threads (None = default based on system)
        """
        self.max_workers = max_workers

    def map(
        self,
        func: Callable[[T], R],
        items: List[T],
        show_progress: bool = True,
        desc: str = "Processing",
    ) -> List[R]:
        """
        Execute a function on each item in parallel.

        Args:
            func: Function to execute on each item
            items: List of items to process
            show_progress: Whether to show a progress bar
            desc: Description for the progress bar

        Returns:
            List of results in the same order as the input items
        """
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            if show_progress:
                # Use tqdm to show progress
                results = list(
                    tqdm(executor.map(func, items), total=len(items), desc=desc)
                )
            else:
                # No progress bar
                results = list(executor.map(func, items))

        return results

    def map_with_args(
        self,
        func: Callable[..., R],
        items: List[T],
        fixed_args: dict = None,
        show_progress: bool = True,
        desc: str = "Processing",
    ) -> List[R]:
        """
        Execute a function on each item in parallel, with additional fixed arguments.

        Args:
            func: Function to execute on each item
            items: List of items to process
            fixed_args: Dictionary of fixed arguments to pass to the function
            show_progress: Whether to show a progress bar
            desc: Description for the progress bar

        Returns:
            List of results in the same order as the input items
        """
        if fixed_args is None:
            fixed_args = {}

        # Create a partial function with the fixed arguments
        partial_func = partial(func, **fixed_args)

        # Use the map method to execute the partial function
        return self.map(partial_func, items, show_progress, desc)
