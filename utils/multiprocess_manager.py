"""
Multi-process scaling for CPU-bound workloads.
"""
import multiprocessing as mp
import signal
import asyncio
from typing import Optional, List
from utils.logger import setup_logger
logger = setup_logger("multiprocess")
class MultiProcessManager:
    """
    Manage multiple worker processes for horizontal scaling.
    Each process gets its own:
    - Event loop (uvloop)
    - Redis connection pool
    - Tick queue
    - Workers
    """
    def __init__(self, num_processes: int = 1):
        self.num_processes = num_processes
        self.processes: List[mp.Process] = []
        self._shutdown = False
        logger.info(f"MultiProcessManager: {num_processes} processes")
    def start(self, target_func, *args, **kwargs):
        """Start worker processes."""
        for i in range(self.num_processes):
            p = mp.Process(
                target=target_func,
                args=(i,) + args,
                kwargs=kwargs,
                daemon=False
            )
            p.start()
            self.processes.append(p)
            logger.info(f"Process {i} started (PID: {p.pid})")
    def shutdown(self):
        """Gracefully shutdown all processes."""
        logger.info("Shutting down all processes...")
        for p in self.processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)
                if p.is_alive():
                    p.kill()
        logger.info("All processes stopped")
    def wait(self):
        """Wait for all processes to complete."""
        for p in self.processes:
            p.join()
