import asyncio
import signal
import sys
from contextlib import suppress
from contract.contract import contract_file
import time
from mail_sender.mail_sender import mail_redis_crash_default


contract = contract_file()
time.sleep(2)

# Install uvloop for 2-3x faster event loop
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print(f"✅ uvloop {uvloop.__version__} enabled (3x faster event loop)")
except ImportError:
    print("uvloop not available (install: pip install uvloop)")

from config import settings
from services.integrated_ohlc_processor import IntegratedOhlcProcessor
from utils.logger import log


def print_startup_banner():
    print("Success Startup Banner")


def validate_config():
    """Validate critical configuration."""
    errors = []

    if not settings.redis_url:
        errors.append("redis_url is empty")

    if not settings.upstox_access_token:
        errors.append("upstox_access_token is empty")

    if errors:
        log.critical(f"❌ Configuration validation failed: {', '.join(errors)}")
        sys.exit(1)

    log.success("✅ Configuration validated")


async def main():
    """Main entry point."""
    print_startup_banner()
    validate_config()

    # Initialize processor
    processor = IntegratedOhlcProcessor(
        redis_url=settings.redis_url,
        ohlc_key_prefix="nse_fo"
    )

    # Shutdown event
    shutdown_event = asyncio.Event()

    # Signal handler
    def signal_handler(sig, frame):
        log.warning(f" Received signal {sig}")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start processor
    task = asyncio.create_task(processor.start())

    # Wait for completion or shutdown
    done, pending = await asyncio.wait(
        [task, asyncio.create_task(shutdown_event.wait())],
        return_when=asyncio.FIRST_COMPLETED
    )

    # Handle shutdown
    if shutdown_event.is_set():
        log.info("Shutdown requested")
        processor.request_stop()
        try:
            await asyncio.wait_for(task, timeout=30.0)
        except asyncio.TimeoutError:
            log.error("❌ Shutdown timeout")
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    else:
        if task.done() and task.exception():
            log.critical(f"💥 Task failed: {task.exception()}", exc_info=True)

    log.success("System shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        log.info("Manual shutdown (Ctrl+C)")
        mail_redis_crash_default(
            settings.redis_url,
            "KeyboardInterrupt (manual shutdown)",
            "Engine stopped manually by operator",
        )

    except Exception as e:
        err = str(e)
        log.critical("Fatal error: {}", err, exc_info=True)
        sys.exit(1)

    sys.exit(0)
