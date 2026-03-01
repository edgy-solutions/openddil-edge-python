# =============================================================================
# OpenDDIL Edge SDK (Python) — Relay Agent
# =============================================================================
# Async background task that flushes the local SQLite Outbox to Redpanda (HQ)
# when connectivity is available. Designed for DDIL environments:
#   - Swallows network exceptions and retries gracefully
#   - Only marks events as processed after Redpanda ACK
#   - Uses exponential backoff on connection failures
#   - Sends raw Protobuf bytes for maximum bandwidth efficiency
# =============================================================================

import asyncio
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from openddil_edge.outbox_manager import OpenDdilOutboxManager

logger = logging.getLogger("openddil.relay")


class OutboxRelayAgent:
    """
    Async background agent that polls the local SQLite Outbox and publishes
    pending events to Redpanda. Resilient to network failures — keeps
    polling and retrying automatically when connectivity is restored.

    Usage:
        outbox = OpenDdilOutboxManager("openddil_edge.db", "edge-node-42")
        relay = OutboxRelayAgent(outbox)
        await relay.run()  # Runs until cancelled
    """

    # Backoff settings for DDIL resilience
    MIN_POLL_INTERVAL: float = 2.0        # seconds
    MAX_BACKOFF: float = 300.0            # 5 minutes
    IDLE_POLL_INTERVAL: float = 10.0      # seconds

    def __init__(
        self,
        outbox: OpenDdilOutboxManager,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "inventory-commands",
        batch_size: int = 50,
    ) -> None:
        self._outbox = outbox
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._batch_size = batch_size

    async def run(self) -> None:
        """
        Main relay loop. Connects to Redpanda, polls the Outbox, and flushes
        events. Automatically reconnects with exponential backoff on failure.
        """
        logger.info(
            "[RELAY] Starting Outbox Relay. bootstrap=%s, topic=%s",
            self._bootstrap_servers,
            self._topic,
        )

        current_backoff = self.MIN_POLL_INTERVAL

        while True:
            producer: Optional[AIOKafkaProducer] = None

            try:
                producer = AIOKafkaProducer(
                    bootstrap_servers=self._bootstrap_servers,
                    # Exactly-once semantics: enable idempotent producer
                    enable_idempotence=True,
                    acks="all",
                    # Compression for bandwidth-limited DDIL links
                    compression_type="zstd",
                    # Retry settings for intermittent connectivity
                    retry_backoff_ms=500,
                    request_timeout_ms=30000,
                    # Batch settings for efficiency
                    linger_ms=100,
                    max_batch_size=65536,
                )

                await producer.start()
                logger.info("[RELAY] Connected to Redpanda.")
                current_backoff = self.MIN_POLL_INTERVAL  # Reset on success

                # Inner loop: poll and flush while connected
                while True:
                    events = self._outbox.get_pending_events(self._batch_size)

                    if not events:
                        # Nothing to flush — idle poll
                        await asyncio.sleep(self.IDLE_POLL_INTERVAL)
                        continue

                    logger.info("[RELAY] Flushing %d events.", len(events))
                    flushed_ids: list[str] = []

                    for event_id, payload_bytes in events:
                        try:
                            # Publish raw Protobuf bytes — no re-serialization
                            await producer.send_and_wait(
                                self._topic,
                                key=event_id.encode("utf-8"),
                                value=payload_bytes,
                            )

                            # Only mark processed after successful ACK
                            flushed_ids.append(event_id)

                            logger.debug(
                                "[RELAY] Event %s published successfully.", event_id
                            )
                        except KafkaError as exc:
                            # Individual message failure — skip and retry next cycle
                            logger.warning(
                                "[RELAY] Failed to produce event %s: %s. Will retry.",
                                event_id,
                                exc,
                            )
                            break  # Stop this batch, retry on next poll

                    # Mark successfully flushed events
                    if flushed_ids:
                        self._outbox.mark_as_processed(flushed_ids)
                        logger.info(
                            "[RELAY] Marked %d events as processed.", len(flushed_ids)
                        )

                    # Brief pause between batches
                    await asyncio.sleep(self.MIN_POLL_INTERVAL)

            except asyncio.CancelledError:
                # Graceful shutdown
                logger.info("[RELAY] Shutting down.")
                break

            except Exception as exc:
                # Connection failure — backoff and retry
                logger.warning(
                    "[RELAY] Connection lost: %s. Retrying in %.1fs...",
                    exc,
                    current_backoff,
                )
                await asyncio.sleep(current_backoff)

                # Exponential backoff with cap
                current_backoff = min(current_backoff * 2, self.MAX_BACKOFF)

            finally:
                if producer is not None:
                    try:
                        await producer.stop()
                    except Exception:
                        pass  # Best-effort cleanup

        logger.info(
            "[RELAY] Stopped. %d events still pending.",
            self._outbox.get_pending_count(),
        )


async def run_relay(
    db_path: str = "openddil_edge.db",
    edge_node_id: str = "edge-node-01",
    bootstrap_servers: str = "localhost:9092",
    topic: str = "inventory-commands",
) -> None:
    """
    Convenience entry point to run the Relay agent as a standalone process.

    Usage:
        python -m openddil_edge.relay_agent
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    outbox = OpenDdilOutboxManager(db_path, edge_node_id)
    relay = OutboxRelayAgent(
        outbox,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )

    try:
        await relay.run()
    finally:
        outbox.close()


if __name__ == "__main__":
    asyncio.run(run_relay())
