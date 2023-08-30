# --------------------------------------------------------------------------------------------
# amqp_consumer.py - example consumer of Flightradar24 Live Feed via AMQP 1.0
#
# Based on from https://github.com/Azure/azure-event-hubs-python/blob/master/examples/eph.py
#
# Copyright (c) 2013-2018 Flightradar24 AB, all rights reserved
# --------------------------------------------------------------------------------------------

import asyncio
import logging
from collections import defaultdict

from azure.eventprocessorhost import (
    AbstractEventProcessor,
    AzureStorageCheckpointLeaseManager,
    EventHubConfig,
    EventProcessorHost,
    EPHOptions, Checkpoint)
from azure.eventprocessorhost.abstract_checkpoint_manager import AbstractCheckpointManager
from azure.eventprocessorhost.abstract_lease_manager import AbstractLeaseManager
from azure.eventprocessorhost.lease import Lease


class EventProcessor(AbstractEventProcessor):
    """
    Example Implementation of AbstractEventProcessor
    """

    def __init__(self, params=None):
        """
        Init Event processor
        """
        super().__init__(params)
        self._on_receive_callback, = params
        self._msg_counter = 0

    async def open_async(self, context):
        """
        Called by processor host to initialize the event processor.
        """
        logging.info(f"Connection established {context.partition_id}")

    async def close_async(self, context, reason):
        """
        Called by processor host to indicate that the event processor is being stopped.
        :param context: Information about the partition
        :type context: ~azure.eventprocessorhost.PartitionContext
        """
        logging.info(
            f"Connection closed (reason {reason}, id {context.partition_id}, offset {context.offset}, sq_number {context.sequence_number})"
        )

    async def process_events_async(self, context, messages):
        """
        Called by the processor host when a batch of events has arrived.
        This is where the real work of the event processor is done.
        :param context: Information about the partition
        :type context: ~azure.eventprocessorhost.PartitionContext
        :param messages: The events to be processed.
        :type messages: list[~azure.eventhub.common.EventData]
        """
        for event_data in messages:
            last_offset = event_data.offset
            last_sn = event_data.sequence_number
            # print('Received: {}, {}'.format(last_offset, last_sn))

            body_segments = list(event_data.message.get_data())
            self._on_receive_callback(body_segments[0])

        # logging.info('Events processed {}'.format(context.sequence_number))
        await context.checkpoint_async()

    async def process_error_async(self, context, error):
        """
        Called when the underlying client experiences an error while receiving.
        EventProcessorHost will take care of recovering from the error and
        continuing to pump messages,so no action is required from
        :param context: Information about the partition
        :type context: ~azure.eventprocessorhost.PartitionContext
        :param error: The error that occurred.
        """
        logging.error("Event Processor Error {!r}".format(error))


async def wait_and_close(host):
    """
    Run EventProcessorHost for 7 days then shutdown.
    """
    await asyncio.sleep(604800)
    await host.close_async()


class EqualToEverything(object):
    """
    This object will be always equal to any other
    """

    def __eq__(self, other):
        return True


class DummyLease(Lease):
    """
    Lease object with the owner equal to anything else (simplifies testing)
    """

    def __init__(self):
        super().__init__()
        self.owner = EqualToEverything()

    def serializable(self):
        """
        Returns Serializable instance of `__dict__`.
        """
        return self.__dict__.copy()


class DummyStorageCheckpointLeaseManager(AbstractCheckpointManager, AbstractLeaseManager):
    """
    Overriden interface of checkpoint manager.
    Normally it is implemented via Azure storage but can as well use any other storage.
    Here it does nothing except storing in memory. Just a proof of concept.
    """

    def __init__(self):
        AbstractCheckpointManager.__init__(self)
        AbstractLeaseManager.__init__(self, lease_renew_interval=10, lease_duration=30)

        self.checkpoints = defaultdict(Checkpoint)
        self.leases = defaultdict(DummyLease)

    def initialize(self, *args, **kwargs):
        pass

    async def create_checkpoint_store_if_not_exists_async(self):
        return True

    async def get_checkpoint_async(self, partition_id):
        if partition_id in self.checkpoints:
            return self.checkpoints[partition_id]
        else:
            return None

    async def create_checkpoint_if_not_exists_async(self, partition_id):
        # print('TODO: Replace this with check pointing.')
        checkpoint = await self.get_checkpoint_async(partition_id)
        if not checkpoint:
            await self.create_lease_if_not_exists_async(partition_id)
            checkpoint = Checkpoint(partition_id)
        return checkpoint

    async def update_checkpoint_async(self, lease, checkpoint):
        self.checkpoints[lease.partition_id] = checkpoint
        return True

    async def delete_checkpoint_async(self, partition_id):
        pass

    async def create_lease_store_if_not_exists_async(self):
        return True

    async def delete_lease_store_async(self):
        return True

    async def get_lease_async(self, partition_id):
        return self.leases[partition_id]

    async def get_all_leases(self):
        return [
            self.get_lease_async(partition_id)
            for partition_id in self.leases.keys()
        ]

    async def create_lease_if_not_exists_async(self, partition_id):
        lease = self.leases[partition_id]
        lease.partition_id = partition_id

    async def delete_lease_async(self, lease):
        pass

    async def acquire_lease_async(self, lease):
        return True

    async def renew_lease_async(self, lease):
        return True

    async def release_lease_async(self, lease):
        return True

    async def update_lease_async(self, lease):
        return True


class AMQPConsumer:
    def __init__(self, namespace, eventhub, user, key, consumer_group, storage_container,
                 storage_account, storage_key):
        self.namespace = namespace
        self.evenhub = eventhub
        self.user = user
        self.key = key
        self.consumer_group = consumer_group
        self.storage_container = storage_container
        self.storage_account = storage_account
        self.storage_key = storage_key
        self.on_receive_callback = None

    def set_callback(self, on_receive_callback):
        self.on_receive_callback = on_receive_callback

    def consume(self):
        try:
            loop = asyncio.get_event_loop()

            # Eventhub config and storage manager
            eh_config = EventHubConfig(self.namespace, self.evenhub, self.user, self.key,
                                       consumer_group=self.consumer_group)
            eh_options = EPHOptions()
            eh_options.release_pump_on_timeout = False
            eh_options.debug_trace = False

            if self._is_storage_checkpoint_enabled():
                storage_manager = AzureStorageCheckpointLeaseManager(
                    self.storage_account, self.storage_key, self.storage_container)
            else:
                storage_manager = DummyStorageCheckpointLeaseManager()

            # Event loop and host
            host = EventProcessorHost(
                EventProcessor,
                eh_config,
                storage_manager,
                ep_params=[self.on_receive_callback],
                eph_options=eh_options,
                loop=loop)

            tasks = asyncio.gather(
                host.open_async(),
                wait_and_close(host))
            loop.run_until_complete(tasks)

        except KeyboardInterrupt:
            # Canceling pending tasks and stopping the loop
            for task in asyncio.Task.all_tasks():
                task.cancel()
            loop.run_forever()
            tasks.exception()

        finally:
            loop.stop()

    def _is_storage_checkpoint_enabled(self):
        return self.storage_account and self.storage_key and self.storage_container
