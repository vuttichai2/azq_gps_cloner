"""
Progress Bar for Ray Actors (tqdm)
==================================

Tracking progress of distributed tasks can be tricky.

This script will demonstrate how to implement a simple
progress bar for a Ray actor to track progress across various
different distributed components.

Original source: `Link <https://github.com/votingworks/arlo-e2e>`_

Setup: Dependencies
-------------------

First, import some dependencies.
"""

# Inspiration: https://github.com/honnibal/spacy-ray/pull/
# 1/files#diff-7ede881ddc3e8456b320afb958362b2aR12-R45
from asyncio import Event
from typing import Tuple

import ray
# For typing purposes
from ray.actor import ActorHandle
from tqdm import tqdm

############################################################
# This is the Ray "actor" that can be called from anywhere to update
# our progress. You'll be using the `update` method. Don't
# instantiate this class yourself. Instead,
# it's something that you'll get from a `ProgressBar`.


@ray.remote
class ProgressBarActor:
    event: Event
    progress_dict = {}

    def __init__(self) -> None:
        self.event = Event()

    def update(self, name, num_items_completed: int) -> None:
        """Updates the ProgressBar with the incremental
        number of items that were just completed.
        """
        self.progress_dict[name] = num_items_completed
        self.event.set()

    async def wait_for_update(self) -> Tuple[int, int]:
        """Blocking call.

        Waits until somebody calls `update`, then returns a tuple of
        the number of updates since the last call to
        `wait_for_update`, and the total number of completed items.
        """
        await self.event.wait()
        self.event.clear()
        progress_sum = 0
        for progress in self.progress_dict:
            progress_sum += self.progress_dict[progress]
        return progress_sum




######################################################################
# This is where the progress bar starts. You create one of these
# on the head node, passing in the expected total number of items,
# and an optional string description.
# Pass along the `actor` reference to any remote task,
# and if they complete ten
# tasks, they'll call `actor.update.remote(10)`.

# Back on the local node, once you launch your remote Ray tasks, call
# `print_until_done`, which will feed everything back into a `tqdm` counter.


class ProgressBar:
    progress_actor: ActorHandle
    total: int
    azm_count: int
    pbar: tqdm
    prev_progress = 0

    def __init__(self, total: int, azm_count: int = 0):
        # Ray actors don't seem to play nice with mypy, generating
        # a spurious warning for the following line,
        # which we need to suppress. The code is fine.
        self.progress_actor = ProgressBarActor.remote()  # type: ignore
        self.total = total
        self.azm_count = azm_count

    @property
    def actor(self) -> ActorHandle:
        """Returns a reference to the remote `ProgressBarActor`.

        When you complete tasks, call `update` on the actor.
        """
        return self.progress_actor

    def print_until_done(self) -> None:
        """Blocking call.

        Do this after starting a series of remote Ray tasks, to which you've
        passed the actor handle. Each of them calls `update` on the actor.
        When the progress meter reaches 100%, this method returns.
        """
        pbar = tqdm(total=self.total)
        while True:
            progress_sum = ray.get(self.actor.wait_for_update.remote())
            progress = progress_sum/self.azm_count
            progress = round(progress, 2)
            delta = progress - self.prev_progress
            pbar.update(delta)
            self.prev_progress = progress
            if progress >= self.total:
                pbar.close()
                return

