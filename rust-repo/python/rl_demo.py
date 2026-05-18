#!/usr/bin/env python3
"""
Dummy RL training example — demonstrates Mooncake Store (Rust-powered)
for data transmission between rollout engines and training engines.

Mirrors the functionality of mooncake-rl/examples/rl_samples.py,
but uses the Rust-backed mooncake_store Python client.

Usage:
    python rl_demo.py [--num_rollout 5] [--num_train_actor 2] [--num_rollout_actor 2]
"""

import asyncio
import pickle
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import torch

from mooncake_store import MooncakeClient


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class RolloutSample:
    rollout_id: int
    obs: List[int]
    action: int
    reward: float


# ---------------------------------------------------------------------------
# TrainActor — performs one training step on a single sample
# ---------------------------------------------------------------------------


class TrainActor:
    def __init__(self):
        self.model = torch.nn.Linear(10, 2)
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-3)

    def init_model(self, args=None):
        torch.nn.init.xavier_uniform_(self.model.weight)
        torch.nn.init.zeros_(self.model.bias)
        print("[TrainActor] Model and optimizer initialized")

    def train(self, sample: RolloutSample) -> float:
        self.model.train()
        obs = torch.tensor(sample.obs, dtype=torch.float32).unsqueeze(0)
        action = sample.action
        reward = torch.tensor([sample.reward], dtype=torch.float32)

        logits = self.model(obs)
        pred = logits[0, action % logits.shape[1]]
        loss = (pred - reward).pow(2).mean()

        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()

        print(
            f"  [TrainActor] action={action} reward={reward.item():.4f} loss={loss.item():.4f}"
        )
        return loss.item()

    def save_model(self, rollout_id: int):
        path = f"model_{rollout_id}.pth"
        torch.save(self.model.state_dict(), path)
        print(f"  [TrainActor] Model saved to {path}")


# ---------------------------------------------------------------------------
# TrainGroup — manages training actors, connects to MooncakeStore
# ---------------------------------------------------------------------------


class TrainGroup:
    def __init__(self, args):
        self.world_size = args.num_train_actor
        self.actor_handlers = [TrainActor() for _ in range(self.world_size)]
        self._client: Optional[MooncakeClient] = None

    async def setup_store(
        self,
        local_hostname: str,
        metadata_server: str,
        master_server_addr: str,
    ):
        self._client = await MooncakeClient.create(
            local_hostname=local_hostname,
            metadata_server=metadata_server,
            master_server_addr=master_server_addr,
            protocol="tcp",
            device="",
            global_segment_size=64 * 1024 * 1024,  # also serve as storage node
            local_buffer_size=128 * 1024 * 1024,
        )

    def init_actors(self, args, role="actor"):
        for actor in self.actor_handlers:
            actor.init_model(args)
        print("[TrainGroup] Initialized")
        return [0]

    def init_weight_update_connections(self, rollout_manager):
        print("[TrainGroup] Connected to rollout manager")

    def update_weights(self):
        print("[TrainGroup] Weights updated")

    async def train(self, rollout_id: int, rollout_key: str):
        assert self._client is not None

        try:
            raw = await self._client.get(rollout_key)
            if not raw:
                print(f"[TrainGroup] Rollout {rollout_id} no data")
                return
            samples: List[RolloutSample] = pickle.loads(raw)
        except Exception as e:
            print(f"[TrainGroup] Rollout {rollout_id} unavailable: {e}")
            return

        losses = []
        for actor, sample in zip(self.actor_handlers, samples):
            loss = actor.train(sample)
            losses.append(loss)

        if losses:
            avg_loss = sum(losses) / len(losses)
            print(f"[TrainGroup] Rollout {rollout_id} avg loss: {avg_loss:.4f}")

    def save_model(self, rollout_id: int):
        for actor in self.actor_handlers:
            actor.save_model(rollout_id)
        print(f"[TrainGroup] Checkpoint saved at rollout {rollout_id}")

    def close(self):
        if self._client:
            self._client.close()


# ---------------------------------------------------------------------------
# RolloutEngine — generates mock rollout samples
# ---------------------------------------------------------------------------


class RolloutEngine:
    def __init__(self, args):
        pass

    def generate(self, rollout_id: int) -> RolloutSample:
        obs = torch.randint(0, 100, (4,), dtype=torch.int32).tolist()
        action = random.randint(0, 9)
        reward = random.uniform(-1.0, 1.0)
        return RolloutSample(rollout_id=rollout_id, obs=obs, action=action, reward=reward)

    def eval(self, rollout_id: int, samples: List[RolloutSample]):
        avg_reward = sum(s.reward for s in samples) / len(samples)
        eval_score = avg_reward**2
        print(
            f"[RolloutEngine] Rollout {rollout_id} "
            f"(action={samples[0].action}, avg_reward={avg_reward:.4f}) "
            f"=> eval_score={eval_score:.4f}"
        )


# ---------------------------------------------------------------------------
# RolloutController — manages dataset state + MooncakeStore access
# ---------------------------------------------------------------------------


class RolloutController:
    def __init__(self, args):
        self.args = args
        self.epoch_id = 0
        self.sample_index = 0
        self.sample_offset = 0
        self.metadata: Dict[str, Any] = {}
        self._client: Optional[MooncakeClient] = None

    async def setup_store(
        self,
        local_hostname: str,
        metadata_server: str,
        master_server_addr: str,
    ):
        self._client = await MooncakeClient.create(
            local_hostname=local_hostname,
            metadata_server=metadata_server,
            master_server_addr=master_server_addr,
            protocol="tcp",
            device="",
            global_segment_size=0,
            local_buffer_size=128 * 1024 * 1024,
        )

    def load(self, rollout_id: int):
        print(f"  [Controller] load metadata for rollout {rollout_id}")

    def save(self, rollout_id: int):
        import os

        state_dict = {
            "sample_offset": self.sample_offset,
            "epoch_id": self.epoch_id,
            "sample_index": self.sample_index,
            "metadata": self.metadata,
        }
        path = os.path.join(
            self.args.model_path, f"rollout/global_dataset_state_dict_{rollout_id}.pt"
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        torch.save(state_dict, path)
        print(f"  [Controller] saved state to {path}")

    async def store_rollout(self, key: str, samples: List[RolloutSample]):
        assert self._client is not None
        payload = pickle.dumps(samples, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"    [store_rollout] key={key} len={len(payload)}")
        await self._client.put(key, payload)
        print(f"    [store_rollout] put succeeded for {key}")

    async def fetch_rollout(self, key: str) -> Optional[List[RolloutSample]]:
        assert self._client is not None
        try:
            raw = await self._client.get(key)
            if not raw:
                return None
            return pickle.loads(raw)
        except Exception as e:
            if "not found" in str(e):
                return None
            print(f"  [Controller] fetch failed for {key}: {e}")
            return None

    def close(self):
        if self._client:
            self._client.close()


# ---------------------------------------------------------------------------
# RolloutManager — manages rollout engines + controller
# ---------------------------------------------------------------------------


class RolloutManager:
    def __init__(self, args):
        self.controller = RolloutController(args)
        self.rollout_engines = [RolloutEngine(args) for _ in range(args.num_rollout_actor)]

    async def generate(self, rollout_id: int) -> str:
        samples = [engine.generate(rollout_id) for engine in self.rollout_engines]
        key = f"rl/rollout/{rollout_id}"
        print(f"  [RolloutManager] storing {len(samples)} samples at key={key}")
        await self.controller.store_rollout(key, samples)
        print(f"  [RolloutManager] Generated rollout {rollout_id}: {len(samples)} samples")
        return key

    async def eval(self, rollout_id: int):
        key = f"rl/rollout/{rollout_id}"
        samples = await self.controller.fetch_rollout(key)
        if samples is None:
            print(f"[RolloutManager] Rollout {rollout_id} not found in store")
            return
        for engine in self.rollout_engines:
            engine.eval(rollout_id, samples)
        print(f"[RolloutManager] Evaluation at rollout {rollout_id}")

    def close(self):
        self.controller.close()


# ---------------------------------------------------------------------------
# Main training loop
# ---------------------------------------------------------------------------


async def train(args):
    import os

    # -- create participants --
    train_group = TrainGroup(args)
    rollout_manager = RolloutManager(args)

    # -- connect to Mooncake Master --
    master_addr = os.environ.get("MOONCAKE_MASTER", "localhost:50051")
    metadata_server = os.environ.get("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE")
    local_host = os.environ.get("MOONCAKE_LOCAL_HOSTNAME", "localhost")

    await train_group.setup_store(
        local_hostname=f"{local_host}-train",
        metadata_server=metadata_server,
        master_server_addr=master_addr,
    )
    await rollout_manager.controller.setup_store(
        local_hostname=f"{local_host}-rollout",
        metadata_server=metadata_server,
        master_server_addr=master_addr,
    )

    # -- init --
    _ = train_group.init_actors(args)
    rollout_manager.controller.load(args.start_rollout_id - 1)
    train_group.init_weight_update_connections(rollout_manager)
    train_group.update_weights()

    # -- train loop --
    for rollout_id in range(args.start_rollout_id, args.num_rollout):
        if args.eval_interval is not None and rollout_id == 0:
            await rollout_manager.eval(rollout_id)

        rollout_key = await rollout_manager.generate(rollout_id)
        await train_group.train(rollout_id, rollout_key)

        if args.save_interval is not None and (rollout_id + 1) % args.save_interval == 0:
            train_group.save_model(rollout_id)
            rollout_manager.controller.save(rollout_id)

        train_group.update_weights()

        if args.eval_interval is not None and (rollout_id + 1) % args.eval_interval == 0:
            await rollout_manager.eval(rollout_id)

    # -- cleanup --
    train_group.close()
    rollout_manager.close()
    print("✓ RL demo finished")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(description="Dummy RL training with MooncakeStore (Rust)")
    parser.add_argument("--num_rollout", type=int, default=5)
    parser.add_argument("--num_train_actor", type=int, default=2)
    parser.add_argument("--num_rollout_actor", type=int, default=2)
    parser.add_argument("--save_interval", type=int, default=2)
    parser.add_argument("--eval_interval", type=int, default=2)
    parser.add_argument("--model_path", type=str, default="./checkpoints")
    parser.add_argument("--start_rollout_id", type=int, default=0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(train(args))
