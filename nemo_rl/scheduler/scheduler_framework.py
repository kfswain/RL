
from scheduling.framework import ScorerPlugin, PickerPlugin, SchedulerProfile, WeightedScorer, FilterPlugin
from scheduling.types import Endpoint, ScoredEndpoint, CycleState, LLMRequest
from scheduling.plugins import SingleProfileHandler
from scheduling.scheduler_config import SchedulerConfig
from scheduling import PrefixCacheScorer
from scheduling.prefix_plugin import _hash_prompt_bytes, _get_user_input_bytes
from scheduling.scheduler import Scheduler
from typing import Dict, Optional, Sequence, Mapping
import random
import time

from typing import (
    List,
    Optional,
    Callable
)

#defining an IGW-like scheduler

class NemoRequestScheduler:
    def __init__(self):
        prefix_profile = SchedulerProfile(name="ray_example").with_filters(StaleEndpointFilter()).with_filters(KVUtilFilter()).with_filters(QueueDepthFilter()).with_scorers(WeightedScorer(PrefixCacheScorer(), 1.0)).with_picker(MaxScorePicker())
        config = SchedulerConfig(profile_handler=SingleProfileHandler(), profiles={
            prefix_profile.name: prefix_profile
        })
        self.scheduler = Scheduler.new_with_config(config)
        print("Scheduler initialized with profile ray_example")
    
    def run(self, request: LLMRequest, candidates: Sequence[Endpoint]) -> Endpoint:
        scheduler_output = self.scheduler.schedule(request, candidates)
        profile_name = scheduler_output.primary_profile_name
        profile_results = scheduler_output.profile_results.get(profile_name)

        selected_endpoint = profile_results.endpoint_list[:1]
        
        # This is in lieu of the PreRequest hook point. Which should be added, but getting something functional for now
        if len(selected_endpoint) > 0:
            self.pre_request(request, selected_endpoint[0].endpoint, profile_name)
            return selected_endpoint  # pick top 1
        print("No endpoint selected")
        return None

    def pre_request(self, request: LLMRequest, selected_endpoint: Endpoint, profile_name: str):
        scorer = self.scheduler.profiles[profile_name].scorers[0].scorer
        selected_endpoint.attributes["requests_since_metric_update"] += 1
        if isinstance(scorer, PrefixCacheScorer) and request.body is not None:
            scorer.add_prefixes_for_server(selected_endpoint.name, _hash_prompt_bytes(request.target_model, _get_user_input_bytes(request.body), 64, 256))

    
class MaxScorePicker(PickerPlugin):
    def pick(self, cycle_state: CycleState, request: LLMRequest, scored_endpoints: Sequence[ScoredEndpoint]) -> Optional[ScoredEndpoint]:
        if not scored_endpoints:
            return None
        # pick the endpoint with the highest score
        return max(scored_endpoints, key=lambda se: se.score)
    
class BlindForthPicker(PickerPlugin):
    def pick(self, cycle_state: CycleState, request: LLMRequest, scored_endpoints: Sequence[ScoredEndpoint]) -> Optional[ScoredEndpoint]:
        if not scored_endpoints:
            return None
        # pick a random endpoint
        num_to_remove = len(scored_endpoints) // 4
        for _ in range(num_to_remove):
            scored_endpoints.pop(random.randint(0, len(scored_endpoints) - 1))
        return max(scored_endpoints, key=lambda se: se.score)

class StaleEndpointFilter(FilterPlugin):
    def filter(self, cycle_state: CycleState, request: LLMRequest, endpoints: Mapping[str, Endpoint]) -> Mapping[str, Endpoint]:
        filtered_endpoints = {}
        for idx, ep in endpoints.items():
            if ep.attributes["requests_since_metric_update"] < 15:
                filtered_endpoints[idx] = ep
        return filtered_endpoints

class QueueDepthFilter(FilterPlugin):
    def __init__(self, queue_depth_threshold: int = 10):
        self.queue_depth_threshold = queue_depth_threshold
    def filter(self, cycle_state: CycleState, request: LLMRequest, endpoints: Mapping[str, Endpoint]) -> Mapping[str, Endpoint]:
        # filter out endpoints with queue depth > 10
        filtered_endpoints = {}
        for idx, ep in endpoints.items():
            if "num_pending_samples" not in ep.attributes:
                #dont filter if the data doesnt exist
                filtered_endpoints[idx] = ep
            elif ep.attributes["num_pending_samples"] < self.queue_depth_threshold:
                filtered_endpoints[idx] = ep
        return filtered_endpoints
    
class KVUtilFilter(FilterPlugin):
    def __init__(self, kv_util_threshold: float = 0.75):
        self.kv_util_threshold = kv_util_threshold

    def filter(self, cycle_state: CycleState, request: LLMRequest, endpoints: Mapping[str, Endpoint]) -> Mapping[str, Endpoint]:
        filtered_endpoints = {}
        for idx, ep in endpoints.items():
            if "kv_cache_usage_perc" not in ep.attributes:
                #dont filter if the data doesnt exist
                filtered_endpoints[idx] = ep
            elif ep.attributes["kv_cache_usage_perc"] < self.kv_util_threshold:
                filtered_endpoints[idx] = ep
        return filtered_endpoints