


from nemo_rl.scheduler.scheduler_framework import NemoRequestScheduler
from nemo_rl.scheduler.scheduler_framework import BlindForthPicker
from scheduling.types import Endpoint, ScoredEndpoint, CycleState, LLMRequest


def test_scheduler():
    scheduler = NemoRequestScheduler()
    candidates = scheduler.run(request=None, candidates=[{"name": "endpoint1"}, {"name": "endpoint2"}])
    print(candidates)
    assert candidates is not None


def test_blind_forth_picker():
    picker = BlindForthPicker()
    scored_endpoints = [ScoredEndpoint(endpoint={"name": f"endpoint{i}"}, score=i) for i in range(12)]
    request = LLMRequest(request_id="test_request", target_model="test_model", body="test_body")
    cycle_state = CycleState()
    
    selected_endpoint = picker.pick(cycle_state, request, scored_endpoints)
    print(f"Selected endpoint: {selected_endpoint}")
    assert selected_endpoint is not None