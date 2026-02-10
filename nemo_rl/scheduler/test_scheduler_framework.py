


from nemo_rl.scheduler.scheduler_framework import NemoRequestScheduler


def test_scheduler():
    scheduler = NemoRequestScheduler()
    candidates = scheduler.run(request=None, candidates=[{"name": "endpoint1"}, {"name": "endpoint2"}])
    print(candidates)
    assert candidates is not None