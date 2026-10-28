"""Standard select-only pgbench using the pinned compute's PostgreSQL client.

Development fixture only: initializes pgbench tables in the scratch database.
"""
import json
import os
import subprocess
from pathlib import Path

OUT = Path('results/2026-10-28-pgbench')
OUT.mkdir(parents=True, exist_ok=True)
ENGINE = os.environ.get('BENCH_CONTAINER_ENGINE', 'podman')
COMPUTE = os.environ.get('BENCH_COMPUTE_CONTAINER', 'loams-engine-bench_compute1_1')


def execute(arguments):
    command = [ENGINE, 'exec', '--env', 'PGPASSWORD=cloud_admin', COMPUTE, 'pgbench',
               '-U', 'cloud_admin', '-d', 'postgres', *arguments]
    completed = subprocess.run(command, text=True, capture_output=True)
    return {'command': command, 'exit_code': completed.returncode,
            'stdout': completed.stdout, 'stderr': completed.stderr}


initialization = execute(['-h', '127.0.0.1', '-p', '55433', '-i', '-s', '1'])
(OUT / 'initialization.json').write_text(json.dumps(initialization, indent=2))
assert initialization['exit_code'] == 0, initialization['stderr']
for topology, host, port in [('direct', '127.0.0.1', '55433'),
                             ('pgdog', 'loams-bench-pgdog', '6432')]:
    for clients in [1, 8, 32, 64]:
        for repetition in [1, 2, 3]:
            result = execute(['-h', host, '-p', port, '-S', '-M', 'simple', '-n',
                              '-c', str(clients), '-j', str(min(clients, 4)), '-T', '10'])
            result.update(topology=topology, clients=clients, repetition=repetition,
                          workload='pgbench select-only; scale=1; simple query protocol',
                          driver_location='inside compute container; direct uses TCP loopback, PgDog uses container bridge')
            (OUT / f'{topology}-c{clients}-r{repetition}.json').write_text(json.dumps(result, indent=2))
            print(topology, clients, repetition, result['stdout'], result['stderr'], flush=True)
            assert result['exit_code'] == 0
