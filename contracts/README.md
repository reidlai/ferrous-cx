# Contracts registry

Cross-component contracts for the platform (AD-27).

| File | What it is | Status |
|------|------------|--------|
| [`topics.yml`](topics.yml) | Every topic as `{stream, topic, kafka_name, partitions, retention, durability, kafka}`. `ferrous-streaming` generates the Iggy-Kafka bridge topic map from it. | in use |
| [`bridge-contract-sheet.md`](bridge-contract-sheet.md) | The Iggy-Kafka Bridge Contract Sheet (AD-28 rule 8). | **draft, desk research only: not yet run against a live stack** (live checks L1-L14 open) |

The evidence behind the sheet and the adopt decision are in the root repository under `_bmad-output/implementation-artifacts/spike-3-13/`.

To change a topic, edit `topics.yml`, then regenerate the bridge map in `ferrous-streaming`:

```bash
python ferrous-streaming/scripts/generate-bridge-topic-map.py
```
