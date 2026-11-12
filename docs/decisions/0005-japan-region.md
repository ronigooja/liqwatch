# 0005: Deploy in Japan (Tokyo)

The recorder host was later chosen; see [infrastructure.md](../infrastructure.md).

Status: accepted

Context: the owner investigated and found the exchange servers are in Japan. v1 listed Tokyo, Singapore, and Hong Kong as candidates to be compared.

Decision: record and run in Japan. Skip the multi-region comparison. Keep latency measurement as a baseline only.

Consequences: simplifies P0 region selection in [data-recording.md](../data-recording.md). The exact provider and availability zone are still open ([open-questions.md](../open-questions.md)).
