"""Runtime-generated synthetic ORU^R01 drill data for the audit feature.

Nothing in this package is a real instrument feed: every message is built
in-process from explicit, hand-checkable rows.  Data is generated at run
time and never committed as a large dataset to the repository.

The drill deliberately packs every required edge case into one small
database:

* multiple senders in the same windows;
* multiple test codes and OBX-2 types (NM, ST, TX, FT, CE, CWE);
* empty-string vs NULL optional fields;
* text values and numeric values, including unparseable NM text;
* messages landing exactly on window boundary timestamps (half-open check);
* low sample sizes, threshold breaches and metrics with no computable
  sample;
* a rejected message whose inbox record must never count as a fact.

The hand-derived expectations used by the test suite live in
``tests/test_audit_drill.py``; the independent cross-check lives in
``refcheck/audit_reference.py``.
"""
