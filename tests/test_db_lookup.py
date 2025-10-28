import yaml, polars as pl
import pytest
from types import SimpleNamespace

import utils.db_lookup as db_lookup

def _write_cfg(tmp_path, **overrides):
    base = {
        "DB_USERNAME": "user",
        "DB_PASSWORD": "pass",
        "DB_HOST": "localhost:5432",
        "DB_NAME": "mydb",
    }
     
    base.update(overrides)
    p = tmp_path / "cfg.yml"
    p.write_text(yaml.safe_dump(base))
    return p

# Create sqa engine tests
def test_create_sql_engine_reads_yaml_returns_engine(monkeypatch, tmp_path):
    calls = []
    test_engine = SimpleNamespace(name="engine")
    
    def fake_create_engine(url, future):
        calls.append({"url": url, "future": future})
        return test_engine
    
    monkeypatch.setattr(db_lookup, "create_engine", fake_create_engine)

    cfg_path = _write_cfg(tmp_path)
    eng = db_lookup.create_sqa_engine(cfg_path)

    assert eng is test_engine
    assert calls == [{
        "url": "postgresql+psycopg2://user:pass@localhost:5432/mydb",
        "future": True,
    }]

# Insert to tmp table tests
def test_insert_to_tmp_tables(monkeypatch):
    df = pl.DataFrame({"a": [1, 2, 3]})
    lf = df.lazy()

    calls = []

    def write_fake_database(self, table_name, connection, engine, if_table_exists, **kwargs):
        calls.append({
            "nrows": self.height,
            "table": table_name,
            "engine": engine,
            "if_exists": if_table_exists,
            "conn": connection,
        })
    
    monkeypatch.setattr(pl.DataFrame, "write_database", write_fake_database)

    fake_conn = object()

    db_lookup.insert_to_tmp_table(
        fake_conn, lf, table_name="tmp_geo_append_table", batch_size=2)

    # Expect two batches, 2 rows, then 1 row
    assert [call["nrows"] for call in calls] == [2, 1]

    # Writes go to pg temp table
    assert all ([call["table"] == "pg_temp.tmp_geo_append_table" 
                 for call in calls])
    
    # First replace, then append
    assert [call["if_exists"] for call in calls] == ["replace", "append"]

    # Engine is sqlalchemy and connection object is passed through
    assert all(call["engine"] == "sqlalchemy" for call in calls)
    assert all(call["conn"] is fake_conn for call in calls)

# Test get_lat_lon
def test_get_lat_lon_builds_expected_sql_and_returns_lazy_df(monkeypatch):
    captured = {}

    returned_df = pl.DataFrame({
        "output_address": ["1234 MARKET ST"],
        "latitude": [39.95],
        "longitude": [-75.16]
    })

    def fake_read_database(query, connection):
        captured["query"] = query
        captured["connection"] = connection

        return returned_df
    
    monkeypatch.setattr(db_lookup.pl, "read_database", fake_read_database)

    fake_conn = object()

    lf = db_lookup.get_lat_lon(fake_conn, "tmp_geo_append_table")

    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().equals(returned_df)

    # Check if important parts present in SQL
    q = captured["query"]
    assert "FROM tmp_geo_append_table b" in q
    assert "LEFT JOIN citygeo.address_summary addr" in q
    assert "b.output_address = addr.street_address" in q
    assert captured["connection"] is fake_conn

# Test append (without DB)
def test_append_calls_insert_and_returns_join_result(monkeypatch):
    input_df = pl.DataFrame({"output_address": ["1234 MARKET ST"]})
    input_lf = input_df.lazy()

    class FakeConn:
        pass

    class FakeEngine:
        def begin(self):
            class _Ctx:
                def __enter__(self_inner): return FakeConn()
                def __exit__(self_inner, exc_type, exc, tb): return False
            
            return _Ctx()

    # Create a sqa engine function that returns a fake engine
    monkeypatch.setattr(db_lookup, "create_sqa_engine", lambda _cfg: FakeEngine())

    write_calls = []
    
    def fake_write_database(self, table_name, connection, engine, if_table_exists, **kwargs):
        write_calls.append((self.height, table_name, if_table_exists))
    
    monkeypatch.setattr(pl.DataFrame, "write_database", fake_write_database)

    joined_df = pl.DataFrame({
        "output_address": ["123 MARKET ST"],
        "latitude": [39.95],
        "longitude": [-75.16],
    })

    monkeypatch.setattr(db_lookup.pl, "read_database", lambda query, connection: joined_df)

     # Act
    out_lf = db_lookup.append(input_lf, config_path="ignored.yml")

    # Assert that insert happened and returned lazy result of the join
    assert write_calls  # at least one call was made
    assert isinstance(out_lf, pl.LazyFrame)
    assert out_lf.collect().equals(joined_df)


def test_insert_to_tmp_table_with_empty_input_calls_nothing(monkeypatch):
    empty_lf = pl.DataFrame({"a": []}).lazy()
    calls = []
    monkeypatch.setattr(pl.DataFrame, "write_database", lambda *a, **k: calls.append(1))

    db_lookup.insert_to_tmp_table(object(), empty_lf, table_name="t", batch_size=10)
    assert calls == []



