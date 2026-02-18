"""Tests para PII detection checks."""

import pandas as pd
import pytest

from checks.pii_checks import run_pii_checks


def test_email_detection():
    df_raw = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"] * 20,
        "contact": ["alice@example.com", "bob@test.org", "charlie@mail.net"] * 20,
    })
    df = df_raw.copy()
    results = run_pii_checks(df_raw, df)
    pii_results = [r for r in results if r.check_id == "PII_DETECTED" and r.column == "contact"]
    assert len(pii_results) > 0
    assert any("email" in r.message.lower() for r in pii_results)


def test_credit_card_detection():
    df_raw = pd.DataFrame({
        "card": ["4111-1111-1111-1111", "5500-0000-0000-0004", "3782-822463-10005"] * 20,
    })
    df = df_raw.copy()
    results = run_pii_checks(df_raw, df)
    pii_results = [r for r in results if r.check_id == "PII_DETECTED" and r.column == "card"]
    assert len(pii_results) > 0
    assert any(r.severity == "CRITICAL" for r in pii_results)


def test_ip_detection():
    df_raw = pd.DataFrame({
        "server_ip": ["192.168.1.1", "10.0.0.5", "172.16.0.100"] * 20,
    })
    df = df_raw.copy()
    results = run_pii_checks(df_raw, df)
    ip_results = [r for r in results if "IP" in r.metadata.get("pii_type", "")]
    assert len(ip_results) > 0


def test_no_pii():
    df_raw = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "score": ["95", "87", "72"],
    })
    df = df_raw.copy()
    results = run_pii_checks(df_raw, df)
    assert len(results) == 0


def test_masked_values():
    df_raw = pd.DataFrame({
        "email": ["alice@example.com"] * 20,
    })
    df = df_raw.copy()
    results = run_pii_checks(df_raw, df)
    for r in results:
        for sample in r.sample_values:
            assert "*" in sample  # Valores deben estar enmascarados
