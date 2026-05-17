"""
Olympic Power Shift P0 data pipeline.

This script builds a reproducible, stable set of CSV outputs for the
Olympic Power Shift project. It intentionally uses lowercase snake_case
columns in every exported analysis table because the HTML prototype reads
that contract directly.

Run from the project root:
    python olympic_cleaning_pipeline.py

Optional:
    python olympic_cleaning_pipeline.py --config config/project_config.yaml
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "project_config.yaml"


DEFAULT_CONFIG: dict[str, Any] = {
    "project": {
        "name": "Olympic Power Shift",
        "season": "Summer",
        "year_min": 1896,
        "year_max": 2024,
    },
    "paths": {
        "data_dir": ".",
        "output_dir": "outputs",
    },
    "inputs": {
        "olympics": "olympics_dataset.csv",
        "athlete_events": "athlete_events.csv",
        "noc_regions": "noc_regions.csv",
        "gdp_per_capita": "gdp-per-capita-worldbank.csv",
        "population": "population.csv",
        "gdp_total": "gdp-penn-world-table.csv",
    },
    "outputs": {
        "clean_olympic_events": "clean_olympic_events.csv",
        "clean_athlete_events_summer": "clean_athlete_events_summer.csv",
        "official_event_medals": "official_event_medals.csv",
        "medal_unique_event_country": "medal_unique_event_country.csv",
        "olympic_timeline": "olympic_timeline_1896_2024.csv",
        "country_year_medals": "country_year_medals.csv",
        "country_sport_year_medals": "country_sport_year_medals.csv",
        "sport_year_country_share": "sport_year_country_share.csv",
        "athlete_demographics": "athlete_demographics.csv",
        "country_gdp_population_medal_efficiency": "country_gdp_population_medal_efficiency.csv",
        "country_economic_efficiency": "country_economic_efficiency.csv",
        "country_expected_vs_actual_medals": "country_expected_vs_actual_medals.csv",
        "country_multi_normalized_efficiency": "country_multi_normalized_efficiency.csv",
        "host_years": "host_years.csv",
        "host_effect_country_sport_panel": "host_effect_country_sport_panel.csv",
        "host_effect_summary": "host_effect_summary.csv",
        "host_effect_summary_modern": "host_effect_summary_modern.csv",
        "event_added_removed": "event_added_removed.csv",
        "event_opportunity_refined": "event_opportunity_refined.csv",
        "coach_events_template": "coach_events_template.csv",
        "coach_events_template_examples": "coach_events_template_examples.csv",
        "gender_event_expansion_by_year": "gender_event_expansion_by_year.csv",
        "country_gender_opportunity_capture": "country_gender_opportunity_capture.csv",
        "country_gender_medal_structure": "country_gender_medal_structure.csv",
        "country_gender_growth_decomposition": "country_gender_growth_decomposition.csv",
        "country_profile_features": "country_profile_features.csv",
        "country_cluster_labels": "country_cluster_labels.csv",
        "sport_entry_barrier_index": "sport_entry_barrier_index.csv",
        "sport_entry_barrier_modern": "sport_entry_barrier_modern.csv",
        "sport_breakthrough_cases": "sport_breakthrough_cases.csv",
        "leap_mechanism_decomposition": "leap_mechanism_decomposition.csv",
        "leap_mechanism_enhanced": "leap_mechanism_enhanced.csv",
        "leap_case_library": "leap_case_library.csv",
        "mechanism_case_cards": "mechanism_case_cards.csv",
        "mechanism_reporting_filter": "mechanism_reporting_filter.csv",
        "country_year_sport_concentration": "country_year_sport_concentration.csv",
        "sport_globalization_metrics": "sport_globalization_metrics.csv",
        "leap_index_country_sport": "leap_index_country_sport.csv",
        "data_quality_report": "data_quality_report.csv",
        "table_manifest": "table_manifest.csv",
        "data_dictionary": "data_dictionary.csv",
        "metadata_run_log": "metadata_run_log.json",
    },
    "medals": {
        "order": ["Gold", "Silver", "Bronze"],
        "score": {"Gold": 3, "Silver": 2, "Bronze": 1},
    },
}


COUNTRY_RENAME = {
    "USA": "United States",
    "UK": "United Kingdom",
    "Great Britain": "United Kingdom",
    "Britain": "United Kingdom",
    "Russia": "Russia",
    "ROC": "Russia",
    "Russian Olympic Committee": "Russia",
    "Soviet Union": "Soviet Union",
    "USSR": "Soviet Union",
    "West Germany": "Germany",
    "East Germany": "Germany",
    "Federal Republic of Germany": "Germany",
    "German Democratic Republic": "Germany",
    "Czech Republic": "Czechia",
    "Korea, South": "South Korea",
    "Korea, North": "North Korea",
    "Iran, Islamic Republic of": "Iran",
    "Viet Nam": "Vietnam",
    "Republic of Moldova": "Moldova",
    "Syrian Arab Republic": "Syria",
    "United Republic of Tanzania": "Tanzania",
    "Venezuela, RB": "Venezuela",
    "Bolivia, Plurinational State of": "Bolivia",
    "Lao PDR": "Laos",
    "Ivory Coast": "Cote d'Ivoire",
    "Cote dIvoire": "Cote d'Ivoire",
    "C么te d'Ivoire": "Cote d'Ivoire",
    "Cape Verde": "Cabo Verde",
    "Swaziland": "Eswatini",
    "Macedonia": "North Macedonia",
    "Macedonia, FYR": "North Macedonia",
    "Chinese Taipei": "Taiwan",
}


NOC_COUNTRY_FIX = {
    "USA": "United States",
    "GBR": "United Kingdom",
    "CHN": "China",
    "RUS": "Russia",
    "ROC": "Russia",
    "URS": "Soviet Union",
    "EUN": "Unified Team",
    "FRG": "Germany",
    "GDR": "Germany",
    "GER": "Germany",
    "KOR": "South Korea",
    "PRK": "North Korea",
    "TPE": "Taiwan",
    "HKG": "Hong Kong",
    "IRI": "Iran",
    "VIE": "Vietnam",
    "CIV": "Cote d'Ivoire",
}


NOC_TO_ISO3_FIX = {
    "GER": "DEU",
    "FRG": "DEU",
    "GDR": "DEU",
    "GRE": "GRC",
    "SUI": "CHE",
    "NED": "NLD",
    "DEN": "DNK",
    "POR": "PRT",
    "CRO": "HRV",
    "SLO": "SVN",
    "LAT": "LVA",
    "RSA": "ZAF",
    "INA": "IDN",
    "MAS": "MYS",
    "PHI": "PHL",
    "IRI": "IRN",
    "VIE": "VNM",
    "TPE": "TWN",
    "KOR": "KOR",
    "PRK": "PRK",
    "CHI": "CHL",
    "CRC": "CRI",
    "PUR": "PRI",
    "ALG": "DZA",
    "ANG": "AGO",
    "BOT": "BWA",
    "BUL": "BGR",
    "COD": "COD",
    "CGO": "COG",
    "TAN": "TZA",
    "UAE": "ARE",
    "ESA": "SLV",
    "MON": "MCO",
    "NGR": "NGA",
    "NIG": "NER",
    "PAR": "PRY",
    "SUD": "SDN",
    "SRI": "LKA",
    "TOG": "TGO",
    "ZIM": "ZWE",
}


HOST_CITY_MAP = {
    1896: {"city": "Athina", "host_country": "Greece", "host_noc": "GRE", "host_continent": "Europe"},
    1900: {"city": "Paris", "host_country": "France", "host_noc": "FRA", "host_continent": "Europe"},
    1904: {"city": "St. Louis", "host_country": "United States", "host_noc": "USA", "host_continent": "North America"},
    1906: {"city": "Athina", "host_country": "Greece", "host_noc": "GRE", "host_continent": "Europe"},
    1908: {"city": "London", "host_country": "United Kingdom", "host_noc": "GBR", "host_continent": "Europe"},
    1912: {"city": "Stockholm", "host_country": "Sweden", "host_noc": "SWE", "host_continent": "Europe"},
    1920: {"city": "Antwerpen", "host_country": "Belgium", "host_noc": "BEL", "host_continent": "Europe"},
    1924: {"city": "Paris", "host_country": "France", "host_noc": "FRA", "host_continent": "Europe"},
    1928: {"city": "Amsterdam", "host_country": "Netherlands", "host_noc": "NED", "host_continent": "Europe"},
    1932: {"city": "Los Angeles", "host_country": "United States", "host_noc": "USA", "host_continent": "North America"},
    1936: {"city": "Berlin", "host_country": "Germany", "host_noc": "GER", "host_continent": "Europe"},
    1948: {"city": "London", "host_country": "United Kingdom", "host_noc": "GBR", "host_continent": "Europe"},
    1952: {"city": "Helsinki", "host_country": "Finland", "host_noc": "FIN", "host_continent": "Europe"},
    1956: {"city": "Melbourne", "host_country": "Australia", "host_noc": "AUS", "host_continent": "Oceania"},
    1960: {"city": "Roma", "host_country": "Italy", "host_noc": "ITA", "host_continent": "Europe"},
    1964: {"city": "Tokyo", "host_country": "Japan", "host_noc": "JPN", "host_continent": "Asia"},
    1968: {"city": "Mexico City", "host_country": "Mexico", "host_noc": "MEX", "host_continent": "North America"},
    1972: {"city": "Munich", "host_country": "Germany", "host_noc": "GER", "host_noc_compatible": "FRG", "host_continent": "Europe"},
    1976: {"city": "Montreal", "host_country": "Canada", "host_noc": "CAN", "host_continent": "North America"},
    1980: {"city": "Moskva", "host_country": "Soviet Union", "host_noc": "URS", "host_continent": "Europe"},
    1984: {"city": "Los Angeles", "host_country": "United States", "host_noc": "USA", "host_continent": "North America"},
    1988: {"city": "Seoul", "host_country": "South Korea", "host_noc": "KOR", "host_continent": "Asia"},
    1992: {"city": "Barcelona", "host_country": "Spain", "host_noc": "ESP", "host_continent": "Europe"},
    1996: {"city": "Atlanta", "host_country": "United States", "host_noc": "USA", "host_continent": "North America"},
    2000: {"city": "Sydney", "host_country": "Australia", "host_noc": "AUS", "host_continent": "Oceania"},
    2004: {"city": "Athina", "host_country": "Greece", "host_noc": "GRE", "host_continent": "Europe"},
    2008: {"city": "Beijing", "host_country": "China", "host_noc": "CHN", "host_continent": "Asia"},
    2012: {"city": "London", "host_country": "United Kingdom", "host_noc": "GBR", "host_continent": "Europe"},
    2016: {"city": "Rio de Janeiro", "host_country": "Brazil", "host_noc": "BRA", "host_continent": "South America"},
    2020: {"city": "Tokyo", "host_country": "Japan", "host_noc": "JPN", "host_continent": "Asia"},
    2024: {"city": "Paris", "host_country": "France", "host_noc": "FRA", "host_continent": "Europe"},
}


def load_simple_yaml(path: Path) -> dict[str, Any]:
    """Load the project YAML subset without requiring PyYAML."""
    if not path.exists():
        return {}

    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        if ":" not in line:
            continue
        key, value = line.strip().split(":", 1)
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if not value:
            node: dict[str, Any] = {}
            parent[key] = node
            stack.append((indent, node))
        else:
            parent[key] = parse_yaml_scalar(value)
    return root


def parse_yaml_scalar(value: str) -> Any:
    value = value.strip().strip('"').strip("'")
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [parse_yaml_scalar(part.strip()) for part in inner.split(",")]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def clean_text(value: Any) -> Any:
    if pd.isna(value):
        return np.nan
    return re.sub(r"\s+", " ", str(value).strip())


def standardize_medal(value: Any) -> str:
    if pd.isna(value):
        return "No medal"
    text = str(value).strip().lower()
    if text in {"gold", "g"}:
        return "Gold"
    if text in {"silver", "s"}:
        return "Silver"
    if text in {"bronze", "b"}:
        return "Bronze"
    if text in {"na", "nan", "none", "no medal", "nomedal", "no_medal", ""}:
        return "No medal"
    return str(clean_text(value))


def standardize_season(value: Any) -> Any:
    if pd.isna(value):
        return np.nan
    text = str(value).strip().title()
    return text


def standardize_country_name(value: Any) -> Any:
    value = clean_text(value)
    if pd.isna(value):
        return np.nan
    return COUNTRY_RENAME.get(str(value), str(value))


def safe_divide(a: Any, b: Any) -> Any:
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)


def hhi_from_counts(counts: pd.Series) -> float:
    total = counts.sum()
    if total == 0 or pd.isna(total):
        return np.nan
    shares = counts / total
    return float((shares**2).sum())


def entropy_from_counts(counts: pd.Series) -> float:
    total = counts.sum()
    if total == 0 or pd.isna(total):
        return np.nan
    p = counts / total
    p = p[p > 0]
    return float(-(p * np.log(p)).sum())


def zscore_by_year(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if std == 0 or pd.isna(std):
        return series * 0
    return (series - series.mean()) / std


def to_export_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "Athlete_ID": "athlete_id",
        "Athlete_Name": "athlete_name",
        "Sex": "sex",
        "Age": "age",
        "Height": "height",
        "Weight": "weight",
        "Team": "team",
        "NOC": "noc",
        "Games": "games",
        "Year": "year",
        "Season": "season",
        "City": "city",
        "Sport": "sport",
        "Event": "event",
        "Medal": "medal",
        "country": "country_name",
    }
    out = df.rename(columns={c: rename.get(c, c) for c in df.columns}).copy()
    out.columns = [re.sub(r"[^0-9a-zA-Z]+", "_", c).strip("_").lower() for c in out.columns]
    return out


def read_csv_required(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input for {label}: {path}")
    return pd.read_csv(path)


def normalize_event_table(df: pd.DataFrame, source_name: str, noc_map: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [clean_text(c) for c in df.columns]
    rename_map = {
        "player_id": "Athlete_ID",
        "ID": "Athlete_ID",
        "Name": "Athlete_Name",
        "Sex": "Sex",
        "Team": "Team",
        "NOC": "NOC",
        "Games": "Games",
        "Year": "Year",
        "Season": "Season",
        "City": "City",
        "Sport": "Sport",
        "Event": "Event",
        "Medal": "Medal",
        "Age": "Age",
        "Height": "Height",
        "Weight": "Weight",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    needed = [
        "Athlete_ID",
        "Athlete_Name",
        "Sex",
        "Age",
        "Height",
        "Weight",
        "Team",
        "NOC",
        "Games",
        "Year",
        "Season",
        "City",
        "Sport",
        "Event",
        "Medal",
    ]
    for column in needed:
        if column not in df.columns:
            df[column] = np.nan

    df = df[needed].copy()
    df["source"] = source_name
    df["NOC"] = df["NOC"].astype(str).str.upper().str.strip()
    for column in ["Athlete_Name", "Team", "Games", "City", "Sport", "Event"]:
        df[column] = df[column].apply(clean_text)
    df["Season"] = df["Season"].apply(standardize_season)
    df["Medal"] = df["Medal"].apply(standardize_medal)
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["Sex"] = df["Sex"].astype(str).str.upper().str.strip().replace({"NAN": np.nan})
    for column in ["Age", "Height", "Weight"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.merge(noc_map, on="NOC", how="left")
    df["country"] = df["country_from_noc"]
    df["country"] = df["country"].fillna(df["Team"].apply(standardize_country_name))
    df["country"] = df.apply(lambda row: NOC_COUNTRY_FIX.get(row["NOC"], row["country"]), axis=1)
    df["country"] = df["country"].apply(standardize_country_name)
    df["iso3"] = df["NOC"].map(NOC_TO_ISO3_FIX).fillna(df["NOC"])
    df["is_medal"] = df["Medal"].isin(["Gold", "Silver", "Bronze"]).astype(int)
    df["is_gold"] = (df["Medal"] == "Gold").astype(int)
    df["medal_score"] = df["Medal"].map({"Gold": 3, "Silver": 2, "Bronze": 1}).fillna(0).astype(int)
    return df


def build_pipeline(config: dict[str, Any]) -> dict[str, pd.DataFrame]:
    data_dir = (PROJECT_ROOT / config["paths"]["data_dir"]).resolve()
    output_dir = (PROJECT_ROOT / config["paths"]["output_dir"]).resolve()
    season = config["project"]["season"]
    year_min = int(config["project"]["year_min"])
    year_max = int(config["project"]["year_max"])

    paths = {key: data_dir / filename for key, filename in config["inputs"].items()}
    olympics = read_csv_required(paths["olympics"], "olympics")
    athlete = read_csv_required(paths["athlete_events"], "athlete_events")
    noc = read_csv_required(paths["noc_regions"], "noc_regions")

    noc.columns = [clean_text(c) for c in noc.columns]
    noc["NOC"] = noc["NOC"].astype(str).str.upper().str.strip()
    noc["country_from_noc"] = noc["region"].apply(standardize_country_name)
    noc_map = noc[["NOC", "country_from_noc"]].drop_duplicates("NOC")

    events_main = normalize_event_table(olympics, "olympics_dataset_1896_2024", noc_map)
    athlete_clean = normalize_event_table(athlete, "athlete_events_1896_2016", noc_map)

    events_main = events_main[events_main["Season"].eq(season)].copy()
    athlete_clean = athlete_clean[athlete_clean["Season"].eq(season)].copy()
    events_main = events_main.dropna(subset=["Year", "Season", "NOC", "Sport", "Event"])
    athlete_clean = athlete_clean.dropna(subset=["Year", "Season", "NOC", "Sport", "Event"])
    events_main = events_main[(events_main["Year"] >= year_min) & (events_main["Year"] <= year_max)].copy()
    athlete_clean = athlete_clean[(athlete_clean["Year"] >= year_min) & (athlete_clean["Year"] <= year_max)].copy()

    timeline = pd.DataFrame({"year": sorted(events_main["Year"].dropna().astype(int).unique())})
    timeline["season"] = season

    medal_events = events_main[events_main["is_medal"].eq(1)].copy()
    medal_unique = medal_events.drop_duplicates(
        subset=["Year", "Season", "Sport", "Event", "NOC", "country", "Medal"]
    ).copy()

    country_year = medal_unique.pivot_table(
        index=["Year", "Season", "NOC", "iso3", "country"],
        columns="Medal",
        values="Event",
        aggfunc="count",
        fill_value=0,
    ).reset_index()
    country_year = add_medal_columns(country_year)
    country_year["total_medals"] = country_year[["gold_medals", "silver_medals", "bronze_medals"]].sum(axis=1)
    country_year["weighted_medal_score"] = (
        country_year["gold_medals"] * 3 + country_year["silver_medals"] * 2 + country_year["bronze_medals"]
    )
    country_year["gold_share"] = safe_divide(country_year["gold_medals"], country_year["total_medals"])

    participation = events_main.groupby(["Year", "Season", "NOC", "country"], as_index=False).agg(
        athlete_event_entries=("Athlete_ID", "count"),
        unique_athletes=("Athlete_ID", pd.Series.nunique),
        sports_entered=("Sport", pd.Series.nunique),
        events_entered=("Event", pd.Series.nunique),
    )
    country_year = country_year.merge(participation, on=["Year", "Season", "NOC", "country"], how="left")
    country_year["rank_total_medals"] = country_year.groupby("Year")["total_medals"].rank(
        method="min", ascending=False
    )
    country_year["rank_gold_medals"] = country_year.groupby("Year")["gold_medals"].rank(method="min", ascending=False)
    country_year = country_year.sort_values(["country", "Year"])
    country_year["prev_total_medals"] = country_year.groupby("country")["total_medals"].shift(1)
    country_year["medal_growth_prev_games"] = country_year["total_medals"] - country_year["prev_total_medals"]
    country_year["medal_growth_rate_prev_games"] = safe_divide(
        country_year["medal_growth_prev_games"], country_year["prev_total_medals"]
    )
    country_year["rolling_3_games_medals"] = country_year.groupby("country")["total_medals"].transform(
        lambda s: s.rolling(3, min_periods=1).mean()
    )
    country_year["country_first_medal_year"] = country_year.groupby("country")["Year"].transform("min")
    country_year["is_country_first_medal_year"] = (
        country_year["Year"] == country_year["country_first_medal_year"]
    ).astype(int)

    country_sport_year = medal_unique.pivot_table(
        index=["Year", "Season", "NOC", "iso3", "country", "Sport"],
        columns="Medal",
        values="Event",
        aggfunc="count",
        fill_value=0,
    ).reset_index()
    country_sport_year = add_medal_columns(country_sport_year)
    country_sport_year["total_medals"] = country_sport_year[
        ["gold_medals", "silver_medals", "bronze_medals"]
    ].sum(axis=1)
    country_sport_year["weighted_medal_score"] = (
        country_sport_year["gold_medals"] * 3
        + country_sport_year["silver_medals"] * 2
        + country_sport_year["bronze_medals"]
    )
    country_totals = country_year[["Year", "country", "total_medals", "gold_medals"]].rename(
        columns={"total_medals": "country_year_total_medals", "gold_medals": "country_year_gold_medals"}
    )
    country_sport_year = country_sport_year.merge(country_totals, on=["Year", "country"], how="left")
    country_sport_year["sport_contribution_share"] = safe_divide(
        country_sport_year["total_medals"], country_sport_year["country_year_total_medals"]
    )
    country_sport_year["gold_share"] = safe_divide(
        country_sport_year["gold_medals"], country_sport_year["total_medals"]
    )
    country_sport_year["events_with_medals"] = country_sport_year["total_medals"]

    sport_year_total = medal_unique.groupby(["Year", "Season", "Sport"], as_index=False).agg(
        sport_year_total_medals=("Medal", "count"),
        sport_year_gold_medals=("Medal", lambda s: (s == "Gold").sum()),
        countries_winning_medals=("country", pd.Series.nunique),
    )
    sport_share = country_sport_year[
        ["Year", "Season", "Sport", "NOC", "iso3", "country", "gold_medals", "silver_medals", "bronze_medals", "total_medals"]
    ].copy()
    sport_share = sport_share.merge(sport_year_total, on=["Year", "Season", "Sport"], how="left")
    sport_share["country_medal_share_in_sport"] = safe_divide(
        sport_share["total_medals"], sport_share["sport_year_total_medals"]
    )
    sport_share["country_gold_share_in_sport"] = safe_divide(
        sport_share["gold_medals"], sport_share["sport_year_gold_medals"]
    )
    sport_share["sport_country_rank"] = sport_share.groupby(["Year", "Sport"])["total_medals"].rank(
        method="min", ascending=False
    )
    sport_hhi = sport_share.groupby(["Year", "Sport"], as_index=False).agg(
        sport_country_hhi=("total_medals", hhi_from_counts),
        sport_medal_entropy=("total_medals", entropy_from_counts),
    )
    sport_share = sport_share.merge(sport_hhi, on=["Year", "Sport"], how="left")

    athlete_demographics = build_athlete_demographics(athlete_clean)

    concentration = country_sport_year.groupby(["Year", "NOC", "country"], as_index=False).agg(
        country_sport_hhi=("total_medals", hhi_from_counts),
        medal_sport_entropy=("total_medals", entropy_from_counts),
        medal_sports=("Sport", "nunique"),
        top_sport_medal_share=("sport_contribution_share", "max"),
    )
    country_year = country_year.merge(
        concentration[["Year", "country", "country_sport_hhi", "medal_sport_entropy", "medal_sports"]],
        on=["Year", "country"],
        how="left",
    )
    country_year = country_year.merge(
        athlete_demographics[["year", "country_name", "female_athletes", "male_athletes", "female_athlete_share"]].rename(
            columns={"year": "Year", "country_name": "country"}
        ),
        on=["Year", "country"],
        how="left",
    )

    sport_globalization = sport_share.groupby(["Year", "Season", "Sport"], as_index=False).agg(
        countries_winning_medals=("country", pd.Series.nunique),
        sport_country_hhi=("total_medals", hhi_from_counts),
        sport_medal_entropy=("total_medals", entropy_from_counts),
        max_country_share=("country_medal_share_in_sport", "max"),
        sport_year_total_medals=("total_medals", "sum"),
    )
    sport_globalization["sport_globalization_index"] = 1 - sport_globalization["sport_country_hhi"]

    efficiency = build_efficiency(country_year, paths)
    economic_efficiency = build_country_economic_efficiency(efficiency)
    expected_vs_actual = build_expected_vs_actual_medals(economic_efficiency)
    country_multi_normalized_efficiency = build_country_multi_normalized_efficiency(country_year, economic_efficiency)
    host_years = build_host_years(events_main, country_year)
    host_effect_panel = build_host_effect_country_sport_panel(
        host_years,
        to_export_columns(country_sport_year),
        to_export_columns(sport_share),
    )
    host_effect_summary = build_host_effect_summary(
        host_years,
        to_export_columns(country_year),
        host_effect_panel,
    )
    host_effect_summary_modern = build_host_effect_summary_modern(host_effect_summary)
    event_added_removed = build_event_added_removed(events_main)
    event_opportunity_refined = build_event_opportunity_refined(event_added_removed)
    coach_events_template = build_coach_events_template()
    coach_events_template_examples = build_coach_events_template_examples()
    gender_event_expansion = build_gender_event_expansion_by_year(events_main, medal_unique)
    country_gender_capture = build_country_gender_opportunity_capture(medal_unique, gender_event_expansion, athlete_demographics)
    country_gender_structure = build_country_gender_medal_structure(country_gender_capture)
    country_gender_growth_decomposition = build_country_gender_growth_decomposition(
        country_gender_capture,
        athlete_demographics,
    )
    country_profile_features = build_country_profile_features(country_year, economic_efficiency, athlete_demographics)
    country_cluster_labels = build_country_cluster_labels(country_profile_features)
    sport_entry_barrier = build_sport_entry_barrier_index(
        sport_share,
        sport_globalization,
        gender_event_expansion,
        economic_efficiency,
    )
    sport_entry_barrier_modern = build_sport_entry_barrier_modern(
        sport_entry_barrier,
        sport_breakthrough_cases=pd.DataFrame(),
        sport_share=sport_share,
        gender_event_expansion=gender_event_expansion,
    )
    sport_breakthrough_cases = build_sport_breakthrough_cases(
        sport_share,
        sport_entry_barrier,
        economic_efficiency,
    )
    leap = build_leap_index(country_sport_year)
    leap_mechanism = build_leap_mechanism_decomposition(
        leap,
        host_years,
        event_opportunity_refined,
        gender_event_expansion,
        coach_events_template,
        expected_vs_actual,
    )
    leap_mechanism_enhanced = build_leap_mechanism_enhanced(
        leap_mechanism,
        expected_vs_actual,
        economic_efficiency,
    )
    leap_case_library = build_leap_case_library(leap_mechanism)
    mechanism_case_cards = build_mechanism_case_cards(
        leap_mechanism_enhanced,
        host_effect_panel,
        country_gender_growth_decomposition,
        sport_entry_barrier_modern,
        economic_efficiency,
    )
    mechanism_reporting_filter = build_mechanism_reporting_filter(
        leap_mechanism_enhanced,
        host_effect_panel,
        host_effect_summary,
        country_gender_growth_decomposition,
        sport_entry_barrier_modern,
        economic_efficiency,
    )

    tables = {
        "clean_olympic_events": to_export_columns(events_main),
        "clean_athlete_events_summer": to_export_columns(athlete_clean),
        "official_event_medals": to_export_columns(medal_unique),
        "medal_unique_event_country": to_export_columns(medal_unique),
        "olympic_timeline": timeline,
        "country_year_medals": select_export(
            to_export_columns(country_year),
            [
                "year",
                "season",
                "noc",
                "iso3",
                "country_name",
                "total_medals",
                "gold_medals",
                "silver_medals",
                "bronze_medals",
                "weighted_medal_score",
                "gold_share",
                "rank_total_medals",
                "rank_gold_medals",
                "athlete_event_entries",
                "unique_athletes",
                "sports_entered",
                "events_entered",
                "prev_total_medals",
                "medal_growth_prev_games",
                "medal_growth_rate_prev_games",
                "rolling_3_games_medals",
                "country_first_medal_year",
                "is_country_first_medal_year",
                "country_sport_hhi",
                "medal_sport_entropy",
                "medal_sports",
                "female_athletes",
                "male_athletes",
                "female_athlete_share",
            ],
        ),
        "country_sport_year_medals": select_export(
            to_export_columns(country_sport_year),
            [
                "year",
                "season",
                "noc",
                "iso3",
                "country_name",
                "sport",
                "total_medals",
                "gold_medals",
                "silver_medals",
                "bronze_medals",
                "weighted_medal_score",
                "events_with_medals",
                "gold_share",
                "country_year_total_medals",
                "sport_contribution_share",
            ],
        ),
        "sport_year_country_share": select_export(
            to_export_columns(sport_share),
            [
                "year",
                "season",
                "sport",
                "noc",
                "iso3",
                "country_name",
                "total_medals",
                "gold_medals",
                "silver_medals",
                "bronze_medals",
                "sport_year_total_medals",
                "sport_year_gold_medals",
                "country_medal_share_in_sport",
                "country_gold_share_in_sport",
                "sport_country_rank",
                "sport_country_hhi",
                "sport_medal_entropy",
            ],
        ),
        "athlete_demographics": athlete_demographics,
        "country_gdp_population_medal_efficiency": efficiency,
        "country_economic_efficiency": economic_efficiency,
        "country_expected_vs_actual_medals": expected_vs_actual,
        "country_multi_normalized_efficiency": country_multi_normalized_efficiency,
        "host_years": host_years,
        "host_effect_country_sport_panel": host_effect_panel,
        "host_effect_summary": host_effect_summary,
        "host_effect_summary_modern": host_effect_summary_modern,
        "event_added_removed": event_added_removed,
        "event_opportunity_refined": event_opportunity_refined,
        "coach_events_template": coach_events_template,
        "coach_events_template_examples": coach_events_template_examples,
        "gender_event_expansion_by_year": gender_event_expansion,
        "country_gender_opportunity_capture": country_gender_capture,
        "country_gender_medal_structure": country_gender_structure,
        "country_gender_growth_decomposition": country_gender_growth_decomposition,
        "country_profile_features": country_profile_features,
        "country_cluster_labels": country_cluster_labels,
        "sport_entry_barrier_index": sport_entry_barrier,
        "sport_entry_barrier_modern": sport_entry_barrier_modern,
        "sport_breakthrough_cases": sport_breakthrough_cases,
        "leap_mechanism_decomposition": leap_mechanism,
        "leap_mechanism_enhanced": leap_mechanism_enhanced,
        "leap_case_library": leap_case_library,
        "mechanism_case_cards": mechanism_case_cards,
        "mechanism_reporting_filter": mechanism_reporting_filter,
        "country_year_sport_concentration": to_export_columns(concentration),
        "sport_globalization_metrics": to_export_columns(sport_globalization),
        "leap_index_country_sport": leap,
    }

    tables["data_quality_report"] = build_quality_report(tables)
    tables["table_manifest"] = build_table_manifest(tables, config)
    tables["data_dictionary"] = build_data_dictionary(tables)
    write_tables(tables, config, output_dir)
    write_metadata_run_log(tables, config, output_dir, paths)
    return tables


def add_medal_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"Gold": "gold_medals", "Silver": "silver_medals", "Bronze": "bronze_medals"})
    for column in ["gold_medals", "silver_medals", "bronze_medals"]:
        if column not in df.columns:
            df[column] = 0
    return df


def select_export(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = np.nan
    return df[columns].copy()


def build_athlete_demographics(athlete_clean: pd.DataFrame) -> pd.DataFrame:
    base = athlete_clean.groupby(["Year", "Season", "NOC", "iso3", "country"], as_index=False).agg(
        athlete_event_entries=("Athlete_ID", "count"),
        athletes=("Athlete_ID", pd.Series.nunique),
        avg_age=("Age", "mean"),
        median_age=("Age", "median"),
        avg_height=("Height", "mean"),
        avg_weight=("Weight", "mean"),
        sports_participated=("Sport", pd.Series.nunique),
        events_participated=("Event", pd.Series.nunique),
    )
    gender = athlete_clean.drop_duplicates(["Year", "NOC", "Athlete_ID"]).pivot_table(
        index=["Year", "NOC"],
        columns="Sex",
        values="Athlete_ID",
        aggfunc=pd.Series.nunique,
        fill_value=0,
    ).reset_index()
    if "F" not in gender.columns:
        gender["F"] = 0
    if "M" not in gender.columns:
        gender["M"] = 0
    gender = gender.rename(columns={"F": "female_athletes", "M": "male_athletes"})
    out = base.merge(gender[["Year", "NOC", "female_athletes", "male_athletes"]], on=["Year", "NOC"], how="left")
    out["female_athlete_share"] = safe_divide(out["female_athletes"], out["female_athletes"] + out["male_athletes"])
    out["female_share"] = out["female_athlete_share"]
    return select_export(
        to_export_columns(out),
        [
            "year",
            "season",
            "noc",
            "iso3",
            "country_name",
            "athletes",
            "female_athletes",
            "male_athletes",
            "female_athlete_share",
            "female_share",
            "avg_age",
            "median_age",
            "avg_height",
            "avg_weight",
            "athlete_event_entries",
            "sports_participated",
            "events_participated",
        ],
    )


def build_efficiency(country_year: pd.DataFrame, paths: dict[str, Path]) -> pd.DataFrame:
    eff = to_export_columns(country_year).copy()
    eff["population"] = np.nan
    eff["gdp_total"] = np.nan
    eff["gdp_per_capita"] = np.nan

    if paths["gdp_per_capita"].exists():
        gdp_pc = pd.read_csv(paths["gdp_per_capita"]).rename(
            columns={"Entity": "gdp_entity", "Code": "iso3", "Year": "year", "GDP per capita": "gdp_per_capita"}
        )
        gdp_pc["iso3"] = gdp_pc["iso3"].astype(str).str.upper().str.strip()
        gdp_pc["year"] = pd.to_numeric(gdp_pc["year"], errors="coerce")
        gdp_pc["gdp_per_capita"] = pd.to_numeric(gdp_pc["gdp_per_capita"], errors="coerce")
        eff = eff.drop(columns=["gdp_per_capita"], errors="ignore").merge(
            gdp_pc[["iso3", "year", "gdp_per_capita"]],
            on=["iso3", "year"],
            how="left",
        )

    if paths["population"].exists():
        pop = pd.read_csv(paths["population"]).rename(
            columns={"Entity": "pop_entity", "Country": "pop_entity", "Code": "iso3", "Year": "year", "Population": "population"}
        )
        if {"iso3", "year", "population"}.issubset(pop.columns):
            pop["iso3"] = pop["iso3"].astype(str).str.upper().str.strip()
            pop["year"] = pd.to_numeric(pop["year"], errors="coerce")
            pop["population"] = pd.to_numeric(pop["population"], errors="coerce")
            eff = eff.drop(columns=["population"], errors="ignore").merge(
                pop[["iso3", "year", "population"]],
                on=["iso3", "year"],
                how="left",
            )

    if paths["gdp_total"].exists():
        gdp_total = pd.read_csv(paths["gdp_total"]).rename(columns={"Entity": "gdp_entity", "Code": "iso3", "Year": "year", "GDP": "gdp_total"})
        if {"iso3", "year", "gdp_total"}.issubset(gdp_total.columns):
            gdp_total["iso3"] = gdp_total["iso3"].astype(str).str.upper().str.strip()
            gdp_total["year"] = pd.to_numeric(gdp_total["year"], errors="coerce")
            gdp_total["gdp_total"] = pd.to_numeric(gdp_total["gdp_total"], errors="coerce")
            eff = eff.drop(columns=["gdp_total"], errors="ignore").merge(
                gdp_total[["iso3", "year", "gdp_total"]],
                on=["iso3", "year"],
                how="left",
            )

    if "gdp_total" not in eff.columns or eff["gdp_total"].isna().all():
        eff["gdp_total"] = eff["gdp_per_capita"] * eff["population"]

    eff["medals_per_million_people"] = safe_divide(eff["total_medals"], eff["population"]) * 1_000_000
    eff["gold_per_million_people"] = safe_divide(eff["gold_medals"], eff["population"]) * 1_000_000
    eff["medals_per_10b_gdp"] = safe_divide(eff["total_medals"], eff["gdp_total"]) * 10_000_000_000
    eff["gold_per_10b_gdp"] = safe_divide(eff["gold_medals"], eff["gdp_total"]) * 10_000_000_000
    eff["medals_per_100b_gdp"] = safe_divide(eff["total_medals"], eff["gdp_total"]) * 100_000_000_000
    eff["gold_per_100b_gdp"] = safe_divide(eff["gold_medals"], eff["gdp_total"]) * 100_000_000_000
    eff["medals_per_10k_gdp_per_capita"] = safe_divide(eff["total_medals"], eff["gdp_per_capita"]) * 10_000
    eff["gold_per_10k_gdp_per_capita"] = safe_divide(eff["gold_medals"], eff["gdp_per_capita"]) * 10_000

    return select_export(
        eff,
        [
            "year",
            "season",
            "noc",
            "iso3",
            "country_name",
            "total_medals",
            "gold_medals",
            "silver_medals",
            "bronze_medals",
            "rank_total_medals",
            "rank_gold_medals",
            "population",
            "gdp_total",
            "gdp_per_capita",
            "medals_per_million_people",
            "gold_per_million_people",
            "medals_per_10b_gdp",
            "gold_per_10b_gdp",
            "medals_per_100b_gdp",
            "gold_per_100b_gdp",
            "medals_per_10k_gdp_per_capita",
            "gold_per_10k_gdp_per_capita",
            "sports_entered",
            "events_entered",
            "unique_athletes",
        ],
    )


def build_country_economic_efficiency(efficiency: pd.DataFrame) -> pd.DataFrame:
    """Create the P1A standard economic efficiency table.

    This table keeps GDP per capita, population and total GDP side by side.
    The 100B GDP ratios are easier to read than the legacy 10B GDP ratios.
    """
    out = efficiency.copy()
    if "medals_per_100b_gdp" not in out.columns:
        out["medals_per_100b_gdp"] = safe_divide(out["total_medals"], out["gdp_total"]) * 100_000_000_000
    if "gold_per_100b_gdp" not in out.columns:
        out["gold_per_100b_gdp"] = safe_divide(out["gold_medals"], out["gdp_total"]) * 100_000_000_000
    out["has_population_match"] = out["population"].notna().astype(int)
    out["has_gdp_per_capita_match"] = out["gdp_per_capita"].notna().astype(int)
    out["has_gdp_total_match"] = out["gdp_total"].notna().astype(int)
    out["economic_data_complete"] = (
        out["has_population_match"].eq(1)
        & out["has_gdp_per_capita_match"].eq(1)
        & out["has_gdp_total_match"].eq(1)
    ).astype(int)
    return select_export(
        out,
        [
            "year",
            "season",
            "noc",
            "iso3",
            "country_name",
            "total_medals",
            "gold_medals",
            "silver_medals",
            "bronze_medals",
            "population",
            "gdp_per_capita",
            "gdp_total",
            "medals_per_million_people",
            "gold_per_million_people",
            "medals_per_10k_gdp_per_capita",
            "gold_per_10k_gdp_per_capita",
            "medals_per_100b_gdp",
            "gold_per_100b_gdp",
            "sports_entered",
            "events_entered",
            "unique_athletes",
            "has_population_match",
            "has_gdp_per_capita_match",
            "has_gdp_total_match",
            "economic_data_complete",
        ],
    )


def build_expected_vs_actual_medals(economic_efficiency: pd.DataFrame) -> pd.DataFrame:
    """Build a descriptive expected-vs-actual benchmark.

    Expected medals are fitted within each Olympic year using available
    economic base variables. This is a descriptive baseline, not a causal
    model and not a forecast.
    """
    df = economic_efficiency.copy()
    for column in ["total_medals", "gdp_total", "population", "sports_entered"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["log_gdp_total"] = np.log1p(df["gdp_total"])
    df["log_population"] = np.log1p(df["population"])
    df["expected_medals"] = np.nan
    df["model_note"] = "insufficient matched economic data"

    feature_sets = [
        (["log_gdp_total", "log_population", "sports_entered"], "year OLS: log_gdp_total + log_population + sports_entered"),
        (["log_gdp_total", "log_population"], "year OLS: log_gdp_total + log_population"),
        (["log_gdp_total"], "year OLS: log_gdp_total"),
    ]

    for year, idx in df.groupby("year").groups.items():
        year_idx = list(idx)
        year_rows = df.loc[year_idx]
        fitted = False
        for features, note in feature_sets:
            fit_rows = year_rows.dropna(subset=["total_medals", *features])
            if len(fit_rows) < len(features) + 2:
                continue
            x = fit_rows[features].astype(float).to_numpy()
            y = fit_rows["total_medals"].astype(float).to_numpy()
            design = np.column_stack([np.ones(len(x)), x])
            try:
                beta, *_ = np.linalg.lstsq(design, y, rcond=None)
            except np.linalg.LinAlgError:
                continue
            predict_rows = year_rows.dropna(subset=features)
            if predict_rows.empty:
                continue
            px = predict_rows[features].astype(float).to_numpy()
            pdesign = np.column_stack([np.ones(len(px)), px])
            pred = np.maximum(0, pdesign @ beta)
            df.loc[predict_rows.index, "expected_medals"] = pred
            df.loc[predict_rows.index, "model_note"] = note
            fitted = True
            break
        missing_expected = df.loc[year_idx, "expected_medals"].isna()
        if missing_expected.any():
            year_total = year_rows["total_medals"].sum()
            countries = year_rows["country_name"].nunique()
            if countries > 0:
                fallback_idx = df.loc[year_idx].index[missing_expected]
                df.loc[fallback_idx, "expected_medals"] = year_total / countries
                fallback_note = (
                    "year mean fallback: missing economic predictors"
                    if fitted
                    else "year mean baseline: insufficient economic matches"
                )
                df.loc[fallback_idx, "model_note"] = fallback_note

    df["medal_overperformance"] = df["total_medals"] - df["expected_medals"]
    return select_export(
        df,
        [
            "year",
            "noc",
            "iso3",
            "country_name",
            "total_medals",
            "expected_medals",
            "medal_overperformance",
            "gold_medals",
            "gdp_total",
            "population",
            "gdp_per_capita",
            "sports_entered",
            "events_entered",
            "model_note",
        ],
    )


def build_country_multi_normalized_efficiency(
    country_year: pd.DataFrame,
    economic_efficiency: pd.DataFrame,
) -> pd.DataFrame:
    """Build country-year metrics comparing scale and multiple efficiency views."""
    cy = to_export_columns(country_year).copy()
    economic = economic_efficiency.copy()

    keep_from_economic = [
        "year",
        "noc",
        "country_name",
        "population",
        "gdp_total",
        "gdp_per_capita",
        "medals_per_million_people",
        "gold_per_million_people",
        "medals_per_100b_gdp",
        "gold_per_100b_gdp",
        "has_population_match",
        "has_gdp_total_match",
        "economic_data_complete",
    ]
    out = cy.merge(
        economic[[column for column in keep_from_economic if column in economic.columns]],
        on=["year", "noc", "country_name"],
        how="left",
        suffixes=("", "_economic"),
    )

    for column in [
        "total_medals",
        "gold_medals",
        "weighted_medal_score",
        "unique_athletes",
        "sports_entered",
        "events_entered",
        "population",
        "gdp_total",
        "gdp_per_capita",
    ]:
        out[column] = pd.to_numeric(out[column], errors="coerce")

    if "medals_per_million_people" not in out.columns or out["medals_per_million_people"].isna().all():
        out["medals_per_million_people"] = safe_divide(out["total_medals"], out["population"]) * 1_000_000
    if "gold_per_million_people" not in out.columns or out["gold_per_million_people"].isna().all():
        out["gold_per_million_people"] = safe_divide(out["gold_medals"], out["population"]) * 1_000_000
    if "medals_per_100b_gdp" not in out.columns:
        out["medals_per_100b_gdp"] = safe_divide(out["total_medals"], out["gdp_total"]) * 100_000_000_000
    if "gold_per_100b_gdp" not in out.columns:
        out["gold_per_100b_gdp"] = safe_divide(out["gold_medals"], out["gdp_total"]) * 100_000_000_000

    out["medals_per_100_athletes"] = safe_divide(out["total_medals"], out["unique_athletes"]) * 100
    out["gold_per_100_athletes"] = safe_divide(out["gold_medals"], out["unique_athletes"]) * 100
    out["medals_per_entered_sport"] = safe_divide(out["total_medals"], out["sports_entered"])
    out["medals_per_entered_event"] = safe_divide(out["total_medals"], out["events_entered"])
    out["weighted_score_per_athlete"] = safe_divide(out["weighted_medal_score"], out["unique_athletes"])
    out["weighted_score_per_entered_sport"] = safe_divide(out["weighted_medal_score"], out["sports_entered"])

    out["small_sample_flag"] = (
        (out["unique_athletes"].fillna(0) < 10)
        | (out["sports_entered"].fillna(0) < 3)
        | (out["events_entered"].fillna(0) < 5)
    ).astype(int)
    out["extreme_single_medal_flag"] = (
        out["total_medals"].eq(1)
        & (
            (pd.to_numeric(out["medals_per_million_people"], errors="coerce") >= 1)
            | (pd.to_numeric(out["medals_per_100_athletes"], errors="coerce") >= 20)
        )
    ).astype(int)
    out["efficiency_interpretation_note"] = np.where(
        out["small_sample_flag"].eq(1),
        "Efficiency metrics may be inflated by a small delegation or few entered events.",
        "Efficiency metrics are descriptive ratios for comparing scale and normalization views.",
    )

    for metric in [
        "total_medals",
        "medals_per_million_people",
        "medals_per_100_athletes",
        "medals_per_entered_sport",
        "weighted_score_per_athlete",
    ]:
        rank_column = f"rank_{metric}"
        out[rank_column] = out.groupby("year")[metric].rank(method="min", ascending=False)

    def label_efficiency(row: pd.Series) -> str:
        total_rank = pd.to_numeric(row.get("rank_total_medals"), errors="coerce")
        athlete_rank = pd.to_numeric(row.get("rank_medals_per_100_athletes"), errors="coerce")
        pop_rank = pd.to_numeric(row.get("rank_medals_per_million_people"), errors="coerce")
        small_sample = pd.to_numeric(row.get("small_sample_flag"), errors="coerce") == 1
        if total_rank <= 10 and athlete_rank > 30 and pop_rank > 30:
            return "scale_power_efficiency_moderate"
        if total_rank > 20 and (athlete_rank <= 10 or pop_rank <= 10) and not small_sample:
            return "small_scale_high_efficiency"
        if total_rank <= 10 and (athlete_rank <= 20 or pop_rank <= 20):
            return "scale_and_efficiency_power"
        if small_sample:
            return "small_sample_ratio_sensitive"
        return "balanced_or_midfield"

    out["efficiency_label"] = out.apply(label_efficiency, axis=1)
    return select_export(
        out,
        [
            "year",
            "season",
            "noc",
            "iso3",
            "country_name",
            "total_medals",
            "gold_medals",
            "silver_medals",
            "bronze_medals",
            "weighted_medal_score",
            "population",
            "gdp_total",
            "gdp_per_capita",
            "unique_athletes",
            "sports_entered",
            "events_entered",
            "medals_per_million_people",
            "gold_per_million_people",
            "medals_per_100_athletes",
            "gold_per_100_athletes",
            "medals_per_entered_sport",
            "medals_per_entered_event",
            "weighted_score_per_athlete",
            "weighted_score_per_entered_sport",
            "medals_per_100b_gdp",
            "gold_per_100b_gdp",
            "rank_total_medals",
            "rank_medals_per_million_people",
            "rank_medals_per_100_athletes",
            "rank_medals_per_entered_sport",
            "rank_weighted_score_per_athlete",
            "small_sample_flag",
            "extreme_single_medal_flag",
            "efficiency_label",
            "efficiency_interpretation_note",
            "has_population_match",
            "has_gdp_total_match",
            "economic_data_complete",
        ],
    )


def build_host_years(events_main: pd.DataFrame, country_year: pd.DataFrame) -> pd.DataFrame:
    """Build Summer Olympic host-year rows.

    City and year are extracted from the Olympic event data. Host country,
    host NOC and continent require a curated city-year mapping because the
    event records do not store host country directly.
    """
    city_year = events_main.groupby(["Year", "City"], as_index=False).size().sort_values(["Year", "size"], ascending=[True, False])
    primary_city = city_year.drop_duplicates("Year").copy()
    host_medal_keys = set(zip(country_year["Year"].astype(int), country_year["NOC"].astype(str)))

    rows = []
    for _, row in primary_city.iterrows():
        year = int(row["Year"])
        city = str(row["City"])
        mapped = HOST_CITY_MAP.get(year, {})
        host_country = mapped.get("host_country")
        host_noc = mapped.get("host_noc")
        host_noc_compatible = mapped.get("host_noc_compatible", host_noc)
        rows.append(
            {
                "year": year,
                "city": city,
                "host_country": host_country,
                "host_noc": host_noc,
                "host_noc_compatible": host_noc_compatible,
                "host_continent": mapped.get("host_continent"),
                "is_home_games": int((year, str(host_noc_compatible)) in host_medal_keys) if host_noc_compatible else np.nan,
                "source_note": "city extracted from olympics_dataset; host country/noc/continent from curated HOST_CITY_MAP",
                "needs_manual_review": int(not host_country or city != mapped.get("city", city)),
            }
        )
    return pd.DataFrame(rows)


def build_host_effect_country_sport_panel(
    host_years: pd.DataFrame,
    country_sport_year: pd.DataFrame,
    sport_year_country_share: pd.DataFrame,
) -> pd.DataFrame:
    """Compare host-country sport results before, during and after hosting.

    The output is candidate evidence for a host-country effect. It is not a
    causal estimate because host assignment, delegation size, event program
    changes and competitive fields are not randomly assigned.
    """
    hosts = host_years.copy()
    csy = country_sport_year.copy()
    share = sport_year_country_share.copy()

    hosts["year"] = pd.to_numeric(hosts["year"], errors="coerce")
    csy["year"] = pd.to_numeric(csy["year"], errors="coerce")
    share["year"] = pd.to_numeric(share["year"], errors="coerce")
    for column in ["total_medals", "gold_medals"]:
        if column in csy.columns:
            csy[column] = pd.to_numeric(csy[column], errors="coerce").fillna(0)
        if column in share.columns:
            share[column] = pd.to_numeric(share[column], errors="coerce").fillna(0)
    share["country_medal_share_in_sport"] = pd.to_numeric(
        share["country_medal_share_in_sport"],
        errors="coerce",
    )

    olympic_years = sorted(csy["year"].dropna().astype(int).unique())
    if not olympic_years:
        return pd.DataFrame()
    previous_year = {year: olympic_years[i - 1] if i > 0 else np.nan for i, year in enumerate(olympic_years)}
    next_year = {year: olympic_years[i + 1] if i + 1 < len(olympic_years) else np.nan for i, year in enumerate(olympic_years)}

    medal_lookup = csy.set_index(["year", "noc", "sport"])["total_medals"].to_dict()
    share_lookup = share.set_index(["year", "noc", "sport"])["country_medal_share_in_sport"].to_dict()
    host_sports = csy.groupby(["year", "noc"])["sport"].apply(lambda s: sorted(set(s.dropna()))).to_dict()
    all_sports_by_year = csy.groupby("year")["sport"].apply(lambda s: sorted(set(s.dropna()))).to_dict()

    rows = []
    for _, host in hosts.dropna(subset=["year"]).iterrows():
        host_year = int(host["year"])
        host_noc = str(host.get("host_noc_compatible") or host.get("host_noc") or "")
        if not host_noc:
            continue
        pre_year = previous_year.get(host_year, np.nan)
        post_year = next_year.get(host_year, np.nan)
        sports = set(all_sports_by_year.get(host_year, []))
        sports.update(host_sports.get((host_year, host_noc), []))
        if not pd.isna(pre_year):
            sports.update(host_sports.get((int(pre_year), host_noc), []))
        if not pd.isna(post_year):
            sports.update(host_sports.get((int(post_year), host_noc), []))

        for sport in sorted(sports):
            pre_medals = medal_lookup.get((pre_year, host_noc, sport), 0.0) if not pd.isna(pre_year) else np.nan
            host_medals = medal_lookup.get((host_year, host_noc, sport), 0.0)
            post_medals = medal_lookup.get((post_year, host_noc, sport), 0.0) if not pd.isna(post_year) else np.nan
            pre_share = share_lookup.get((pre_year, host_noc, sport), 0.0) if not pd.isna(pre_year) else np.nan
            host_share = share_lookup.get((host_year, host_noc, sport), 0.0)
            post_share = share_lookup.get((post_year, host_noc, sport), 0.0) if not pd.isna(post_year) else np.nan

            baseline_values = [value for value in [pre_medals, post_medals] if not pd.isna(value)]
            baseline = float(np.mean(baseline_values)) if baseline_values else np.nan
            host_lift_vs_pre = host_medals - pre_medals if not pd.isna(pre_medals) else np.nan
            host_lift_vs_post = host_medals - post_medals if not pd.isna(post_medals) else np.nan
            host_lift_vs_baseline = host_medals - baseline if not pd.isna(baseline) else np.nan

            peers = share[
                share["year"].eq(host_year)
                & share["sport"].eq(sport)
                & ~share["noc"].eq(host_noc)
            ][["noc", "total_medals"]].copy()
            if not peers.empty and not pd.isna(pre_year):
                peer_pre = csy[
                    csy["year"].eq(pre_year)
                    & csy["sport"].eq(sport)
                    & csy["noc"].isin(peers["noc"])
                ][["noc", "total_medals"]].rename(columns={"total_medals": "pre_total_medals"})
                peers = peers.merge(peer_pre, on="noc", how="left")
                peers["pre_total_medals"] = pd.to_numeric(peers["pre_total_medals"], errors="coerce").fillna(0)
                peers["peer_change"] = peers["total_medals"] - peers["pre_total_medals"]
                non_host_peer_change = peers["peer_change"].mean()
            else:
                non_host_peer_change = np.nan
            host_lift_vs_peers = host_lift_vs_pre - non_host_peer_change if not pd.isna(host_lift_vs_pre) and not pd.isna(non_host_peer_change) else np.nan

            components = [
                host_lift_vs_pre if not pd.isna(host_lift_vs_pre) else 0,
                host_lift_vs_baseline if not pd.isna(host_lift_vs_baseline) else 0,
                host_lift_vs_peers if not pd.isna(host_lift_vs_peers) else 0,
            ]
            positive_components = sum(value > 0 for value in components)
            if positive_components >= 2 and (not pd.isna(host_lift_vs_baseline) and host_lift_vs_baseline >= 2):
                strength = "strong_candidate"
            elif positive_components >= 2 and (not pd.isna(host_lift_vs_baseline) and host_lift_vs_baseline > 0):
                strength = "moderate_candidate"
            elif positive_components >= 1 and host_medals > 0:
                strength = "weak_candidate"
            elif pd.isna(pre_year) or pd.isna(post_year):
                strength = "insufficient_window"
            else:
                strength = "no_positive_host_lift"

            missing_windows = []
            if pd.isna(pre_year):
                missing_windows.append("pre Games unavailable")
            if pd.isna(post_year):
                missing_windows.append("post Games unavailable")
            window_note = f" Window limit: {', '.join(missing_windows)}." if missing_windows else ""
            interpretation = (
                f"Host-effect candidate evidence for {host.get('host_country')} {host_year} {sport}: "
                f"host medals {host_medals:g} versus pre {'' if pd.isna(pre_medals) else f'{pre_medals:g}'} "
                f"and post {'' if pd.isna(post_medals) else f'{post_medals:g}'}. "
                f"Lift versus baseline is {'' if pd.isna(host_lift_vs_baseline) else f'{host_lift_vs_baseline:g}'}; "
                f"peer-adjusted lift is {'' if pd.isna(host_lift_vs_peers) else f'{host_lift_vs_peers:g}'}. "
                f"Read this as internal comparative evidence, not a causal claim.{window_note}"
            )

            rows.append(
                {
                    "host_year": host_year,
                    "host_country": host.get("host_country"),
                    "host_noc": host_noc,
                    "sport": sport,
                    "pre_year": int(pre_year) if not pd.isna(pre_year) else np.nan,
                    "post_year": int(post_year) if not pd.isna(post_year) else np.nan,
                    "pre_host_medals": pre_medals,
                    "host_year_medals": host_medals,
                    "post_host_medals": post_medals,
                    "pre_country_share_in_sport": pre_share,
                    "host_country_share_in_sport": host_share,
                    "post_country_share_in_sport": post_share,
                    "host_lift_vs_pre": host_lift_vs_pre,
                    "host_lift_vs_post": host_lift_vs_post,
                    "host_lift_vs_baseline": host_lift_vs_baseline,
                    "non_host_peer_change": non_host_peer_change,
                    "host_lift_vs_peers": host_lift_vs_peers,
                    "host_effect_strength": strength,
                    "host_effect_interpretation": interpretation,
                }
            )
    return pd.DataFrame(rows)


def build_host_effect_summary(
    host_years: pd.DataFrame,
    country_year: pd.DataFrame,
    host_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize host-country total medal lift and strongest sport candidates."""
    hosts = host_years.copy()
    cy = country_year.copy()
    panel = host_panel.copy()
    hosts["year"] = pd.to_numeric(hosts["year"], errors="coerce")
    cy["year"] = pd.to_numeric(cy["year"], errors="coerce")
    cy["total_medals"] = pd.to_numeric(cy["total_medals"], errors="coerce").fillna(0)
    olympic_years = sorted(cy["year"].dropna().astype(int).unique())
    previous_year = {year: olympic_years[i - 1] if i > 0 else np.nan for i, year in enumerate(olympic_years)}
    next_year = {year: olympic_years[i + 1] if i + 1 < len(olympic_years) else np.nan for i, year in enumerate(olympic_years)}
    total_lookup = cy.set_index(["year", "noc"])["total_medals"].to_dict()

    rows = []
    for _, host in hosts.dropna(subset=["year"]).iterrows():
        host_year = int(host["year"])
        host_noc = str(host.get("host_noc_compatible") or host.get("host_noc") or "")
        pre_year = previous_year.get(host_year, np.nan)
        post_year = next_year.get(host_year, np.nan)
        pre_total = total_lookup.get((pre_year, host_noc), np.nan) if not pd.isna(pre_year) else np.nan
        host_total = total_lookup.get((host_year, host_noc), np.nan)
        post_total = total_lookup.get((post_year, host_noc), np.nan) if not pd.isna(post_year) else np.nan
        total_baseline_values = [value for value in [pre_total, post_total] if not pd.isna(value)]
        total_baseline = float(np.mean(total_baseline_values)) if total_baseline_values else np.nan
        lift_vs_pre = host_total - pre_total if not pd.isna(host_total) and not pd.isna(pre_total) else np.nan
        lift_vs_baseline = host_total - total_baseline if not pd.isna(host_total) and not pd.isna(total_baseline) else np.nan

        host_sports = panel[panel["host_year"].eq(host_year)].copy()
        host_sports["host_lift_vs_baseline"] = pd.to_numeric(host_sports["host_lift_vs_baseline"], errors="coerce")
        positive_sports = int((host_sports["host_lift_vs_baseline"] > 0).sum()) if not host_sports.empty else 0
        strongest = ""
        if not host_sports.empty and host_sports["host_lift_vs_baseline"].notna().any():
            strongest = str(host_sports.sort_values("host_lift_vs_baseline", ascending=False).iloc[0]["sport"])

        strong_count = int(host_sports["host_effect_strength"].eq("strong_candidate").sum()) if not host_sports.empty else 0
        moderate_count = int(host_sports["host_effect_strength"].eq("moderate_candidate").sum()) if not host_sports.empty else 0
        if pd.isna(pre_year) or pd.isna(post_year):
            confidence = "limited_missing_pre_or_post_games"
        elif strong_count >= 2 and not pd.isna(lift_vs_baseline) and lift_vs_baseline > 0:
            confidence = "medium_high_candidate"
        elif strong_count >= 1 or (moderate_count >= 2 and not pd.isna(lift_vs_baseline) and lift_vs_baseline > 0):
            confidence = "medium_candidate"
        elif not pd.isna(lift_vs_baseline) and lift_vs_baseline > 0:
            confidence = "weak_candidate"
        else:
            confidence = "no_positive_total_lift"

        rows.append(
            {
                "host_year": host_year,
                "host_country": host.get("host_country"),
                "total_medals_pre": pre_total,
                "total_medals_host": host_total,
                "total_medals_post": post_total,
                "host_total_lift_vs_pre": lift_vs_pre,
                "host_total_lift_vs_baseline": lift_vs_baseline,
                "sports_with_positive_host_lift": positive_sports,
                "strongest_host_effect_sport": strongest,
                "host_effect_confidence": confidence,
            }
        )
    return pd.DataFrame(rows)


def build_host_effect_summary_modern(host_summary: pd.DataFrame, modern_start: int = 1992, modern_end: int = 2024) -> pd.DataFrame:
    """Create a presentation-safe modern host-effect summary.

    The modern view keeps complete pre/host/post windows and avoids early Games
    where event systems and host participation rules are less comparable.
    """
    out = host_summary.copy()
    for column in [
        "host_year",
        "total_medals_pre",
        "total_medals_host",
        "total_medals_post",
        "host_total_lift_vs_baseline",
    ]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")

    out = out[
        out["host_year"].between(modern_start, modern_end)
        & out["total_medals_pre"].notna()
        & out["total_medals_post"].notna()
        & ~out["host_effect_confidence"].astype(str).str.contains("limited|missing", case=False, na=False)
    ].copy()

    def reporting_note(row: pd.Series) -> str:
        year = int(row["host_year"]) if pd.notna(row.get("host_year")) else "NA"
        country = row.get("host_country", "the host")
        lift = row.get("host_total_lift_vs_baseline")
        sport = row.get("strongest_host_effect_sport") or "the strongest sport"
        if pd.notna(lift) and lift > 0:
            return (
                f"Use as a modern host-year candidate case: {country} {year} exceeded its pre/post total-medal baseline by {lift:g}; "
                f"{sport} is the strongest sport-level lift. Present as internal comparative evidence, not a causal claim."
            )
        return (
            f"Use as a modern comparison case with limited or no positive total lift: {country} {year}. "
            "Do not present as evidence of a strong host effect."
        )

    out["reporting_note"] = out.apply(reporting_note, axis=1)
    return select_export(
        out.sort_values(["host_year", "host_country"]),
        [
            "host_year",
            "host_country",
            "total_medals_pre",
            "total_medals_host",
            "total_medals_post",
            "host_total_lift_vs_baseline",
            "strongest_host_effect_sport",
            "host_effect_confidence",
            "reporting_note",
        ],
    )


def infer_event_sex(event: Any, sex: Any = None) -> str:
    text = str(event or "").lower()
    sex_text = str(sex or "").upper()
    if "mixed" in text:
        return "mixed"
    if "open" in text:
        return "open_unknown"
    if "women" in text or sex_text == "F":
        return "women"
    if "men" in text or sex_text == "M":
        return "men"
    return "open_unknown"


def clean_event_family(sport: Any, event: Any) -> str:
    sport_text = clean_text(sport)
    event_text = clean_text(event)
    if pd.isna(event_text):
        return ""
    family = str(event_text)
    if not pd.isna(sport_text):
        family = re.sub(rf"^{re.escape(str(sport_text))}\s+", "", family, flags=re.IGNORECASE)
    family = re.sub(r"\b(Men's|Women's|Mixed|Open)\b", "", family, flags=re.IGNORECASE)
    family = re.sub(r"\s+", " ", family).strip(" -")
    return family or str(event_text)


def build_event_added_removed(events_main: pd.DataFrame) -> pd.DataFrame:
    """Detect event-program changes by comparing adjacent Summer Games.

    added: event appears in current Games but not previous Games.
    removed: event appears in previous Games but not current Games.
    restored: event appears in current Games, not previous Games, but existed before.
    """
    event_rows = events_main[["Year", "Sport", "Event", "Sex"]].dropna(subset=["Year", "Sport", "Event"]).copy()
    event_rows["Year"] = event_rows["Year"].astype(int)
    event_rows["sport_clean"] = event_rows["Sport"].apply(clean_text)
    event_rows["event_clean"] = event_rows["Event"].apply(clean_text)
    event_rows["sex"] = event_rows.apply(lambda row: infer_event_sex(row["event_clean"], row["Sex"]), axis=1)
    event_rows["event_family"] = event_rows.apply(lambda row: clean_event_family(row["sport_clean"], row["event_clean"]), axis=1)
    event_rows["event_key"] = (
        event_rows["sport_clean"].astype(str)
        + "||"
        + event_rows["event_clean"].astype(str)
        + "||"
        + event_rows["sex"].astype(str)
    )
    unique_events = event_rows.drop_duplicates(["Year", "event_key"]).copy()
    years = sorted(unique_events["Year"].unique())
    events_by_year = {
        year: set(unique_events.loc[unique_events["Year"].eq(year), "event_key"])
        for year in years
    }
    lookup = (
        unique_events.sort_values("Year")
        .drop_duplicates("event_key")
        .set_index("event_key")[["sport_clean", "event_clean", "sex", "event_family"]]
        .to_dict("index")
    )

    rows = []
    seen_before: set[str] = set()
    for i, year in enumerate(years):
        current = events_by_year[year]
        previous = events_by_year[years[i - 1]] if i > 0 else set()
        if i > 0:
            for key in sorted(current - previous):
                meta = lookup[key]
                rows.append(
                    {
                        "year": int(year),
                        "sport": meta["sport_clean"],
                        "event": meta["event_clean"],
                        "sex": meta["sex"],
                        "change_type": "restored" if key in seen_before else "added",
                        "event_family": meta["event_family"],
                        "note": "Detected from adjacent Summer Games event-set comparison.",
                    }
                )
            for key in sorted(previous - current):
                meta = lookup[key]
                rows.append(
                    {
                        "year": int(year),
                        "sport": meta["sport_clean"],
                        "event": meta["event_clean"],
                        "sex": meta["sex"],
                        "change_type": "removed",
                        "event_family": meta["event_family"],
                        "note": f"Present in {years[i - 1]} but absent in {year}.",
                    }
                )
        seen_before |= current
    return pd.DataFrame(rows)


def standardize_event_family_for_opportunity(value: Any) -> str:
    """Normalize event-family text for conservative opportunity-change matching."""
    if pd.isna(value):
        return ""
    text = str(value).lower()
    text = text.replace("metres", "meters").replace("metre", "meter")
    text = text.replace("kilometres", "kilometers").replace("kilometre", "kilometer")
    text = re.sub(r"\bmen'?s\b|\bwomen'?s\b|\bmixed\b|\bopen\b", " ", text)
    text = re.sub(r"\bteam competition\b", "team", text)
    text = re.sub(r"\bindividual competition\b", "individual", text)
    text = re.sub(r"\bclass\b|\bdivision\b|\bcategory\b", " ", text)
    text = re.sub(r"\b\d+(?:\.\d+)?\s*(?:kg|kilogram|kilograms|meter|meters|m|km|kilometer|kilometers)\b", " ", text)
    text = re.sub(r"\b\d+(?:,\d+)?\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    stopwords = {
        "the",
        "and",
        "a",
        "an",
        "of",
        "for",
        "to",
        "with",
        "under",
        "over",
        "unknown",
        "event",
    }
    tokens = [token for token in text.split() if token and token not in stopwords]
    return " ".join(tokens)


def build_event_opportunity_refined(event_added_removed: pd.DataFrame) -> pd.DataFrame:
    """Refine raw adjacent-Games event-set changes into opportunity signals.

    This is a rule-based triage layer. It flags likely true medal opportunity
    additions conservatively and marks possible renames, reclassifications or
    splits as uncertain instead of treating every raw added event as new supply.
    """
    changes = event_added_removed.copy()
    changes["year"] = pd.to_numeric(changes["year"], errors="coerce")
    changes["standard_event_family"] = changes["event_family"].apply(standardize_event_family_for_opportunity)
    changes["sport_sex_family_key"] = (
        changes["sport"].astype(str)
        + "||"
        + changes["sex"].astype(str)
        + "||"
        + changes["standard_event_family"].astype(str)
    )
    changes["sport_family_key"] = (
        changes["sport"].astype(str)
        + "||"
        + changes["standard_event_family"].astype(str)
    )

    removed_by_year_sport_sex = {
        (int(year), sport, sex): group
        for (year, sport, sex), group in changes[changes["change_type"].eq("removed")].groupby(["year", "sport", "sex"])
    }
    added_by_year_sport_sex = {
        (int(year), sport, sex): group
        for (year, sport, sex), group in changes[changes["change_type"].isin(["added", "restored"])].groupby(["year", "sport", "sex"])
    }
    removed_by_year_sport = {
        (int(year), sport): group
        for (year, sport), group in changes[changes["change_type"].eq("removed")].groupby(["year", "sport"])
    }
    added_by_year_sport = {
        (int(year), sport): group
        for (year, sport), group in changes[changes["change_type"].isin(["added", "restored"])].groupby(["year", "sport"])
    }
    all_added_family_seen_before: set[str] = set()
    all_seen_family: set[str] = set()

    refined_rows = []
    for _, row in changes.sort_values(["year", "sport", "sex", "event"]).iterrows():
        year = int(row["year"]) if not pd.isna(row["year"]) else np.nan
        sport = row["sport"]
        sex = row["sex"]
        original = row["change_type"]
        family = row["standard_event_family"]
        note_parts = []
        rename_or_split = 0
        restored = 1 if original == "restored" else 0
        true_new = 0
        confidence = "medium"

        same_year_removed = removed_by_year_sport_sex.get((year, sport, sex), pd.DataFrame())
        same_year_added = added_by_year_sport_sex.get((year, sport, sex), pd.DataFrame())
        same_sport_removed = removed_by_year_sport.get((year, sport), pd.DataFrame())
        same_sport_added = added_by_year_sport.get((year, sport), pd.DataFrame())
        family_matches_removed = (
            not same_sport_removed.empty
            and family != ""
            and same_sport_removed["standard_event_family"].eq(family).any()
        )
        same_sex_family_matches_removed = (
            not same_year_removed.empty
            and family != ""
            and same_year_removed["standard_event_family"].eq(family).any()
        )
        simultaneous_add_remove = not same_sport_removed.empty and not same_sport_added.empty
        same_family_added_count = (
            int(same_year_added["standard_event_family"].eq(family).sum())
            if not same_year_added.empty and family != ""
            else 0
        )
        same_family_removed_count = (
            int(same_year_removed["standard_event_family"].eq(family).sum())
            if not same_year_removed.empty and family != ""
            else 0
        )

        if original == "removed":
            refined = "removed"
            opportunity_delta = -1
            if simultaneous_add_remove and (family_matches_removed or same_year_added["standard_event_family"].isin([family]).any()):
                confidence = "medium"
                note_parts.append("Removal coincides with added/restored event in same sport; may be paired with reclassification.")
            else:
                note_parts.append("Raw adjacent-Games comparison shows the event absent in the current Games.")
        elif original == "restored":
            refined = "restored"
            opportunity_delta = 1
            confidence = "medium"
            note_parts.append("Event family appeared before, disappeared, and returned; treated as restored, not brand-new.")
        else:
            if same_sex_family_matches_removed:
                refined = "likely_renamed_or_reclassified"
                rename_or_split = 1
                opportunity_delta = 0
                confidence = "medium"
                note_parts.append("Same sport/sex standardized family appears among removed events in the same transition.")
            elif family_matches_removed:
                refined = "likely_renamed_or_reclassified"
                rename_or_split = 1
                opportunity_delta = 0
                confidence = "low"
                note_parts.append("Same sport standardized family appears among removed events, but sex differs or is ambiguous.")
            elif simultaneous_add_remove and same_family_added_count > 1 and same_family_removed_count <= 1:
                refined = "likely_renamed_or_reclassified"
                rename_or_split = 1
                opportunity_delta = max(0, same_family_added_count - same_family_removed_count)
                confidence = "low"
                note_parts.append("Multiple same-family additions during an add/remove transition suggest a split or reclassification.")
            elif family == "" or re.search(r"\bunknown\b", str(row.get("event_family", "")).lower()):
                refined = "uncertain"
                opportunity_delta = np.nan
                confidence = "low"
                note_parts.append("Event family is missing or too generic to classify as a true new opportunity.")
            elif simultaneous_add_remove and same_family_added_count > 1:
                refined = "uncertain"
                opportunity_delta = np.nan
                confidence = "low"
                note_parts.append("Multiple same-family additions occur during a same-sport add/remove transition; possible reclassification cannot be ruled out.")
            else:
                refined = "true_added"
                true_new = 1
                opportunity_delta = 1
                confidence = "medium"
                note_parts.append("No same-transition standardized-family removal detected; conservatively treated as a true added medal opportunity.")

        if original in {"added", "restored"} and refined != "true_added":
            true_new = 0
        if original == "removed":
            true_new = 0

        refined_rows.append(
            {
                "year": year,
                "sport": sport,
                "event": row["event"],
                "sex": sex,
                "original_change_type": original,
                "refined_change_type": refined,
                "event_family": row["event_family"],
                "standard_event_family": family,
                "true_new_medal_opportunity_flag": int(true_new),
                "restored_event_flag": int(restored),
                "rename_or_split_likely_flag": int(rename_or_split),
                "opportunity_delta_vs_prev_games": opportunity_delta,
                "confidence_level": confidence,
                "note": " ".join(note_parts) + " Rule-based classification; not a perfect rename detector.",
            }
        )
        all_seen_family.add(row["sport_family_key"])
        if original in {"added", "restored"}:
            all_added_family_seen_before.add(row["sport_family_key"])

    return pd.DataFrame(refined_rows)


def build_coach_events_template() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "country_name",
            "sport",
            "start_year",
            "end_year",
            "coach_name",
            "event_type",
            "evidence_level",
            "source_note",
        ]
    )


def build_coach_events_template_examples() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "country_name": "Example Country",
                "sport": "Example Sport",
                "start_year": 2000,
                "end_year": 2008,
                "coach_name": "Example Coach Name",
                "event_type": "appointment",
                "evidence_level": "secondary_source",
                "source_note": "Example only. Replace with a verifiable citation; do not use as data.",
            },
            {
                "country_name": "Example Country",
                "sport": "Example Sport",
                "start_year": 2012,
                "end_year": "",
                "coach_name": "Example Coach Name",
                "event_type": "departure",
                "evidence_level": "official_source",
                "source_note": "Example only. Record source title, publisher, URL and access date.",
            },
        ]
    )


def add_sex_category(df: pd.DataFrame, event_col: str = "Event", sex_col: str = "Sex") -> pd.DataFrame:
    out = df.copy()
    out["sex_category"] = out.apply(lambda row: infer_event_sex(row.get(event_col), row.get(sex_col)), axis=1)
    return out


def build_gender_event_expansion_by_year(events_main: pd.DataFrame, medal_unique: pd.DataFrame) -> pd.DataFrame:
    """Build year-sport-sex opportunity counts.

    event_count is the count of unique events. medal_opportunities is the
    count of country-event-medal rows in the deduplicated official medal table.
    """
    event_base = events_main[["Year", "Sport", "Event", "Sex"]].dropna(subset=["Year", "Sport", "Event"]).copy()
    event_base = add_sex_category(event_base)
    event_unique = event_base.drop_duplicates(["Year", "Sport", "Event", "sex_category"])
    event_counts = event_unique.groupby(["Year", "Sport", "sex_category"], as_index=False).agg(
        event_count=("Event", "nunique")
    )

    medals = medal_unique[["Year", "Sport", "Event", "Sex", "country", "Medal"]].copy()
    medals = add_sex_category(medals)
    medal_counts = medals.groupby(["Year", "Sport", "sex_category"], as_index=False).agg(
        medal_opportunities=("Medal", "count"),
        countries_winning_medals=("country", pd.Series.nunique),
    )

    out = event_counts.merge(medal_counts, on=["Year", "Sport", "sex_category"], how="left")
    out["medal_opportunities"] = out["medal_opportunities"].fillna(0).astype(int)
    out["countries_winning_medals"] = out["countries_winning_medals"].fillna(0).astype(int)
    return select_export(
        to_export_columns(out),
        [
            "year",
            "sport",
            "sex_category",
            "event_count",
            "medal_opportunities",
            "countries_winning_medals",
        ],
    )


def build_country_gender_opportunity_capture(
    medal_unique: pd.DataFrame,
    gender_event_expansion: pd.DataFrame,
    athlete_demographics: pd.DataFrame,
) -> pd.DataFrame:
    medals = medal_unique[["Year", "NOC", "iso3", "country", "Sport", "Event", "Sex", "Medal"]].copy()
    medals = add_sex_category(medals)
    country_totals = medals.groupby(["Year", "NOC", "iso3", "country"], as_index=False).agg(
        total_medals=("Medal", "count")
    )
    by_sex = medals.groupby(["Year", "NOC", "iso3", "country", "sex_category"], as_index=False).agg(
        medals=("Medal", "count")
    )
    wide = by_sex.pivot_table(
        index=["Year", "NOC", "iso3", "country"],
        columns="sex_category",
        values="medals",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    for column in ["women", "men", "mixed", "open_unknown"]:
        if column not in wide.columns:
            wide[column] = 0
    wide = wide.rename(
        columns={
            "women": "female_medals",
            "men": "male_medals",
            "mixed": "mixed_medals",
            "open_unknown": "open_unknown_medals",
        }
    )
    out = wide.merge(country_totals, on=["Year", "NOC", "iso3", "country"], how="left")
    out["female_medal_share"] = safe_divide(out["female_medals"], out["total_medals"])

    opp = gender_event_expansion.groupby("year", as_index=False).agg(
        total_event_count=("event_count", "sum"),
        female_event_count=("event_count", lambda s: s[gender_event_expansion.loc[s.index, "sex_category"].eq("women")].sum()),
        total_medal_opportunities=("medal_opportunities", "sum"),
        female_medal_opportunities=("medal_opportunities", lambda s: s[gender_event_expansion.loc[s.index, "sex_category"].eq("women")].sum()),
    )
    opp["female_event_opportunity_share"] = safe_divide(opp["female_event_count"], opp["total_event_count"])
    opp["female_medal_opportunity_share"] = safe_divide(opp["female_medal_opportunities"], opp["total_medal_opportunities"])

    out = out.merge(
        opp[["year", "female_event_opportunity_share", "female_medal_opportunity_share"]].rename(columns={"year": "Year"}),
        on="Year",
        how="left",
    )
    out = out.merge(
        athlete_demographics[["year", "noc", "female_athlete_share"]].rename(columns={"year": "Year", "noc": "NOC"}),
        on=["Year", "NOC"],
        how="left",
    )
    out["female_capture_ratio"] = safe_divide(out["female_medal_share"], out["female_event_opportunity_share"])
    out["female_medal_share_minus_opportunity_share"] = out["female_medal_share"] - out["female_event_opportunity_share"]
    return select_export(
        to_export_columns(out),
        [
            "year",
            "noc",
            "iso3",
            "country_name",
            "total_medals",
            "female_medals",
            "male_medals",
            "mixed_medals",
            "open_unknown_medals",
            "female_medal_share",
            "female_event_opportunity_share",
            "female_medal_opportunity_share",
            "female_athlete_share",
            "female_capture_ratio",
            "female_medal_share_minus_opportunity_share",
        ],
    )


def build_country_gender_medal_structure(country_gender_capture: pd.DataFrame) -> pd.DataFrame:
    out = country_gender_capture.sort_values(["country_name", "year"]).copy()
    out["prev_female_medals"] = out.groupby("country_name")["female_medals"].shift(1)
    out["female_medal_growth"] = out["female_medals"] - out["prev_female_medals"]
    out["prev_female_event_opportunity_share"] = out.groupby("country_name")["female_event_opportunity_share"].shift(1)
    out["female_opportunity_share_growth"] = (
        out["female_event_opportunity_share"] - out["prev_female_event_opportunity_share"]
    )
    out["prev_female_capture_ratio"] = out.groupby("country_name")["female_capture_ratio"].shift(1)
    out["female_capture_ratio_growth"] = out["female_capture_ratio"] - out["prev_female_capture_ratio"]

    def classify(row: pd.Series) -> str:
        medal_growth = row.get("female_medal_growth")
        opp_growth = row.get("female_opportunity_share_growth")
        capture_growth = row.get("female_capture_ratio_growth")
        if pd.isna(medal_growth) or medal_growth <= 0:
            return "no_female_medal_growth"
        opp_positive = not pd.isna(opp_growth) and opp_growth > 0
        capture_positive = not pd.isna(capture_growth) and capture_growth > 0
        if opp_positive and capture_positive:
            return "both_opportunity_and_capture"
        if opp_positive:
            return "opportunity_expansion"
        if capture_positive:
            return "capture_improvement"
        return "unclassified_growth"

    out["growth_interpretation"] = out.apply(classify, axis=1)
    return select_export(
        out,
        [
            "year",
            "noc",
            "iso3",
            "country_name",
            "total_medals",
            "female_medals",
            "male_medals",
            "mixed_medals",
            "female_medal_share",
            "female_event_opportunity_share",
            "female_athlete_share",
            "female_capture_ratio",
            "female_medal_growth",
            "female_opportunity_share_growth",
            "female_capture_ratio_growth",
            "growth_interpretation",
        ],
    )


def build_country_gender_growth_decomposition(
    country_gender_capture: pd.DataFrame,
    athlete_demographics: pd.DataFrame,
) -> pd.DataFrame:
    """Describe female-medal growth with a simplified shift-share decomposition.

    The decomposition treats female medals as a descriptive product of medal
    scale, women's event opportunity share, female athlete participation share
    and a residual conversion rate. It is for mechanism triage, not causal
    identification.
    """
    periods = [(1984, 1996), (1996, 2008), (2008, 2020), (2020, 2024)]
    df = country_gender_capture.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    for column in [
        "total_medals",
        "female_medals",
        "female_event_opportunity_share",
        "female_athlete_share",
        "female_capture_ratio",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    demo = athlete_demographics.copy()
    demo["year"] = pd.to_numeric(demo["year"], errors="coerce")
    if "female_athlete_share" in demo.columns:
        demo["female_athlete_share"] = pd.to_numeric(demo["female_athlete_share"], errors="coerce")
        demo_lookup = demo.drop_duplicates(["year", "noc"]).set_index(["year", "noc"])["female_athlete_share"].to_dict()
    else:
        demo_lookup = {}

    def get_row(country_rows: pd.DataFrame, year: int) -> pd.Series | None:
        matches = country_rows[country_rows["year"].eq(year)]
        if matches.empty:
            return None
        return matches.iloc[0]

    def scaled_conversion(row: pd.Series) -> float:
        total = row.get("total_medals")
        opportunity = row.get("female_event_opportunity_share")
        participation = row.get("female_athlete_share")
        female = row.get("female_medals")
        denominator = total * opportunity * participation
        if pd.isna(denominator) or denominator == 0:
            return np.nan
        return float(female / denominator)

    rows = []
    for (noc, country_name), country_rows in df.groupby(["noc", "country_name"], dropna=False):
        country_rows = country_rows.sort_values("year").copy()
        for start, end in periods:
            start_row = get_row(country_rows, start)
            end_row = get_row(country_rows, end)
            if start_row is None or end_row is None:
                continue

            start_data = start_row.copy()
            end_data = end_row.copy()
            for row in [start_data, end_data]:
                if pd.isna(row.get("female_athlete_share")):
                    share = demo_lookup.get((row.get("year"), row.get("noc")), np.nan)
                    row["female_athlete_share"] = share

            total_start = float(start_data["total_medals"]) if not pd.isna(start_data["total_medals"]) else np.nan
            total_end = float(end_data["total_medals"]) if not pd.isna(end_data["total_medals"]) else np.nan
            female_start = float(start_data["female_medals"]) if not pd.isna(start_data["female_medals"]) else np.nan
            female_end = float(end_data["female_medals"]) if not pd.isna(end_data["female_medals"]) else np.nan
            opportunity_start = float(start_data["female_event_opportunity_share"]) if not pd.isna(start_data["female_event_opportunity_share"]) else np.nan
            opportunity_end = float(end_data["female_event_opportunity_share"]) if not pd.isna(end_data["female_event_opportunity_share"]) else np.nan
            participation_start = float(start_data["female_athlete_share"]) if not pd.isna(start_data["female_athlete_share"]) else np.nan
            participation_end = float(end_data["female_athlete_share"]) if not pd.isna(end_data["female_athlete_share"]) else np.nan
            capture_start = float(start_data["female_capture_ratio"]) if not pd.isna(start_data["female_capture_ratio"]) else np.nan
            capture_end = float(end_data["female_capture_ratio"]) if not pd.isna(end_data["female_capture_ratio"]) else np.nan
            growth_total = female_end - female_start if not pd.isna(female_start) and not pd.isna(female_end) else np.nan

            conversion_start = scaled_conversion(start_data)
            conversion_end = scaled_conversion(end_data)
            can_full_decompose = not any(
                pd.isna(value)
                for value in [
                    total_start,
                    total_end,
                    opportunity_start,
                    opportunity_end,
                    participation_start,
                    participation_end,
                    conversion_start,
                    conversion_end,
                    growth_total,
                ]
            )

            if can_full_decompose:
                opportunity_component = total_start * (opportunity_end - opportunity_start) * participation_start * conversion_start
                participation_component = total_start * opportunity_end * (participation_end - participation_start) * conversion_start
                conversion_component = total_start * opportunity_end * participation_end * (conversion_end - conversion_start)
                explained = opportunity_component + participation_component + conversion_component
                unexplained = growth_total - explained
                confidence = "medium" if abs(growth_total) >= 1 else "low_small_growth"
            else:
                opportunity_component = (
                    total_start * (opportunity_end - opportunity_start) * capture_start
                    if not any(pd.isna(value) for value in [total_start, opportunity_start, opportunity_end, capture_start])
                    else np.nan
                )
                participation_component = np.nan
                conversion_component = (
                    total_start * opportunity_end * (capture_end - capture_start)
                    if not any(pd.isna(value) for value in [total_start, opportunity_end, capture_start, capture_end])
                    else np.nan
                )
                explained = np.nansum([opportunity_component, conversion_component])
                unexplained = growth_total - explained if not pd.isna(growth_total) else np.nan
                confidence = "low_missing_athlete_share_or_conversion_inputs"

            components = {
                "opportunity_expansion": opportunity_component,
                "participation_expansion": participation_component,
                "conversion_improvement": conversion_component,
                "unexplained_or_scale_change": unexplained,
            }
            positive_components = {
                key: value for key, value in components.items() if not pd.isna(value) and value > 0
            }
            if pd.isna(growth_total) or growth_total <= 0:
                dominant = "no_positive_female_medal_growth"
            elif positive_components:
                dominant = max(positive_components, key=positive_components.get)
            else:
                dominant = "unexplained_or_scale_change"

            missing_notes = []
            if pd.isna(participation_start) or pd.isna(participation_end):
                missing_notes.append("female athlete share missing for at least one endpoint")
            if pd.isna(conversion_start) or pd.isna(conversion_end):
                missing_notes.append("conversion factor could not be fully decomposed")
            method_note = (
                "full opportunity-participation-conversion decomposition"
                if can_full_decompose
                else "limited decomposition using opportunity and capture ratio"
            )
            limits = f" Limits: {'; '.join(missing_notes)}." if missing_notes else ""
            interpretation = (
                f"{country_name} {start}-{end}: female medals moved from "
                f"{female_start:g} to {female_end:g} ({growth_total:+g}). "
                f"Dominant descriptive driver: {dominant}. "
                f"Opportunity component {opportunity_component if pd.isna(opportunity_component) else round(opportunity_component, 2)}, "
                f"participation component {participation_component if pd.isna(participation_component) else round(participation_component, 2)}, "
                f"conversion component {conversion_component if pd.isna(conversion_component) else round(conversion_component, 2)}, "
                f"unexplained/scale component {unexplained if pd.isna(unexplained) else round(unexplained, 2)}. "
                f"This is descriptive shift-share decomposition, not causal identification. Method: {method_note}.{limits}"
            )

            rows.append(
                {
                    "country_name": country_name,
                    "noc": noc,
                    "period_start": start,
                    "period_end": end,
                    "female_medals_start": female_start,
                    "female_medals_end": female_end,
                    "female_medal_growth_total": growth_total,
                    "female_event_opportunity_start": opportunity_start,
                    "female_event_opportunity_end": opportunity_end,
                    "female_athlete_share_start": participation_start,
                    "female_athlete_share_end": participation_end,
                    "female_capture_ratio_start": capture_start,
                    "female_capture_ratio_end": capture_end,
                    "opportunity_expansion_component": opportunity_component,
                    "participation_expansion_component": participation_component,
                    "conversion_improvement_component": conversion_component,
                    "unexplained_component": unexplained,
                    "dominant_growth_driver": dominant,
                    "decomposition_confidence": confidence,
                    "interpretation_text": interpretation,
                }
            )
    return pd.DataFrame(rows)


def build_country_profile_features(
    country_year: pd.DataFrame,
    economic_efficiency: pd.DataFrame,
    athlete_demographics: pd.DataFrame,
) -> pd.DataFrame:
    """Build country-level profile features for typology clustering."""
    cy = to_export_columns(country_year).copy()
    eff = economic_efficiency.copy()
    demo = athlete_demographics.copy()

    cy["medals_per_100_athletes"] = safe_divide(cy["total_medals"], cy["unique_athletes"]) * 100
    current_year = int(pd.to_numeric(cy["year"], errors="coerce").max())
    recent_cutoff = current_year - 12

    base = cy.groupby(["noc", "iso3", "country_name"], as_index=False).agg(
        total_medals_mean=("total_medals", "mean"),
        gold_share_mean=("gold_share", "mean"),
        medal_volatility=("total_medals", "std"),
        sport_concentration_hhi=("country_sport_hhi", "mean"),
        medal_sports_count=("medal_sports", "mean"),
        recent_growth_rate=("medal_growth_rate_prev_games", lambda s: s[pd.to_numeric(cy.loc[s.index, "year"], errors="coerce") >= recent_cutoff].mean()),
        medals_per_100_athletes=("medals_per_100_athletes", "mean"),
        country_first_medal_year=("country_first_medal_year", "min"),
        years_with_medals=("year", "nunique"),
        recent_total_medals_mean=("total_medals", lambda s: s[pd.to_numeric(cy.loc[s.index, "year"], errors="coerce") >= recent_cutoff].mean()),
    )

    eff_agg = eff.groupby(["noc", "iso3", "country_name"], as_index=False).agg(
        medals_per_million_people=("medals_per_million_people", "mean"),
        medals_per_100b_gdp=("medals_per_100b_gdp", "mean"),
        economic_data_complete_rate=("economic_data_complete", "mean"),
    )
    demo_agg = demo.groupby(["noc", "iso3", "country_name"], as_index=False).agg(
        female_athlete_share=("female_athlete_share", "mean"),
        athletes_mean=("athletes", "mean"),
    )

    out = base.merge(eff_agg, on=["noc", "iso3", "country_name"], how="left")
    out = out.merge(demo_agg, on=["noc", "iso3", "country_name"], how="left")
    out["medal_volatility"] = out["medal_volatility"].fillna(0)
    out["recent_growth_rate"] = out["recent_growth_rate"].replace([np.inf, -np.inf], np.nan)
    out["profile_note"] = "Country profile features aggregated from medal, diversity, gender and efficiency outputs."
    return select_export(
        out,
        [
            "noc",
            "iso3",
            "country_name",
            "total_medals_mean",
            "gold_share_mean",
            "medal_volatility",
            "sport_concentration_hhi",
            "medal_sports_count",
            "recent_growth_rate",
            "female_athlete_share",
            "medals_per_million_people",
            "medals_per_100b_gdp",
            "medals_per_100_athletes",
            "country_first_medal_year",
            "years_with_medals",
            "recent_total_medals_mean",
            "economic_data_complete_rate",
            "athletes_mean",
            "profile_note",
        ],
    )


def zscore_frame(df: pd.DataFrame, columns: list[str]) -> tuple[pd.DataFrame, dict[str, float], dict[str, float]]:
    out = df.copy()
    means: dict[str, float] = {}
    stds: dict[str, float] = {}
    for col in columns:
        series = pd.to_numeric(out[col], errors="coerce")
        mean = float(series.mean()) if not series.dropna().empty else 0.0
        std = float(series.std(ddof=0)) if not series.dropna().empty else 0.0
        means[col] = mean
        stds[col] = std
        if std == 0 or pd.isna(std):
            out[col] = 0.0
        else:
            out[col] = (series.fillna(mean) - mean) / std
    return out, means, stds


def kmeans_numpy(matrix: np.ndarray, k: int, n_init: int = 15, max_iter: int = 200, seed: int = 42) -> tuple[np.ndarray, np.ndarray, float]:
    rng = np.random.default_rng(seed)
    best_labels = None
    best_centroids = None
    best_inertia = np.inf
    n = matrix.shape[0]
    if n == 0:
        return np.array([]), np.empty((0, matrix.shape[1])), np.nan
    k = max(1, min(k, n))
    for _ in range(n_init):
        centroid_idx = rng.choice(n, size=k, replace=False)
        centroids = matrix[centroid_idx].copy()
        labels = np.zeros(n, dtype=int)
        for _iter in range(max_iter):
            distances = ((matrix[:, None, :] - centroids[None, :, :]) ** 2).sum(axis=2)
            new_labels = distances.argmin(axis=1)
            if np.array_equal(new_labels, labels):
                break
            labels = new_labels
            new_centroids = centroids.copy()
            for ci in range(k):
                cluster_points = matrix[labels == ci]
                if len(cluster_points) == 0:
                    new_centroids[ci] = matrix[rng.integers(0, n)]
                else:
                    new_centroids[ci] = cluster_points.mean(axis=0)
            centroids = new_centroids
        inertia = float(((matrix - centroids[labels]) ** 2).sum())
        if inertia < best_inertia:
            best_inertia = inertia
            best_labels = labels.copy()
            best_centroids = centroids.copy()
    return best_labels, best_centroids, best_inertia


def recommend_country_cluster_k(features: pd.DataFrame, cluster_cols: list[str]) -> int:
    n = len(features)
    if n < 12:
        return max(2, min(4, n))
    return 5


def name_country_cluster(summary: pd.Series) -> str:
    if summary["total_medals_mean"] >= 30 and summary["gold_share_mean"] >= 0.32:
        return "Elite diversified powers"
    if summary["medals_per_million_people"] >= 2 and summary["total_medals_mean"] < 15:
        return "Efficient small-population performers"
    if summary["recent_growth_rate"] >= 0.15:
        return "Rising challengers"
    if summary["sport_concentration_hhi"] >= 0.55:
        return "Specialist concentrated programs"
    return "Broad mid-tier competitors"


def build_country_cluster_labels(country_profile_features: pd.DataFrame) -> pd.DataFrame:
    features = country_profile_features.copy()
    cluster_cols = [
        "total_medals_mean",
        "gold_share_mean",
        "medal_volatility",
        "sport_concentration_hhi",
        "medal_sports_count",
        "recent_growth_rate",
        "female_athlete_share",
        "medals_per_million_people",
        "medals_per_100b_gdp",
        "medals_per_100_athletes",
        "country_first_medal_year",
    ]
    scaled, means, stds = zscore_frame(features, cluster_cols)
    matrix = scaled[cluster_cols].astype(float).to_numpy()
    k = recommend_country_cluster_k(features, cluster_cols)
    labels, centroids, inertia = kmeans_numpy(matrix, k=k)
    out = features.copy()
    out["cluster_id"] = labels

    summary = out.groupby("cluster_id", as_index=False).agg(
        cluster_size=("country_name", "count"),
        total_medals_mean=("total_medals_mean", "mean"),
        gold_share_mean=("gold_share_mean", "mean"),
        sport_concentration_hhi=("sport_concentration_hhi", "mean"),
        recent_growth_rate=("recent_growth_rate", "mean"),
        medals_per_million_people=("medals_per_million_people", "mean"),
        medals_per_100b_gdp=("medals_per_100b_gdp", "mean"),
    )
    summary["cluster_name"] = summary.apply(name_country_cluster, axis=1)

    exemplars = []
    for cluster_id in summary["cluster_id"]:
        rows = out[out["cluster_id"].eq(cluster_id)].copy()
        rows["rank_score"] = (
            pd.to_numeric(rows["total_medals_mean"], errors="coerce").fillna(0)
            + pd.to_numeric(rows["gold_share_mean"], errors="coerce").fillna(0) * 10
            + pd.to_numeric(rows["medals_per_million_people"], errors="coerce").fillna(0)
        )
        examples = rows.sort_values("rank_score", ascending=False)["country_name"].head(5).tolist()
        exemplars.append({"cluster_id": cluster_id, "typical_countries": ", ".join(examples)})
    summary = summary.merge(pd.DataFrame(exemplars), on="cluster_id", how="left")

    out = out.merge(summary[["cluster_id", "cluster_name", "typical_countries"]], on="cluster_id", how="left")
    out["cluster_k"] = k
    out["clustering_method"] = "z-score standardization + numpy k-means"
    out["cluster_note"] = "Cluster labels are descriptive typologies, not fixed rankings."
    return select_export(
        out,
        [
            "noc",
            "iso3",
            "country_name",
            "cluster_id",
            "cluster_name",
            "cluster_k",
            "typical_countries",
            "total_medals_mean",
            "gold_share_mean",
            "medal_volatility",
            "sport_concentration_hhi",
            "medal_sports_count",
            "recent_growth_rate",
            "female_athlete_share",
            "medals_per_million_people",
            "medals_per_100b_gdp",
            "medals_per_100_athletes",
            "country_first_medal_year",
            "clustering_method",
            "cluster_note",
        ],
    )


def min_max_scale(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(np.nan, index=series.index, dtype=float)
    min_value = float(valid.min())
    max_value = float(valid.max())
    if math.isclose(min_value, max_value):
        return pd.Series(0.5, index=series.index, dtype=float)
    return (numeric - min_value) / (max_value - min_value)


def classify_sport_barrier_type(row: pd.Series) -> str:
    if (
        pd.to_numeric(row.get("female_event_growth_rate"), errors="coerce") >= 0.25
        and pd.to_numeric(row.get("breakthrough_friendliness_index"), errors="coerce") >= 0.55
    ):
        return "Women-expansion opportunity sports"
    if (
        pd.to_numeric(row.get("event_growth_rate"), errors="coerce") >= 0.2
        and pd.to_numeric(row.get("new_country_medal_count"), errors="coerce") >= 8
    ):
        return "Emerging opportunity sports"
    if (
        pd.to_numeric(row.get("entry_barrier_index"), errors="coerce") >= 0.65
        or (
            pd.to_numeric(row.get("sport_country_hhi_mean"), errors="coerce") >= 0.22
            and pd.to_numeric(row.get("repeat_winner_rate"), errors="coerce") >= 0.75
        )
    ):
        return "High-barrier traditional-power sports"
    return "Medium-barrier breakthrough sports"


def build_sport_entry_barrier_index(
    sport_share: pd.DataFrame,
    sport_globalization: pd.DataFrame,
    gender_event_expansion: pd.DataFrame,
    economic_efficiency: pd.DataFrame,
) -> pd.DataFrame:
    """Build sport-level barrier and breakthrough friendliness metrics."""
    share = to_export_columns(sport_share).copy()
    global_metrics = to_export_columns(sport_globalization).copy()
    gender = gender_event_expansion.copy()
    economic = economic_efficiency.copy()

    share["year"] = pd.to_numeric(share["year"], errors="coerce")
    global_metrics["year"] = pd.to_numeric(global_metrics["year"], errors="coerce")
    gender["year"] = pd.to_numeric(gender["year"], errors="coerce")
    economic["year"] = pd.to_numeric(economic["year"], errors="coerce")

    sport_base = global_metrics.groupby("sport", as_index=False).agg(
        sport_country_hhi_mean=("sport_country_hhi", "mean"),
        countries_winning_medals_mean=("countries_winning_medals", "mean"),
        top1_share_mean=("max_country_share", "mean"),
        sport_years_observed=("year", "nunique"),
        medal_opportunity_count=("sport_year_total_medals", "sum"),
    )

    event_totals = gender.groupby(["sport", "year"], as_index=False).agg(
        total_event_count=("event_count", "sum"),
        total_medal_opportunities=("medal_opportunities", "sum"),
    )
    women_events = gender[gender["sex_category"].eq("women")].copy()
    women_events = women_events.rename(
        columns={
            "event_count": "women_event_count",
            "medal_opportunities": "women_medal_opportunities",
            "countries_winning_medals": "women_countries_winning_medals",
        }
    )
    event_totals = event_totals.merge(
        women_events[["sport", "year", "women_event_count", "women_medal_opportunities"]],
        on=["sport", "year"],
        how="left",
    )
    event_totals["women_event_count"] = event_totals["women_event_count"].fillna(0)
    event_totals["women_medal_opportunities"] = event_totals["women_medal_opportunities"].fillna(0)

    event_features = []
    for sport, group in event_totals.groupby("sport"):
        group = group.sort_values("year").copy()
        first_events = pd.to_numeric(group["total_event_count"], errors="coerce").dropna()
        last_events = pd.to_numeric(group["total_event_count"], errors="coerce").dropna()
        first_women = pd.to_numeric(group["women_event_count"], errors="coerce").dropna()
        last_women = pd.to_numeric(group["women_event_count"], errors="coerce").dropna()
        first_event_value = float(first_events.iloc[0]) if not first_events.empty else np.nan
        last_event_value = float(last_events.iloc[-1]) if not last_events.empty else np.nan
        first_women_value = float(first_women.iloc[0]) if not first_women.empty else np.nan
        last_women_value = float(last_women.iloc[-1]) if not last_women.empty else np.nan
        event_growth_rate = (
            (last_event_value - first_event_value) / first_event_value
            if pd.notna(first_event_value) and first_event_value > 0
            else np.nan
        )
        female_event_growth_rate = (
            (last_women_value - first_women_value) / first_women_value
            if pd.notna(first_women_value) and first_women_value > 0
            else (1.0 if pd.notna(last_women_value) and last_women_value > 0 else np.nan)
        )
        event_features.append(
            {
                "sport": sport,
                "event_growth_rate": event_growth_rate,
                "female_event_growth_rate": female_event_growth_rate,
                "latest_event_count": last_event_value,
                "latest_women_event_count": last_women_value,
                "latest_total_medal_opportunities": pd.to_numeric(
                    group["total_medal_opportunities"], errors="coerce"
                ).iloc[-1]
                if not group.empty
                else np.nan,
            }
        )
    event_features_df = pd.DataFrame(event_features)

    share = share.sort_values(["sport", "country_name", "year"]).copy()
    first_sport_medal_year = share.groupby(["sport", "noc"])["year"].transform("min")
    share["is_first_country_sport_medal"] = (share["year"] == first_sport_medal_year).astype(int)
    new_country_medal = share[share["is_first_country_sport_medal"].eq(1)].groupby("sport", as_index=False).agg(
        new_country_medal_count=("noc", "nunique")
    )

    share["previous_country_sport_medals"] = share.groupby(["sport", "noc"])["total_medals"].cumsum() - share["total_medals"]
    share["is_repeat_winner"] = (share["previous_country_sport_medals"] > 0).astype(int)
    repeat_rate = share.groupby("sport", as_index=False).agg(
        repeat_winner_rate=("is_repeat_winner", "mean")
    )

    winners = share.merge(
        economic[
            [
                "year",
                "noc",
                "country_name",
                "population",
                "gdp_total",
                "medals_per_million_people",
                "medals_per_100b_gdp",
            ]
        ],
        on=["year", "noc", "country_name"],
        how="left",
    )
    winner_economic = winners.groupby("sport", as_index=False).agg(
        median_population_of_winners=("population", "median"),
        median_gdp_of_winners=("gdp_total", "median"),
        median_medals_per_million_of_winners=("medals_per_million_people", "median"),
        median_medals_per_100b_gdp_of_winners=("medals_per_100b_gdp", "median"),
    )

    out = sport_base.merge(event_features_df, on="sport", how="left")
    out = out.merge(new_country_medal, on="sport", how="left")
    out = out.merge(repeat_rate, on="sport", how="left")
    out = out.merge(winner_economic, on="sport", how="left")
    out["new_country_medal_count"] = out["new_country_medal_count"].fillna(0)

    components = pd.DataFrame({"sport": out["sport"]})
    components["friendly_low_hhi"] = 1 - min_max_scale(out["sport_country_hhi_mean"])
    components["friendly_broad_winner_base"] = min_max_scale(out["countries_winning_medals_mean"])
    components["friendly_low_top1_share"] = 1 - min_max_scale(out["top1_share_mean"])
    components["friendly_new_country_entry"] = min_max_scale(out["new_country_medal_count"])
    components["friendly_medal_opportunities"] = min_max_scale(out["medal_opportunity_count"])
    components["friendly_event_growth"] = min_max_scale(out["event_growth_rate"])
    components["friendly_female_event_growth"] = min_max_scale(out["female_event_growth_rate"])
    components["friendly_lower_winner_population"] = 1 - min_max_scale(out["median_population_of_winners"])
    components["friendly_lower_winner_gdp"] = 1 - min_max_scale(out["median_gdp_of_winners"])
    components["friendly_low_repeat_winner_rate"] = 1 - min_max_scale(out["repeat_winner_rate"])

    component_weights = {
        "friendly_low_hhi": 0.20,
        "friendly_broad_winner_base": 0.15,
        "friendly_low_top1_share": 0.15,
        "friendly_new_country_entry": 0.15,
        "friendly_medal_opportunities": 0.10,
        "friendly_event_growth": 0.10,
        "friendly_female_event_growth": 0.05,
        "friendly_lower_winner_population": 0.05,
        "friendly_lower_winner_gdp": 0.03,
        "friendly_low_repeat_winner_rate": 0.02,
    }
    weighted_sum = pd.Series(0.0, index=out.index, dtype=float)
    weight_sum = pd.Series(0.0, index=out.index, dtype=float)
    for column, weight in component_weights.items():
        values = pd.to_numeric(components[column], errors="coerce")
        weighted_sum = weighted_sum + values.fillna(0) * weight
        weight_sum = weight_sum + values.notna().astype(float) * weight
    out["breakthrough_friendliness_index"] = safe_divide(weighted_sum, weight_sum)
    out["entry_barrier_index"] = 1 - out["breakthrough_friendliness_index"]
    out["sport_type"] = out.apply(classify_sport_barrier_type, axis=1)

    def build_recommendation(row: pd.Series) -> str:
        sport_type = row.get("sport_type", "")
        if sport_type == "Women-expansion opportunity sports":
            return "Women's event expansion is relatively strong; suitable for targeted entry where female participation can scale."
        if sport_type == "Emerging opportunity sports":
            return "Program growth and new-medal-country churn are relatively high; suitable for selective breakthrough attempts."
        if sport_type == "High-barrier traditional-power sports":
            return "Medal concentration and incumbent persistence are high; breakthroughs likely require longer-cycle investment."
        return "Barrier indicators are moderate; selective niche investment may be more realistic than broad expansion."

    out["recommendation_note"] = out.apply(build_recommendation, axis=1)
    out["index_note"] = (
        "Higher breakthrough_friendliness_index means a sport looks easier for new or smaller countries to break into. "
        "This is a descriptive composite index, not a causal estimate."
    )
    return select_export(
        out,
        [
            "sport",
            "sport_country_hhi_mean",
            "countries_winning_medals_mean",
            "top1_share_mean",
            "new_country_medal_count",
            "medal_opportunity_count",
            "event_growth_rate",
            "female_event_growth_rate",
            "median_population_of_winners",
            "median_gdp_of_winners",
            "median_medals_per_million_of_winners",
            "median_medals_per_100b_gdp_of_winners",
            "repeat_winner_rate",
            "latest_event_count",
            "latest_women_event_count",
            "latest_total_medal_opportunities",
            "breakthrough_friendliness_index",
            "entry_barrier_index",
            "sport_type",
            "recommendation_note",
            "index_note",
        ],
    )


def classify_modern_sport_type(row: pd.Series) -> str:
    friendliness = pd.to_numeric(row.get("breakthrough_friendliness_index_modern"), errors="coerce")
    barrier = pd.to_numeric(row.get("entry_barrier_index_modern"), errors="coerce")
    female_growth = pd.to_numeric(row.get("female_event_growth_rate_modern"), errors="coerce")
    new_countries = pd.to_numeric(row.get("new_country_medal_count_modern"), errors="coerce")
    hhi = pd.to_numeric(row.get("sport_country_hhi_mean_modern"), errors="coerce")
    repeat = pd.to_numeric(row.get("repeat_winner_rate_modern"), errors="coerce")
    if female_growth >= 0.25 and friendliness >= 0.55:
        return "Modern women-expansion opportunity"
    if new_countries >= 8 and friendliness >= 0.55:
        return "Modern breakthrough-friendly"
    if barrier >= 0.65 or (hhi >= 0.22 and repeat >= 0.75):
        return "Modern high-barrier incumbent sport"
    return "Modern medium-barrier sport"


def build_sport_entry_barrier_modern(
    sport_entry_barrier: pd.DataFrame,
    sport_breakthrough_cases: pd.DataFrame,
    sport_share: pd.DataFrame,
    gender_event_expansion: pd.DataFrame,
    modern_start: int = 1992,
    modern_end: int = 2024,
) -> pd.DataFrame:
    """Build a modern-only sport entry barrier index for strategy discussion.

    The table excludes historical one-off sports by requiring observations in
    the modern window. active_modern_sport_flag is set when a sport appears in
    the latest observed Games in the project data.
    """
    _ = sport_entry_barrier
    _ = sport_breakthrough_cases
    share_all = to_export_columns(sport_share).copy()
    gender = gender_event_expansion.copy()
    share_all["year"] = pd.to_numeric(share_all["year"], errors="coerce")
    gender["year"] = pd.to_numeric(gender["year"], errors="coerce")
    for column in [
        "total_medals",
        "country_medal_share_in_sport",
        "sport_country_hhi",
        "sport_year_total_medals",
    ]:
        if column in share_all.columns:
            share_all[column] = pd.to_numeric(share_all[column], errors="coerce")
    for column in ["event_count", "medal_opportunities"]:
        if column in gender.columns:
            gender[column] = pd.to_numeric(gender[column], errors="coerce")

    share = share_all[
        share_all["year"].between(modern_start, modern_end, inclusive="both")
    ].copy()
    if share.empty:
        return pd.DataFrame()
    latest_year = int(share["year"].max())
    active_sports = set(share.loc[share["year"].eq(latest_year), "sport"].dropna().astype(str))

    sport_base = share.groupby("sport", as_index=False).agg(
        modern_year_min=("year", "min"),
        modern_year_max=("year", "max"),
        countries_winning_medals_mean_modern=("noc", pd.Series.nunique),
        sport_country_hhi_mean_modern=("sport_country_hhi", "mean"),
        top1_share_mean_modern=("country_medal_share_in_sport", "max"),
        medal_opportunity_count_modern=("total_medals", "sum"),
    )
    yearly_breadth = share.groupby(["sport", "year"], as_index=False).agg(
        countries_winning_medals=("noc", pd.Series.nunique),
        top1_share=("country_medal_share_in_sport", "max"),
        sport_country_hhi=("sport_country_hhi", "first"),
        sport_year_medals=("sport_year_total_medals", "first"),
    )
    sport_base = yearly_breadth.groupby("sport", as_index=False).agg(
        modern_year_min=("year", "min"),
        modern_year_max=("year", "max"),
        countries_winning_medals_mean_modern=("countries_winning_medals", "mean"),
        sport_country_hhi_mean_modern=("sport_country_hhi", "mean"),
        top1_share_mean_modern=("top1_share", "mean"),
        medal_opportunity_count_modern=("sport_year_medals", "sum"),
    )

    first_sport_medal_year = share_all.groupby(["sport", "noc"])["year"].transform("min")
    share_all["country_first_sport_medal_year"] = first_sport_medal_year
    modern_firsts = share_all[
        share_all["country_first_sport_medal_year"].between(modern_start, modern_end, inclusive="both")
        & share_all["year"].eq(share_all["country_first_sport_medal_year"])
    ]
    new_country_medal = modern_firsts.groupby("sport", as_index=False).agg(
        new_country_medal_count_modern=("noc", "nunique")
    )

    share_all = share_all.sort_values(["sport", "noc", "year"]).copy()
    share_all["previous_country_sport_medals"] = (
        share_all.groupby(["sport", "noc"])["total_medals"].cumsum() - share_all["total_medals"]
    )
    modern_repeat = share_all[
        share_all["year"].between(modern_start, modern_end, inclusive="both")
    ].copy()
    modern_repeat["is_repeat_winner_modern"] = (modern_repeat["previous_country_sport_medals"] > 0).astype(int)
    repeat_rate = modern_repeat.groupby("sport", as_index=False).agg(
        repeat_winner_rate_modern=("is_repeat_winner_modern", "mean")
    )

    gender_modern = gender[
        gender["year"].between(modern_start, modern_end, inclusive="both")
    ].copy()
    event_totals = gender_modern.groupby(["sport", "year"], as_index=False).agg(
        total_event_count=("event_count", "sum"),
        total_medal_opportunities=("medal_opportunities", "sum"),
    )
    women_events = gender_modern[gender_modern["sex_category"].eq("women")].copy()
    women_year = women_events.groupby(["sport", "year"], as_index=False).agg(
        women_event_count=("event_count", "sum"),
    )
    event_totals = event_totals.merge(women_year, on=["sport", "year"], how="left")
    event_totals["women_event_count"] = event_totals["women_event_count"].fillna(0)
    event_features = []
    for sport, group in event_totals.groupby("sport"):
        group = group.sort_values("year").copy()
        total_events = pd.to_numeric(group["total_event_count"], errors="coerce").dropna()
        women_counts = pd.to_numeric(group["women_event_count"], errors="coerce").dropna()
        first_women = float(women_counts.iloc[0]) if not women_counts.empty else np.nan
        last_women = float(women_counts.iloc[-1]) if not women_counts.empty else np.nan
        female_growth = (
            (last_women - first_women) / first_women
            if pd.notna(first_women) and first_women > 0
            else (1.0 if pd.notna(last_women) and last_women > 0 else np.nan)
        )
        event_features.append(
            {
                "sport": sport,
                "female_event_growth_rate_modern": female_growth,
                "latest_event_count_modern": float(total_events.iloc[-1]) if not total_events.empty else np.nan,
                "latest_women_event_count_modern": last_women,
            }
        )
    event_features_df = pd.DataFrame(event_features)

    out = sport_base.merge(new_country_medal, on="sport", how="left")
    out = out.merge(repeat_rate, on="sport", how="left")
    out = out.merge(event_features_df, on="sport", how="left")
    out["new_country_medal_count_modern"] = out["new_country_medal_count_modern"].fillna(0)
    out["active_modern_sport_flag"] = out["sport"].astype(str).isin(active_sports).astype(int)

    components = pd.DataFrame({"sport": out["sport"]})
    components["friendly_low_hhi"] = 1 - min_max_scale(out["sport_country_hhi_mean_modern"])
    components["friendly_broad_winner_base"] = min_max_scale(out["countries_winning_medals_mean_modern"])
    components["friendly_low_top1_share"] = 1 - min_max_scale(out["top1_share_mean_modern"])
    components["friendly_new_country_entry"] = min_max_scale(out["new_country_medal_count_modern"])
    components["friendly_medal_opportunities"] = min_max_scale(out["medal_opportunity_count_modern"])
    components["friendly_female_event_growth"] = min_max_scale(out["female_event_growth_rate_modern"])
    components["friendly_low_repeat_winner_rate"] = 1 - min_max_scale(out["repeat_winner_rate_modern"])

    component_weights = {
        "friendly_low_hhi": 0.24,
        "friendly_broad_winner_base": 0.18,
        "friendly_low_top1_share": 0.18,
        "friendly_new_country_entry": 0.18,
        "friendly_medal_opportunities": 0.12,
        "friendly_female_event_growth": 0.05,
        "friendly_low_repeat_winner_rate": 0.05,
    }
    weighted_sum = pd.Series(0.0, index=out.index, dtype=float)
    weight_sum = pd.Series(0.0, index=out.index, dtype=float)
    for column, weight in component_weights.items():
        values = pd.to_numeric(components[column], errors="coerce")
        weighted_sum = weighted_sum + values.fillna(0) * weight
        weight_sum = weight_sum + values.notna().astype(float) * weight
    out["breakthrough_friendliness_index_modern"] = safe_divide(weighted_sum, weight_sum)
    out["entry_barrier_index_modern"] = 1 - out["breakthrough_friendliness_index_modern"]
    out["modern_sport_type"] = out.apply(classify_modern_sport_type, axis=1)

    def recommendation(row: pd.Series) -> str:
        if int(row.get("active_modern_sport_flag", 0)) != 1:
            return "Not active in the latest observed Games; exclude from modern recommendation unless the program returns."
        sport_type = row.get("modern_sport_type", "")
        if sport_type == "Modern breakthrough-friendly":
            return "Modern medal distribution is relatively broad; useful for selective breakthrough strategy research."
        if sport_type == "Modern women-expansion opportunity":
            return "Women's event growth and medal breadth make this a candidate opportunity area where participation can scale."
        if sport_type == "Modern high-barrier incumbent sport":
            return "Modern medal concentration or repeat-winner persistence is high; breakthroughs likely require longer-cycle investment."
        return "Modern barrier indicators are moderate; use as strategy reference, not as a medal prediction."

    out["recommendation_note"] = out.apply(recommendation, axis=1)
    out["index_note"] = (
        "Modern entry-barrier index uses 1992-2024 medal distribution, new-country entries, opportunity breadth and repeat-winner persistence. "
        "It is a strategy reference, not a medal prediction."
    )
    out = out[out["active_modern_sport_flag"].eq(1)].copy()
    return select_export(
        out.sort_values("breakthrough_friendliness_index_modern", ascending=False),
        [
            "sport",
            "modern_year_min",
            "modern_year_max",
            "active_modern_sport_flag",
            "countries_winning_medals_mean_modern",
            "sport_country_hhi_mean_modern",
            "top1_share_mean_modern",
            "new_country_medal_count_modern",
            "female_event_growth_rate_modern",
            "medal_opportunity_count_modern",
            "repeat_winner_rate_modern",
            "breakthrough_friendliness_index_modern",
            "entry_barrier_index_modern",
            "modern_sport_type",
            "recommendation_note",
            "index_note",
        ],
    )


def build_sport_breakthrough_cases(
    sport_share: pd.DataFrame,
    sport_entry_barrier: pd.DataFrame,
    economic_efficiency: pd.DataFrame,
) -> pd.DataFrame:
    """Build country-sport breakthrough case table with sport-level barrier context."""
    share = to_export_columns(sport_share).copy()
    barrier = sport_entry_barrier.copy()
    economic = economic_efficiency.copy()

    share["year"] = pd.to_numeric(share["year"], errors="coerce")
    economic["year"] = pd.to_numeric(economic["year"], errors="coerce")
    share = share.sort_values(["sport", "country_name", "year"]).copy()
    share["previous_country_sport_medals"] = share.groupby(["sport", "noc"])["total_medals"].cumsum() - share["total_medals"]
    share["country_first_sport_medal_year"] = share.groupby(["sport", "noc"])["year"].transform("min")
    share["is_first_country_sport_medal"] = (share["year"] == share["country_first_sport_medal_year"]).astype(int)

    out = share.merge(
        economic[
            [
                "year",
                "noc",
                "country_name",
                "population",
                "gdp_total",
                "medals_per_million_people",
                "medals_per_100b_gdp",
            ]
        ],
        on=["year", "noc", "country_name"],
        how="left",
    )
    out = out.merge(
        barrier[
            [
                "sport",
                "breakthrough_friendliness_index",
                "entry_barrier_index",
                "sport_type",
                "recommendation_note",
            ]
        ],
        on="sport",
        how="left",
    )

    def classify_case(row: pd.Series) -> str:
        if pd.to_numeric(row.get("is_first_country_sport_medal"), errors="coerce") == 1:
            return "first_sport_medal_breakthrough"
        if pd.to_numeric(row.get("previous_country_sport_medals"), errors="coerce") <= 2:
            return "early_stage_repeat_breakthrough"
        return "established_program_result"

    out["case_type"] = out.apply(classify_case, axis=1)

    def build_case_note(row: pd.Series) -> str:
        if row.get("case_type") == "first_sport_medal_breakthrough":
            return "First recorded medal for this country in this sport."
        if row.get("case_type") == "early_stage_repeat_breakthrough":
            return "Country had limited prior medals in this sport before this Olympic year."
        return "Country already had an established medal history in this sport."

    out["case_note"] = out.apply(build_case_note, axis=1)
    out = out.sort_values(["year", "sport", "total_medals"], ascending=[False, True, False])
    return select_export(
        out,
        [
            "year",
            "sport",
            "noc",
            "iso3",
            "country_name",
            "total_medals",
            "gold_medals",
            "sport_year_total_medals",
            "country_medal_share_in_sport",
            "country_first_sport_medal_year",
            "previous_country_sport_medals",
            "is_first_country_sport_medal",
            "population",
            "gdp_total",
            "medals_per_million_people",
            "medals_per_100b_gdp",
            "breakthrough_friendliness_index",
            "entry_barrier_index",
            "sport_type",
            "case_type",
            "case_note",
            "recommendation_note",
        ],
    )


def build_leap_index(country_sport_year: pd.DataFrame) -> pd.DataFrame:
    csy = country_sport_year.sort_values(["country", "Sport", "Year"]).copy()
    csy["prev_total_medals"] = csy.groupby(["country", "Sport"])["total_medals"].shift(1).fillna(0)
    csy["prev_gold_medals"] = csy.groupby(["country", "Sport"])["gold_medals"].shift(1).fillna(0)
    csy["rolling3_prev_medals"] = csy.groupby(["country", "Sport"])["total_medals"].transform(
        lambda s: s.shift(1).rolling(3, min_periods=1).mean()
    ).fillna(0)
    csy["medal_growth"] = csy["total_medals"] - csy["prev_total_medals"]
    csy["gold_growth"] = csy["gold_medals"] - csy["prev_gold_medals"]
    csy["above_recent_baseline"] = (csy["total_medals"] - csy["rolling3_prev_medals"]).clip(lower=0)
    first_year = csy.groupby(["country", "Sport"])["Year"].transform("min")
    csy["first_medal_bonus"] = (csy["Year"] == first_year).astype(int)
    csy["next_total_medals"] = csy.groupby(["country", "Sport"])["total_medals"].shift(-1).fillna(0)
    csy["continuity_bonus"] = (csy["next_total_medals"] > 0).astype(float)

    sport_total = country_sport_year.groupby(["Year", "Sport"], as_index=False)["total_medals"].sum()
    sport_total = sport_total.sort_values(["Sport", "Year"])
    sport_total["prev_sport_total"] = sport_total.groupby("Sport")["total_medals"].shift(1).fillna(0)
    sport_total["sport_total_growth"] = sport_total["total_medals"] - sport_total["prev_sport_total"]
    csy = csy.merge(
        sport_total[["Year", "Sport", "total_medals", "sport_total_growth"]].rename(
            columns={"total_medals": "sport_total_medals"}
        ),
        on=["Year", "Sport"],
        how="left",
    )
    csy["sport_expansion_penalty"] = csy["sport_total_growth"].clip(lower=0)

    csy["z_medal_growth"] = csy.groupby("Year")["medal_growth"].transform(zscore_by_year)
    csy["z_gold_growth"] = csy.groupby("Year")["gold_growth"].transform(zscore_by_year)
    csy["z_above_recent_baseline"] = csy.groupby("Year")["above_recent_baseline"].transform(zscore_by_year)
    csy["z_sport_expansion_penalty"] = csy.groupby("Year")["sport_expansion_penalty"].transform(zscore_by_year)
    csy["olympic_leap_index"] = (
        csy["z_medal_growth"]
        + 1.5 * csy["z_gold_growth"]
        + csy["z_above_recent_baseline"]
        + 0.5 * csy["first_medal_bonus"]
        + 0.5 * csy["continuity_bonus"]
        - 0.5 * csy["z_sport_expansion_penalty"]
    )

    out = to_export_columns(csy)
    return select_export(
        out.sort_values("olympic_leap_index", ascending=False),
        [
            "noc",
            "iso3",
            "country_name",
            "sport",
            "year",
            "total_medals",
            "gold_medals",
            "prev_total_medals",
            "prev_gold_medals",
            "rolling3_prev_medals",
            "medal_growth",
            "gold_growth",
            "above_recent_baseline",
            "first_medal_bonus",
            "continuity_bonus",
            "sport_total_medals",
            "sport_total_growth",
            "sport_expansion_penalty",
            "olympic_leap_index",
        ],
    )


def build_leap_mechanism_decomposition(
    leap: pd.DataFrame,
    host_years: pd.DataFrame,
    event_opportunity_refined: pd.DataFrame,
    gender_event_expansion: pd.DataFrame,
    coach_events_template: pd.DataFrame,
    expected_vs_actual: pd.DataFrame,
) -> pd.DataFrame:
    """Attach candidate mechanism signals to each country-sport leap row."""
    out = leap.copy()
    out["olympic_leap_index"] = pd.to_numeric(out["olympic_leap_index"], errors="coerce")
    out = out.sort_values("olympic_leap_index", ascending=False).drop_duplicates(
        ["year", "noc", "sport"],
        keep="first",
    )
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    for column in [
        "olympic_leap_index",
        "total_medals",
        "medal_growth",
        "gold_growth",
        "first_medal_bonus",
        "continuity_bonus",
        "sport_total_medals",
        "sport_total_growth",
        "sport_expansion_penalty",
    ]:
        out[column] = pd.to_numeric(out[column], errors="coerce")

    host = host_years.copy()
    host["year"] = pd.to_numeric(host["year"], errors="coerce")
    out = out.merge(
        host[["year", "host_noc", "host_noc_compatible", "host_country"]],
        on="year",
        how="left",
    )
    out["host_effect_score"] = (
        out["noc"].eq(out["host_noc"]) | out["noc"].eq(out["host_noc_compatible"])
    ).astype(float)

    changes = event_opportunity_refined.copy()
    changes["year"] = pd.to_numeric(changes["year"], errors="coerce")
    added = changes[changes["true_new_medal_opportunity_flag"].eq(1)].groupby(["year", "sport"], as_index=False).agg(
        added_or_restored_events=("event", "nunique"),
        added_women_events=("sex", lambda s: (s == "women").sum()),
    )
    restored = changes[changes["restored_event_flag"].eq(1)].groupby(["year", "sport"], as_index=False).agg(
        restored_events=("event", "nunique"),
    )
    uncertain = changes[changes["refined_change_type"].eq("uncertain")].groupby(["year", "sport"], as_index=False).agg(
        uncertain_opportunity_events=("event", "nunique"),
    )
    out = out.merge(added, on=["year", "sport"], how="left")
    out = out.merge(restored, on=["year", "sport"], how="left")
    out = out.merge(uncertain, on=["year", "sport"], how="left")
    out["added_or_restored_events"] = out["added_or_restored_events"].fillna(0)
    out["added_women_events"] = out["added_women_events"].fillna(0)
    out["restored_events"] = out["restored_events"].fillna(0)
    out["uncertain_opportunity_events"] = out["uncertain_opportunity_events"].fillna(0)
    out["event_expansion_effect_score"] = np.clip(
        safe_divide(out["added_or_restored_events"], out["sport_total_medals"]) * 10,
        0,
        1,
    )

    gender = gender_event_expansion.copy()
    gender["year"] = pd.to_numeric(gender["year"], errors="coerce")
    women = gender[gender["sex_category"].eq("women")].sort_values(["sport", "year"]).copy()
    women["event_count"] = pd.to_numeric(women["event_count"], errors="coerce")
    women["prev_women_event_count"] = women.groupby("sport")["event_count"].shift(1)
    women["women_event_growth"] = women["event_count"] - women["prev_women_event_count"]
    out = out.merge(
        women[["year", "sport", "event_count", "women_event_growth"]].rename(
            columns={"event_count": "women_event_count"}
        ),
        on=["year", "sport"],
        how="left",
    )
    out["female_expansion_effect_score"] = np.where(
        pd.to_numeric(out["women_event_growth"], errors="coerce").fillna(0) > 0,
        min_max_scale(out["women_event_growth"].fillna(0)),
        0,
    )
    out["female_expansion_effect_score"] = pd.to_numeric(
        out["female_expansion_effect_score"], errors="coerce"
    ).fillna(0).clip(0, 1)

    coach = coach_events_template.copy()
    if not coach.empty and {"country_name", "sport", "start_year", "end_year"}.issubset(coach.columns):
        coach["start_year"] = pd.to_numeric(coach["start_year"], errors="coerce")
        coach["end_year"] = pd.to_numeric(coach["end_year"], errors="coerce").fillna(9999)
        coach_rows = []
        for _, row in out[["year", "country_name", "sport"]].drop_duplicates().iterrows():
            matches = coach[
                coach["country_name"].eq(row["country_name"])
                & coach["sport"].eq(row["sport"])
                & (coach["start_year"] <= row["year"])
                & (coach["end_year"] >= row["year"] - 4)
            ]
            if matches.empty:
                evidence = "no_coach_evidence_in_template"
                score = 0.0
                source_note = ""
            else:
                evidence = str(matches["evidence_level"].iloc[0]) if "evidence_level" in matches.columns else "unverified"
                score = 1.0 if evidence == "official_source" else (0.7 if evidence == "secondary_source" else 0.4)
                source_note = str(matches["source_note"].iloc[0]) if "source_note" in matches.columns else ""
            coach_rows.append(
                {
                    "year": row["year"],
                    "country_name": row["country_name"],
                    "sport": row["sport"],
                    "coach_effect_score": score,
                    "coach_evidence_level": evidence,
                    "coach_source_note": source_note,
                }
            )
        coach_signal = pd.DataFrame(coach_rows)
    else:
        coach_signal = out[["year", "country_name", "sport"]].drop_duplicates().copy()
        coach_signal["coach_effect_score"] = 0.0
        coach_signal["coach_evidence_level"] = "no_coach_evidence_in_template"
        coach_signal["coach_source_note"] = ""
    out = out.merge(coach_signal, on=["year", "country_name", "sport"], how="left")

    expected = expected_vs_actual.copy()
    expected["year"] = pd.to_numeric(expected["year"], errors="coerce")
    expected = expected.drop_duplicates(["year", "noc", "country_name"], keep="first")
    expected = expected.sort_values(["country_name", "year"])
    expected["medal_overperformance"] = pd.to_numeric(expected["medal_overperformance"], errors="coerce")
    expected["prev_medal_overperformance"] = expected.groupby("country_name")["medal_overperformance"].shift(1)
    expected["economic_overperformance_growth"] = expected["medal_overperformance"] - expected["prev_medal_overperformance"]
    out = out.merge(
        expected[["year", "noc", "country_name", "medal_overperformance", "economic_overperformance_growth", "model_note"]],
        on=["year", "noc", "country_name"],
        how="left",
    )
    out["economic_growth_effect_score"] = min_max_scale(
        pd.to_numeric(out["economic_overperformance_growth"], errors="coerce").clip(lower=0)
    ).fillna(0).clip(0, 1)

    out["continuity_score"] = pd.to_numeric(out["continuity_bonus"], errors="coerce").fillna(0).clip(0, 1)
    out["one_off_spike_risk_score"] = np.clip(
        (
            (1 - out["continuity_score"])
            + pd.to_numeric(out["first_medal_bonus"], errors="coerce").fillna(0) * 0.5
            + (pd.to_numeric(out["sport_expansion_penalty"], errors="coerce").fillna(0) > 0).astype(float) * 0.25
        )
        / 1.75,
        0,
        1,
    )

    mechanism_columns = [
        "host_effect_score",
        "event_expansion_effect_score",
        "female_expansion_effect_score",
        "coach_effect_score",
        "economic_growth_effect_score",
        "continuity_score",
    ]
    label_map = {
        "host_effect_score": "host_effect",
        "event_expansion_effect_score": "event_expansion_effect",
        "female_expansion_effect_score": "female_expansion_effect",
        "coach_effect_score": "coach_effect",
        "economic_growth_effect_score": "economic_growth_effect",
        "continuity_score": "continuity_effect",
    }
    out[mechanism_columns] = out[mechanism_columns].fillna(0)
    out["primary_mechanism_label"] = out[mechanism_columns].idxmax(axis=1).map(label_map)
    out.loc[out[mechanism_columns].max(axis=1) <= 0, "primary_mechanism_label"] = "unexplained_by_current_tables"
    out.loc[out["one_off_spike_risk_score"] >= 0.65, "primary_mechanism_label"] = "one_off_spike_risk"

    def explain(row: pd.Series) -> str:
        parts = []
        if row["host_effect_score"] > 0:
            parts.append("host-year overlap")
        if row["event_expansion_effect_score"] > 0:
            parts.append(f"{int(row['added_or_restored_events'])} added/restored sport events")
        if row["female_expansion_effect_score"] > 0:
            parts.append("women's event expansion")
        if row["coach_effect_score"] > 0:
            parts.append(f"coach/program signal from template ({row['coach_evidence_level']})")
        if row["economic_growth_effect_score"] > 0:
            parts.append("improved country-year overperformance versus economic baseline")
        if row["continuity_score"] > 0:
            parts.append("medal continuity into next Games")
        if row["one_off_spike_risk_score"] >= 0.65:
            parts.append("high one-off spike risk")
        if not parts:
            parts.append("no strong mechanism signal in current structured tables")
        return "Candidate explanation: " + "; ".join(parts) + ". Not a causal claim."

    out["mechanism_explanation_text"] = out.apply(explain, axis=1)
    out["mechanism_evidence_level"] = np.where(
        out["coach_effect_score"] > 0,
        out["coach_evidence_level"],
        "derived_from_project_tables",
    )
    return select_export(
        out,
        [
            "year",
            "country_name",
            "noc",
            "iso3",
            "sport",
            "total_medals",
            "gold_medals",
            "medal_growth",
            "gold_growth",
            "olympic_leap_index",
            "host_effect_score",
            "event_expansion_effect_score",
            "female_expansion_effect_score",
            "coach_effect_score",
            "economic_growth_effect_score",
            "continuity_score",
            "one_off_spike_risk_score",
            "primary_mechanism_label",
            "mechanism_explanation_text",
            "mechanism_evidence_level",
            "coach_evidence_level",
            "coach_source_note",
            "added_or_restored_events",
            "added_women_events",
            "restored_events",
            "uncertain_opportunity_events",
            "women_event_growth",
            "medal_overperformance",
            "economic_overperformance_growth",
        ],
    )


def build_leap_mechanism_enhanced(
    leap_mechanism: pd.DataFrame,
    expected_vs_actual: pd.DataFrame,
    economic_efficiency: pd.DataFrame,
) -> pd.DataFrame:
    """Build an evidence-tiered interpretation layer for country-sport leap rows.

    This table intentionally separates descriptive leap evidence, internal
    candidate mechanisms, internal comparative support and external evidence.
    Continuity and one-off spike scores are treated as supporting/risk signals,
    not primary mechanisms.
    """
    out = leap_mechanism.copy()
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    score_columns = [
        "host_effect_score",
        "event_expansion_effect_score",
        "female_expansion_effect_score",
        "coach_effect_score",
        "economic_growth_effect_score",
        "continuity_score",
        "one_off_spike_risk_score",
        "added_or_restored_events",
        "added_women_events",
        "women_event_growth",
        "medal_overperformance",
        "economic_overperformance_growth",
    ]
    for column in score_columns:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0)

    expected = expected_vs_actual.copy()
    expected["year"] = pd.to_numeric(expected["year"], errors="coerce")
    expected = expected.drop_duplicates(["year", "noc", "country_name"], keep="first")
    expected_keep = [
        "year",
        "noc",
        "country_name",
        "expected_medals",
        "gdp_total",
        "population",
        "gdp_per_capita",
        "model_note",
    ]
    out = out.merge(
        expected[[column for column in expected_keep if column in expected.columns]],
        on=["year", "noc", "country_name"],
        how="left",
        suffixes=("", "_expected"),
    )

    economic = economic_efficiency.copy()
    economic["year"] = pd.to_numeric(economic["year"], errors="coerce")
    economic = economic.drop_duplicates(["year", "noc", "country_name"], keep="first")
    economic_keep = [
        "year",
        "noc",
        "country_name",
        "has_population_match",
        "has_gdp_per_capita_match",
        "has_gdp_total_match",
        "economic_data_complete",
    ]
    out = out.merge(
        economic[[column for column in economic_keep if column in economic.columns]],
        on=["year", "noc", "country_name"],
        how="left",
    )

    for column in [
        "has_population_match",
        "has_gdp_per_capita_match",
        "has_gdp_total_match",
        "economic_data_complete",
    ]:
        if column not in out.columns:
            out[column] = 0
        out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0).astype(int)

    out["economic_data_is_complete"] = out["economic_data_complete"].eq(1)
    out["economic_strong_claim_blocked"] = (
        out["year"].eq(2024)
        & (out["has_gdp_total_match"].eq(0) | out["has_population_match"].eq(0))
    )

    mechanism_score_columns = [
        "host_effect_score",
        "event_expansion_effect_score",
        "female_expansion_effect_score",
        "coach_effect_score",
        "economic_growth_effect_score",
    ]

    def has_signal(row: pd.Series, column: str, threshold: float = 0.15) -> bool:
        if column == "host_effect_score":
            return float(row.get(column, 0)) > 0
        if column == "coach_effect_score":
            return float(row.get(column, 0)) > 0 and row.get("coach_evidence_level") != "no_coach_evidence_in_template"
        if column == "economic_growth_effect_score":
            return (
                float(row.get(column, 0)) >= threshold
                and not bool(row.get("economic_strong_claim_blocked", False))
                and bool(row.get("economic_data_is_complete", False))
            )
        return float(row.get(column, 0)) >= threshold

    def mechanism_signals(row: pd.Series) -> dict[str, float]:
        labels = {
            "host_effect": float(row.get("host_effect_score", 0)) if has_signal(row, "host_effect_score") else 0.0,
            "event_expansion_effect": float(row.get("event_expansion_effect_score", 0))
            if has_signal(row, "event_expansion_effect_score")
            else 0.0,
            "female_expansion_effect": float(row.get("female_expansion_effect_score", 0))
            if has_signal(row, "female_expansion_effect_score")
            else 0.0,
            "economic_overperformance": float(row.get("economic_growth_effect_score", 0))
            if has_signal(row, "economic_growth_effect_score")
            else 0.0,
            "coach_evidence_supported": float(row.get("coach_effect_score", 0))
            if has_signal(row, "coach_effect_score")
            else 0.0,
        }
        return labels

    def pick_mechanism(row: pd.Series) -> str:
        signals = mechanism_signals(row)
        positive = {label: score for label, score in signals.items() if score > 0}
        if not positive:
            return "unexplained_by_current_tables"
        if len(positive) >= 2:
            top_scores = sorted(positive.values(), reverse=True)
            if top_scores[1] >= max(0.2, top_scores[0] * 0.55):
                return "mixed_mechanism"
        return max(positive, key=positive.get)

    out["candidate_primary_mechanism"] = out.apply(pick_mechanism, axis=1)

    def host_evidence(row: pd.Series) -> str:
        if float(row.get("host_effect_score", 0)) <= 0:
            return "none"
        host_country = row.get("host_country")
        if pd.isna(host_country) or str(host_country).strip() == "":
            return "internal_data_candidate: same-year host NOC match; host country mapping incomplete"
        return f"internal_data_candidate: {int(row['year'])} host-year overlap with {host_country}"

    def event_expansion_evidence(row: pd.Series) -> str:
        count = int(round(float(row.get("added_or_restored_events", 0))))
        if count <= 0:
            return "none"
        score = float(row.get("event_expansion_effect_score", 0))
        return f"internal_comparative_support: {count} added/restored {row['sport']} events in the same Games; score={score:.2f}"

    def female_expansion_evidence(row: pd.Series) -> str:
        added_women = int(round(float(row.get("added_women_events", 0))))
        growth = float(row.get("women_event_growth", 0))
        if added_women <= 0 and growth <= 0:
            return "none"
        return f"internal_data_candidate: women's {row['sport']} opportunities expanded by {growth:.0f} events; added women events={added_women}"

    def economic_evidence(row: pd.Series) -> str:
        if bool(row.get("economic_strong_claim_blocked", False)):
            return "limited: 2024 GDP total or population missing, so no strong economic interpretation"
        if not bool(row.get("economic_data_is_complete", False)):
            return "limited: incomplete GDP/population match, economic baseline is weak"
        growth = float(row.get("economic_overperformance_growth", 0))
        if growth <= 0:
            return "none"
        model_note = row.get("model_note")
        return f"internal_data_candidate: medal overperformance improved by {growth:.2f} versus descriptive economic baseline ({model_note})"

    def coach_evidence(row: pd.Series) -> str:
        level = row.get("coach_evidence_level")
        score = float(row.get("coach_effect_score", 0))
        if score <= 0 or level == "no_coach_evidence_in_template":
            return "none: no curated coach evidence in coach_events_template"
        source = str(row.get("coach_source_note", "")).strip()
        suffix = f"; source={source}" if source else ""
        return f"external_evidence_support: coach/program template row with evidence_level={level}{suffix}"

    out["host_evidence"] = out.apply(host_evidence, axis=1)
    out["event_expansion_evidence"] = out.apply(event_expansion_evidence, axis=1)
    out["female_expansion_evidence"] = out.apply(female_expansion_evidence, axis=1)
    out["economic_evidence"] = out.apply(economic_evidence, axis=1)
    out["coach_evidence"] = out.apply(coach_evidence, axis=1)

    def continuity_signal(row: pd.Series) -> str:
        if float(row.get("continuity_score", 0)) > 0:
            return "supporting_signal: medals continued in the next observed Games"
        return "no_continuity_signal: no medal in the next observed Games"

    def one_off_risk(row: pd.Series) -> str:
        score = float(row.get("one_off_spike_risk_score", 0))
        if score >= 0.65:
            return f"high: score={score:.2f}"
        if score >= 0.35:
            return f"medium: score={score:.2f}"
        return f"low: score={score:.2f}"

    out["continuity_signal"] = out.apply(continuity_signal, axis=1)
    out["one_off_spike_risk"] = out.apply(one_off_risk, axis=1)

    def supporting_signals(row: pd.Series) -> str:
        signals = []
        if float(row.get("continuity_score", 0)) > 0:
            signals.append("post-leap medal continuity")
        if float(row.get("medal_overperformance", 0)) > 0 and bool(row.get("economic_data_is_complete", False)):
            signals.append("positive medal overperformance versus economic baseline")
        if float(row.get("gold_growth", 0)) > 0:
            signals.append("gold medal growth")
        return "; ".join(signals) if signals else "none"

    def risk_flags(row: pd.Series) -> str:
        flags = []
        if float(row.get("one_off_spike_risk_score", 0)) >= 0.65:
            flags.append("high_one_off_spike_risk")
        if bool(row.get("economic_strong_claim_blocked", False)):
            flags.append("economic_claim_limited_by_2024_missing_gdp_total_or_population")
        elif not bool(row.get("economic_data_is_complete", False)):
            flags.append("economic_data_incomplete")
        if row.get("coach_evidence_level") == "no_coach_evidence_in_template":
            flags.append("no_coach_external_evidence")
        return "; ".join(flags) if flags else "none"

    out["supporting_signals"] = out.apply(supporting_signals, axis=1)
    out["risk_flags"] = out.apply(risk_flags, axis=1)

    def alternative_explanations(row: pd.Series) -> str:
        alternatives = [
            "athlete cohort change",
            "competition field changes",
            "selection or qualification changes",
        ]
        if float(row.get("added_or_restored_events", 0)) == 0:
            alternatives.append("event mix not captured by added/restored table")
        if row.get("coach_evidence_level") == "no_coach_evidence_in_template":
            alternatives.append("unobserved coaching or program changes")
        if float(row.get("one_off_spike_risk_score", 0)) >= 0.65:
            alternatives.append("short-run spike rather than sustained power shift")
        return "; ".join(alternatives)

    out["alternative_explanations"] = out.apply(alternative_explanations, axis=1)

    def evidence_stack(row: pd.Series) -> str:
        stack = [
            f"descriptive_phenomenon: Olympic Leap Index {float(row.get('olympic_leap_index', 0)):.2f}, medal growth {float(row.get('medal_growth', 0)):.0f}",
        ]
        if row["host_evidence"] != "none":
            stack.append(row["host_evidence"])
        if row["event_expansion_evidence"] != "none":
            stack.append(row["event_expansion_evidence"])
        if row["female_expansion_evidence"] != "none":
            stack.append(row["female_expansion_evidence"])
        if row["economic_evidence"] != "none":
            stack.append(row["economic_evidence"])
        if not str(row["coach_evidence"]).startswith("none"):
            stack.append(row["coach_evidence"])
        stack.append(row["continuity_signal"])
        if float(row.get("one_off_spike_risk_score", 0)) >= 0.65:
            stack.append(f"risk_flag: {row['one_off_spike_risk']}")
        return " | ".join(stack)

    out["evidence_stack"] = out.apply(evidence_stack, axis=1)

    def evidence_level(row: pd.Series) -> str:
        mechanism = row.get("candidate_primary_mechanism")
        if mechanism == "coach_evidence_supported":
            return "external_evidence_supported"
        if mechanism in {"event_expansion_effect", "mixed_mechanism"} and float(row.get("added_or_restored_events", 0)) > 0:
            return "internal_comparative_support"
        if mechanism == "unexplained_by_current_tables":
            return "descriptive_only"
        return "internal_data_candidate"

    out["evidence_level"] = out.apply(evidence_level, axis=1)

    def confidence_level(row: pd.Series) -> str:
        mechanism = row.get("candidate_primary_mechanism")
        risk = float(row.get("one_off_spike_risk_score", 0))
        positive_signals = sum(1 for value in mechanism_signals(row).values() if value > 0)
        if mechanism == "unexplained_by_current_tables" or risk >= 0.75:
            return "low"
        if row.get("evidence_level") == "external_evidence_supported" and risk < 0.65:
            return "high"
        if row.get("evidence_level") == "internal_comparative_support" and positive_signals >= 2 and risk < 0.65:
            return "medium_high"
        if positive_signals >= 1 and risk < 0.65:
            return "medium"
        return "low"

    out["confidence_level"] = out.apply(confidence_level, axis=1)

    def interpretation_text(row: pd.Series) -> str:
        mechanism = row.get("candidate_primary_mechanism")
        country = row.get("country_name")
        year = int(row.get("year")) if pd.notna(row.get("year")) else row.get("year")
        sport = row.get("sport")
        leap = float(row.get("olympic_leap_index", 0))
        opening = (
            f"{country}'s {year} {sport} result is a descriptive leap case "
            f"(index {leap:.2f}, {int(row.get('total_medals', 0))} medals)."
        )
        if mechanism == "host_effect":
            mechanism_text = "The strongest internal candidate mechanism is host-year overlap."
        elif mechanism == "event_expansion_effect":
            mechanism_text = "The strongest internal candidate mechanism is same-Games sport event expansion."
        elif mechanism == "female_expansion_effect":
            mechanism_text = "The strongest internal candidate mechanism is expansion of women's medal opportunities."
        elif mechanism == "economic_overperformance":
            mechanism_text = "The strongest internal candidate mechanism is improved medal overperformance versus a descriptive economic baseline."
        elif mechanism == "coach_evidence_supported":
            mechanism_text = "A coach/program mechanism is supported only because a curated external-evidence row is present."
        elif mechanism == "mixed_mechanism":
            mechanism_text = "No single mechanism dominates; multiple internal signals should be read together."
        else:
            mechanism_text = "Current structured tables do not identify a strong candidate mechanism."
        return (
            f"{opening} {mechanism_text} Supporting signals: {row['supporting_signals']}. "
            f"Risks/limits: {row['risk_flags']}. This is mechanism interpretation, not a causal estimate."
        )

    out["interpretation_text"] = out.apply(interpretation_text, axis=1)
    out["interpretation_scope_note"] = (
        "Scope: descriptive anomaly plus candidate mechanism triage from project tables. "
        "This is not a causal claim; coach claims require curated external evidence; "
        "economic claims are limited when GDP total or population is missing."
    )

    return select_export(
        out,
        [
            "year",
            "country_name",
            "noc",
            "sport",
            "total_medals",
            "gold_medals",
            "olympic_leap_index",
            "candidate_primary_mechanism",
            "confidence_level",
            "evidence_level",
            "evidence_stack",
            "supporting_signals",
            "risk_flags",
            "alternative_explanations",
            "host_evidence",
            "event_expansion_evidence",
            "female_expansion_evidence",
            "economic_evidence",
            "coach_evidence",
            "continuity_signal",
            "one_off_spike_risk",
            "interpretation_text",
            "interpretation_scope_note",
        ],
    )


def build_leap_case_library(leap_mechanism: pd.DataFrame) -> pd.DataFrame:
    """Create a compact case library for mechanism-analysis cards."""
    df = leap_mechanism.copy()
    df["olympic_leap_index"] = pd.to_numeric(df["olympic_leap_index"], errors="coerce")
    target_countries = [
        "China",
        "United States",
        "United Kingdom",
        "Japan",
        "South Korea",
        "Kenya",
        "Jamaica",
        "Kyrgyzstan",
        "North Korea",
        "Ethiopia",
    ]
    selected = []
    for country in target_countries:
        rows = df[df["country_name"].eq(country)].sort_values("olympic_leap_index", ascending=False).head(2)
        selected.append(rows)
    top_candidates = df.sort_values("olympic_leap_index", ascending=False).head(30)
    selected.append(top_candidates)
    cases = pd.concat(selected, ignore_index=True).drop_duplicates(["year", "noc", "sport"])
    cases = cases.sort_values("olympic_leap_index", ascending=False).head(40).copy()
    cases["case_id"] = cases.apply(
        lambda row: f"{int(row['year'])}_{row['noc']}_{re.sub(r'[^0-9A-Za-z]+', '_', str(row['sport'])).strip('_')}",
        axis=1,
    )
    cases["case_title"] = cases.apply(
        lambda row: f"{row['country_name']} {int(row['year'])} {row['sport']} leap",
        axis=1,
    )
    cases["case_priority"] = cases["olympic_leap_index"].rank(method="first", ascending=False).astype(int)
    cases["case_card_prompt"] = (
        "Review this as a candidate mechanism case. Separate descriptive leap evidence from possible mechanisms and add external sources before making stronger claims."
    )
    cases["recommended_next_research"] = np.where(
        cases["coach_effect_score"].fillna(0).eq(0),
        "Check federation/Olympic committee sources for coaching, funding, selection or program changes.",
        "Verify the coach/program source note and compare with event expansion and host context.",
    )
    return select_export(
        cases,
        [
            "case_id",
            "case_priority",
            "case_title",
            "year",
            "country_name",
            "noc",
            "sport",
            "total_medals",
            "gold_medals",
            "medal_growth",
            "gold_growth",
            "olympic_leap_index",
            "primary_mechanism_label",
            "mechanism_explanation_text",
            "mechanism_evidence_level",
            "host_effect_score",
            "event_expansion_effect_score",
            "female_expansion_effect_score",
            "coach_effect_score",
            "economic_growth_effect_score",
            "continuity_score",
            "one_off_spike_risk_score",
            "case_card_prompt",
            "recommended_next_research",
        ],
    )


def build_mechanism_case_cards(
    leap_enhanced: pd.DataFrame,
    host_panel: pd.DataFrame,
    gender_growth: pd.DataFrame,
    sport_barrier_modern: pd.DataFrame,
    economic_efficiency: pd.DataFrame,
) -> pd.DataFrame:
    """Create curated, front-end ready mechanism explanation cards."""
    leap = leap_enhanced.copy()
    host = host_panel.copy()
    gender = gender_growth.copy()
    barrier = sport_barrier_modern.copy()
    economic = economic_efficiency.copy()

    for df, column in [(leap, "year"), (host, "host_year"), (economic, "year")]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    for column in ["olympic_leap_index", "total_medals", "gold_medals"]:
        if column in leap.columns:
            leap[column] = pd.to_numeric(leap[column], errors="coerce")
    for column in ["host_lift_vs_baseline", "host_lift_vs_peers", "pre_host_medals", "host_year_medals", "post_host_medals"]:
        if column in host.columns:
            host[column] = pd.to_numeric(host[column], errors="coerce")
    for column in ["breakthrough_friendliness_index_modern", "entry_barrier_index_modern"]:
        if column in barrier.columns:
            barrier[column] = pd.to_numeric(barrier[column], errors="coerce")
    for column in ["total_medals", "gold_medals", "medals_per_million_people", "medals_per_100b_gdp"]:
        if column in economic.columns:
            economic[column] = pd.to_numeric(economic[column], errors="coerce")

    def first(df: pd.DataFrame) -> pd.Series | None:
        return None if df.empty else df.iloc[0]

    def fmt(value: Any, digits: int = 2) -> str:
        value = pd.to_numeric(value, errors="coerce")
        if pd.isna(value):
            return "NA"
        return str(int(value)) if float(value).is_integer() else f"{float(value):.{digits}f}"

    def conf(value: Any, fallback: str = "medium") -> str:
        text = str(value or "").lower()
        if "high" in text and "medium_high" not in text:
            return "high"
        if "medium" in text:
            return "medium"
        if "low" in text:
            return "low"
        return fallback

    def leap_row(country: str, sport: str, year: int | None = None) -> pd.Series | None:
        rows = leap[leap["country_name"].eq(country) & leap["sport"].eq(sport)].copy()
        if year is not None:
            rows = rows[rows["year"].eq(year)]
        return first(rows.sort_values("olympic_leap_index", ascending=False))

    def host_row(country: str, sport: str, year: int) -> pd.Series | None:
        rows = host[host["host_country"].eq(country) & host["sport"].eq(sport) & host["host_year"].eq(year)].copy()
        return first(rows.sort_values("host_lift_vs_baseline", ascending=False))

    def barrier_row(sport: str) -> pd.Series | None:
        return first(barrier[barrier["sport"].eq(sport)])

    def econ_row(country: str, year: int) -> pd.Series | None:
        rows = economic[economic["country_name"].eq(country) & economic["year"].eq(year)].copy()
        return first(rows.sort_values(["medals_per_million_people", "total_medals"], ascending=False))

    cards: list[dict[str, Any]] = []

    def add_card(
        case_id: str,
        case_title: str,
        country_name: str,
        noc: str,
        sport: str,
        year_or_period: str,
        phenomenon_summary: str,
        key_metrics: str,
        candidate_mechanism: str,
        evidence_stack: str,
        alternative_explanations: str,
        confidence_level: str,
        external_evidence_needed: str,
        chart_recommendation: str,
        frontend_card_text: str,
    ) -> None:
        cards.append(
            {
                "case_id": case_id,
                "case_title": case_title,
                "country_name": country_name,
                "noc": noc,
                "sport": sport,
                "year_or_period": year_or_period,
                "phenomenon_summary": phenomenon_summary,
                "key_metrics": key_metrics,
                "candidate_mechanism": candidate_mechanism,
                "evidence_stack": evidence_stack,
                "alternative_explanations": alternative_explanations,
                "confidence_level": confidence_level,
                "external_evidence_needed": external_evidence_needed,
                "chart_recommendation": chart_recommendation,
                "frontend_card_text": frontend_card_text,
            }
        )

    lr = leap_row("China", "Gymnastics", 2008)
    hr = host_row("China", "Gymnastics", 2008)
    br = barrier_row("Artistic Gymnastics")
    if lr is not None and hr is not None:
        add_card(
            "china_2008_gymnastics_host",
            "China 2008 Gymnastics: host-year leap candidate",
            "China",
            "CHN",
            "Gymnastics",
            "2008",
            f"China won {fmt(lr.get('total_medals'))} gymnastics medals with a high leap index.",
            f"leap_index={fmt(lr.get('olympic_leap_index'))}; host_lift_vs_baseline={fmt(hr.get('host_lift_vs_baseline'))}; host_lift_vs_peers={fmt(hr.get('host_lift_vs_peers'))}; modern_barrier={fmt(br.get('entry_barrier_index_modern')) if br is not None else 'NA'}",
            "host_effect candidate with sport-specific lift evidence",
            f"{lr.get('evidence_stack')} | host_panel: pre={fmt(hr.get('pre_host_medals'))}, host={fmt(hr.get('host_year_medals'))}, post={fmt(hr.get('post_host_medals'))}",
            str(lr.get("alternative_explanations")),
            conf(lr.get("confidence_level")),
            "Need external sources on host preparation, funding, selection, coaching and athlete pipeline before attributing the leap.",
            "Pre-host-post line chart plus peer-adjusted host-lift bar.",
            "China 2008 Gymnastics is a strong internal host-year candidate, but the data supports candidate evidence rather than a causal claim.",
        )

    diving = leap[leap["country_name"].eq("China") & leap["sport"].eq("Diving")].copy()
    br = barrier_row("Diving")
    if not diving.empty:
        modern_total = diving[diving["year"].between(1992, 2024)]["total_medals"].sum()
        best = diving.sort_values("olympic_leap_index", ascending=False).iloc[0]
        add_card(
            "china_diving_long_term_advantage",
            "China Diving: repeated advantage in a high-barrier sport",
            "China",
            "CHN",
            "Diving",
            "1992-2024",
            "China shows repeated modern Diving medal production rather than a single isolated spike.",
            f"modern_medals_sum={fmt(modern_total)}; best_leap_year={int(best['year'])}; best_leap_index={fmt(best.get('olympic_leap_index'))}; modern_entry_barrier={fmt(br.get('entry_barrier_index_modern')) if br is not None else 'NA'}",
            "sustained specialization / high-barrier incumbent advantage candidate",
            f"sport_barrier: {br.get('modern_sport_type') if br is not None else 'NA'} | best_leap_evidence: {best.get('evidence_stack')}",
            "Event mix, judging structure, athlete cohort and program investment are not separated by current tables.",
            "medium",
            "Need external evidence on training system, selection depth, coaching pipeline and federation strategy.",
            "Modern medal timeline with high-barrier badge.",
            "China Diving is best presented as sustained specialization in a modern high-barrier sport, not as a single causal mechanism.",
        )

    uk = leap[leap["country_name"].eq("United Kingdom") & leap["sport"].str.contains("Cycling", na=False)].copy()
    uk = uk[uk["year"].isin([2008, 2012])].sort_values("olympic_leap_index", ascending=False)
    br = barrier_row("Cycling Track")
    if not uk.empty:
        row = uk.iloc[0]
        add_card(
            "uk_2008_2012_cycling",
            "United Kingdom 2008/2012 Cycling: performance-cycle candidate",
            "United Kingdom",
            "GBR",
            str(row.get("sport")),
            "2008/2012",
            "United Kingdom cycling appears as a high-performing case across the Beijing-London cycle.",
            f"best_year={int(row['year'])}; leap_index={fmt(row.get('olympic_leap_index'))}; medals={fmt(row.get('total_medals'))}; cycling_track_barrier={fmt(br.get('entry_barrier_index_modern')) if br is not None else 'NA'}",
            "program-cycle / specialization candidate, no coach causal claim",
            str(row.get("evidence_stack")),
            str(row.get("alternative_explanations")),
            conf(row.get("confidence_level"), "medium"),
            "Need external evidence on funding cycle, coaching staff, technology, athlete pipeline and selection policy.",
            "Cycling medal timeline for 2004/2008/2012/2016.",
            "The data supports a candidate performance-cycle story for UK Cycling; the mechanism needs external program evidence.",
        )

    lr = leap_row("Japan", "Judo", 2020)
    hr = host_row("Japan", "Judo", 2020)
    br = barrier_row("Judo")
    if lr is not None:
        add_card(
            "japan_2020_judo_host",
            "Japan 2020 Judo: host-year strength candidate",
            "Japan",
            "JPN",
            "Judo",
            "2020",
            f"Japan won {fmt(lr.get('total_medals'))} Judo medals in the Tokyo Games period.",
            f"leap_index={fmt(lr.get('olympic_leap_index'))}; medals={fmt(lr.get('total_medals'))}; host_lift_vs_baseline={fmt(hr.get('host_lift_vs_baseline')) if hr is not None else 'NA'}; modern_friendliness={fmt(br.get('breakthrough_friendliness_index_modern')) if br is not None else 'NA'}",
            "host_effect / incumbent-strength candidate",
            str(lr.get("evidence_stack")) + (f" | host_panel: {hr.get('host_effect_strength')}" if hr is not None else ""),
            str(lr.get("alternative_explanations")),
            conf(lr.get("confidence_level")),
            "Need external evidence on home conditions, selection depth, cohort strength and federation preparation.",
            "Pre-host-post Judo medal line with host annotation.",
            "Japan 2020 Judo combines host-year context with incumbent sport strength; the data does not isolate a causal source.",
        )

    sk = leap[leap["country_name"].eq("South Korea") & leap["sport"].eq("Archery")].sort_values("olympic_leap_index", ascending=False)
    br = barrier_row("Archery")
    if not sk.empty:
        best = sk.iloc[0]
        modern_total = sk[sk["year"].between(1992, 2024)]["total_medals"].sum()
        add_card(
            "south_korea_archery_incumbent",
            "South Korea Archery: high-barrier incumbent case",
            "South Korea",
            "KOR",
            "Archery",
            "1992-2024",
            "South Korea repeatedly appears in Archery medal rows while modern Archery is high-barrier.",
            f"modern_medals_sum={fmt(modern_total)}; best_year={int(best['year'])}; best_leap_index={fmt(best.get('olympic_leap_index'))}; modern_entry_barrier={fmt(br.get('entry_barrier_index_modern')) if br is not None else 'NA'}",
            "incumbent advantage / specialization candidate",
            f"sport_barrier: {br.get('modern_sport_type') if br is not None else 'NA'} | best_leap_evidence: {best.get('evidence_stack')}",
            "Current tables do not identify coaching, selection, school-system or federation mechanisms.",
            "medium",
            "Need external evidence on selection system, domestic competition, coaching depth and federation investment.",
            "Modern Archery medal timeline with barrier-index badge.",
            "South Korea Archery is a strong case for sustained dominance in a high-barrier sport, not proof of one program feature.",
        )

    for country, noc, year, case_id, title in [
        ("Jamaica", "JAM", 2008, "jamaica_2008_athletics", "Jamaica 2008 Athletics: leap in a broad modern sport"),
        ("Ethiopia", "ETH", 2000, "ethiopia_2000_athletics", "Ethiopia 2000 Athletics: endurance-country leap candidate"),
    ]:
        lr = leap_row(country, "Athletics", year)
        br = barrier_row("Athletics")
        if lr is not None:
            add_card(
                case_id,
                title,
                country,
                noc,
                "Athletics",
                str(year),
                f"{country} won {fmt(lr.get('total_medals'))} Athletics medals with a positive leap signal.",
                f"leap_index={fmt(lr.get('olympic_leap_index'))}; medals={fmt(lr.get('total_medals'))}; athletics_friendliness={fmt(br.get('breakthrough_friendliness_index_modern')) if br is not None else 'NA'}",
                str(lr.get("candidate_primary_mechanism")),
                str(lr.get("evidence_stack")),
                str(lr.get("alternative_explanations")),
                conf(lr.get("confidence_level")),
                "Need external evidence on event specialization, athlete cohort, coaching, domestic competition and federation support.",
                "Country-sport leap card plus Athletics modern friendliness gauge.",
                f"{country} {year} Athletics is a descriptive leap case in a broad modern sport; current data does not prove why the leap occurred.",
            )

    lr = leap_row("France", "Swimming", 2024)
    hr = host_row("France", "Swimming", 2024)
    if lr is not None and hr is not None:
        add_card(
            "france_2024_swimming_host",
            "France 2024 Swimming: host-related candidate with missing post window",
            "France",
            "FRA",
            "Swimming",
            "2024",
            f"France won {fmt(lr.get('total_medals'))} Swimming medals in 2024 with a strong pre-to-host lift.",
            f"leap_index={fmt(lr.get('olympic_leap_index'))}; host_lift_vs_baseline={fmt(hr.get('host_lift_vs_baseline'))}; host_lift_vs_peers={fmt(hr.get('host_lift_vs_peers'))}; post_year_missing=1",
            "host_effect candidate, limited by missing post-Games comparison",
            str(lr.get("evidence_stack")) + f" | host_panel: {hr.get('host_effect_strength')}",
            str(lr.get("alternative_explanations")),
            "medium",
            "Need post-2024 comparison plus external federation/program evidence.",
            "Pre-host-post panel with post side marked unavailable; peer-adjusted lift bar.",
            "France 2024 Swimming is a host-related candidate, but confidence is capped because the post-Games window is unavailable.",
        )

    for country, noc, year, case_id, title, text in [
        ("Bermuda", "BER", 2020, "bermuda_2020_small_country_efficiency", "Bermuda 2020: small-country medal efficiency case", "Bermuda 2020 should be shown as an efficiency outlier with small-sample caveats."),
        ("San Marino", "SMR", 2020, "san_marino_2020_small_country_efficiency", "San Marino 2020: multi-medal small-country efficiency case", "San Marino 2020 is a clear normalized-efficiency outlier, with small-denominator and data-completeness caveats."),
    ]:
        er = econ_row(country, year)
        if er is not None:
            add_card(
                case_id,
                title,
                country,
                noc,
                "All sports",
                str(year),
                f"{country} produced a notable small-country medal outcome in {year}.",
                f"total_medals={fmt(er.get('total_medals'))}; gold_medals={fmt(er.get('gold_medals'))}; medals_per_million={fmt(er.get('medals_per_million_people'))}; medals_per_100b_gdp={fmt(er.get('medals_per_100b_gdp'))}; economic_data_complete={er.get('economic_data_complete')}",
                "small-country efficiency / normalization outlier candidate",
                "country_economic_efficiency table: normalized medal rate highlights the case; economic data completeness limits interpretation.",
                "Small denominator effects, sport-specific pathway and single-cycle athlete cohort are not separated.",
                "low" if country == "Bermuda" else "medium",
                "Need sport-specific athlete pathway, qualification route, funding context and complete economic matches.",
                "Population-normalized efficiency scatterplot with small-sample warning.",
                text,
            )

    def enrich_case_cards(cards_df: pd.DataFrame) -> pd.DataFrame:
        presentation = {
            "china_2008_gymnastics_host": {
                "ready_for_presentation": "yes",
                "presentation_priority": 1,
                "risk_note": "Strong visual case, but host-year overlap is candidate evidence only; external preparation and selection evidence is still needed.",
                "short_title": "China 2008 Gymnastics",
                "one_sentence_takeaway": "China 2008 Gymnastics is a clear leap case with host-year and pre/host/post internal support.",
                "safest_claim": "China's 2008 Gymnastics result is a strong host-year candidate case supported by internal before/during/after comparison.",
                "not_allowed_claim": "The Beijing host effect caused China's gymnastics medals.",
                "evidence_summary_short": "Leap index 24.17; medals rose to 14; host panel shows pre=3, host=14, post=8.",
            },
            "china_diving_long_term_advantage": {
                "ready_for_presentation": "yes",
                "presentation_priority": 2,
                "risk_note": "Good for sustained dominance, but not for attributing the reason without external sport-system evidence.",
                "short_title": "China Diving 1992-2024",
                "one_sentence_takeaway": "China Diving is best used as a sustained high-barrier advantage case rather than a one-cycle spike.",
                "safest_claim": "The project tables show repeated modern Diving medal production by China in a high-barrier sport.",
                "not_allowed_claim": "China's diving dominance is caused by one specific training system shown in this dataset.",
                "evidence_summary_short": "Modern medal sum 83; high modern entry-barrier signal; repeated medals across modern Games.",
            },
            "jamaica_2008_athletics": {
                "ready_for_presentation": "yes",
                "presentation_priority": 3,
                "risk_note": "Good broad-sport leap example; external athlete/cohort and event-specialization evidence is still needed.",
                "short_title": "Jamaica 2008 Athletics",
                "one_sentence_takeaway": "Jamaica 2008 Athletics is a strong descriptive leap in a modern breakthrough-friendly sport.",
                "safest_claim": "Jamaica's 2008 Athletics row is a descriptive leap case with internal event-opportunity and overperformance signals.",
                "not_allowed_claim": "Female event expansion or coaching caused Jamaica's 2008 Athletics leap.",
                "evidence_summary_short": "Leap index 7.82; 9 Athletics medals; Athletics has high modern breakthrough-friendliness.",
            },
            "south_korea_archery_incumbent": {
                "ready_for_presentation": "yes",
                "presentation_priority": 4,
                "risk_note": "Useful as incumbent/high-barrier example; 2024 economic interpretation is limited by missing GDP/population matches.",
                "short_title": "South Korea Archery",
                "one_sentence_takeaway": "South Korea Archery illustrates sustained incumbent strength in a relatively high-barrier modern sport.",
                "safest_claim": "The data supports a sustained high-barrier incumbent case for South Korea Archery, while the underlying program mechanism needs external evidence.",
                "not_allowed_claim": "The dataset proves why South Korea dominates Archery.",
                "evidence_summary_short": "Modern medal sum 42; Archery marked high-barrier; repeated modern medal production.",
            },
            "japan_2020_judo_host": {
                "ready_for_presentation": "yes",
                "presentation_priority": 5,
                "risk_note": "Good host/incumbent contrast case, but host panel strength is weak and the Tokyo timing needs careful explanation.",
                "short_title": "Japan 2020 Judo",
                "one_sentence_takeaway": "Japan 2020 Judo combines host-year context with incumbent sport strength, but the source cannot isolate a cause.",
                "safest_claim": "Japan's 2020 Judo result is a candidate host/incumbent-strength case with internal support and clear caveats.",
                "not_allowed_claim": "Hosting Tokyo caused Japan's Judo medal result.",
                "evidence_summary_short": "12 Judo medals; host-year row present; modern Judo has broad medal participation but Japan is an incumbent power.",
            },
            "san_marino_2020_small_country_efficiency": {
                "ready_for_presentation": "yes",
                "presentation_priority": 6,
                "risk_note": "Good normalized-efficiency discussion case, but GDP-total completeness is limited and small denominators are unstable.",
                "short_title": "San Marino 2020",
                "one_sentence_takeaway": "San Marino 2020 is a useful small-country efficiency outlier with explicit small-denominator caveats.",
                "safest_claim": "San Marino 2020 stands out in population-normalized medal efficiency, but it should be presented with small-sample and missing-GDP caveats.",
                "not_allowed_claim": "San Marino is structurally more efficient because of its economy or population size.",
                "evidence_summary_short": "3 medals; very high medals per million; GDP-normalized metric unavailable.",
            },
            "uk_2008_2012_cycling": {
                "ready_for_presentation": "limited",
                "presentation_priority": 7,
                "risk_note": "Recognizable case, but it is program/coach-adjacent and external evidence is required before high-priority use.",
                "short_title": "UK Cycling 2008/2012",
                "one_sentence_takeaway": "UK Cycling is a strong candidate performance-cycle story, but program causes require external evidence.",
                "safest_claim": "The project tables identify UK Cycling as a high-performing candidate case across the Beijing-London cycle.",
                "not_allowed_claim": "Funding, coaching, or technology caused UK Cycling's medal gains based on this dataset alone.",
                "evidence_summary_short": "Best leap year 2008; leap index 22.74; 14 medals; external program validation required.",
            },
            "ethiopia_2000_athletics": {
                "ready_for_presentation": "limited",
                "presentation_priority": 8,
                "risk_note": "Useful comparator to Jamaica, but explanation needs event-specialization and athlete-cohort evidence.",
                "short_title": "Ethiopia 2000 Athletics",
                "one_sentence_takeaway": "Ethiopia 2000 Athletics is a descriptive leap in a broad modern sport, not a proven endurance-system mechanism.",
                "safest_claim": "Ethiopia's 2000 Athletics row is a descriptive leap with internal opportunity and overperformance signals.",
                "not_allowed_claim": "Ethiopia's athletics system caused the 2000 leap.",
                "evidence_summary_short": "Leap index 8.68; 8 Athletics medals; external specialization evidence needed.",
            },
            "france_2024_swimming_host": {
                "ready_for_presentation": "limited",
                "presentation_priority": 9,
                "risk_note": "Timely and visually useful, but post-Games comparison is unavailable and 2024 economic data is limited.",
                "short_title": "France 2024 Swimming",
                "one_sentence_takeaway": "France 2024 Swimming is a timely host-related candidate, but confidence is capped by the missing post-window.",
                "safest_claim": "France 2024 Swimming can be shown as a host-year candidate with missing-post-window and GDP-data caveats.",
                "not_allowed_claim": "France's 2024 host advantage caused the Swimming lift.",
                "evidence_summary_short": "Leap index 12.56; host lift vs baseline 6; no post-Games comparison yet.",
            },
            "bermuda_2020_small_country_efficiency": {
                "ready_for_presentation": "no",
                "presentation_priority": 10,
                "risk_note": "Too fragile for main defense example because normalized metrics are unavailable or incomplete and it is a one-medal case.",
                "short_title": "Bermuda 2020",
                "one_sentence_takeaway": "Bermuda 2020 is an interesting small-country outlier but too data-limited for a main case.",
                "safest_claim": "Bermuda 2020 can be mentioned as a small-country medal case requiring complete normalization context.",
                "not_allowed_claim": "Bermuda proves the small-country efficiency mechanism.",
                "evidence_summary_short": "1 gold medal; normalized economic fields are incomplete; one-medal denominator risk.",
            },
        }
        for column in [
            "ready_for_presentation",
            "risk_note",
            "short_title",
            "one_sentence_takeaway",
            "safest_claim",
            "not_allowed_claim",
            "evidence_summary_short",
        ]:
            cards_df[column] = ""
        cards_df["presentation_priority"] = np.nan
        for case_id, values in presentation.items():
            mask = cards_df["case_id"].eq(case_id)
            for column, value in values.items():
                cards_df.loc[mask, column] = value
        cards_df["ready_for_presentation"] = cards_df["ready_for_presentation"].fillna("limited")
        cards_df["presentation_priority"] = pd.to_numeric(cards_df["presentation_priority"], errors="coerce").fillna(99).astype(int)
        cards_df["risk_note"] = cards_df["risk_note"].fillna("Use with standard candidate-mechanism caveats and external validation note.")
        cards_df["short_title"] = cards_df["short_title"].fillna(cards_df["case_title"].astype(str).str.split(":").str[0])
        cards_df["one_sentence_takeaway"] = cards_df["one_sentence_takeaway"].fillna(cards_df["frontend_card_text"])
        cards_df["safest_claim"] = cards_df["safest_claim"].fillna(cards_df["frontend_card_text"])
        cards_df["not_allowed_claim"] = cards_df["not_allowed_claim"].fillna("Do not present this case as causal proof.")
        cards_df["evidence_summary_short"] = cards_df["evidence_summary_short"].fillna(cards_df["key_metrics"])
        cards_df = cards_df.sort_values(["presentation_priority", "case_id"]).reset_index(drop=True)
        return cards_df

    cards_df = pd.DataFrame(cards).drop_duplicates("case_id")
    cards_df = enrich_case_cards(cards_df)
    return select_export(
        cards_df,
        [
            "case_id",
            "case_title",
            "short_title",
            "country_name",
            "noc",
            "sport",
            "year_or_period",
            "ready_for_presentation",
            "presentation_priority",
            "phenomenon_summary",
            "one_sentence_takeaway",
            "safest_claim",
            "not_allowed_claim",
            "key_metrics",
            "evidence_summary_short",
            "candidate_mechanism",
            "evidence_stack",
            "alternative_explanations",
            "confidence_level",
            "risk_note",
            "external_evidence_needed",
            "chart_recommendation",
            "frontend_card_text",
        ],
    )


def missing_rate(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns:
        return math.nan
    return float(df[column].isna().mean())


def build_mechanism_reporting_filter(
    leap_enhanced: pd.DataFrame,
    host_panel: pd.DataFrame,
    host_summary: pd.DataFrame,
    gender_decomp: pd.DataFrame,
    sport_barrier_modern: pd.DataFrame,
    economic_efficiency: pd.DataFrame,
) -> pd.DataFrame:
    """Translate existing quality signals into reporting guidance."""
    rows: list[dict[str, Any]] = []

    def add(
        record_type: str,
        entity_id: str,
        recommendation: str,
        risk_level: str,
        reason: str,
        safer: str,
        requires_external_validation: bool,
    ) -> None:
        rows.append(
            {
                "record_type": record_type,
                "entity_id": entity_id,
                "recommended_for_presentation": recommendation,
                "reporting_risk_level": risk_level,
                "reporting_risk_reason": reason,
                "safer_interpretation": safer,
                "requires_external_validation": int(requires_external_validation),
            }
        )

    econ = economic_efficiency.copy()
    for column in ["year", "has_population_match", "has_gdp_total_match", "economic_data_complete", "total_medals"]:
        if column in econ.columns:
            econ[column] = pd.to_numeric(econ[column], errors="coerce")
    econ_risk = econ[
        (econ.get("total_medals", 0) > 0)
        & (
            econ.get("economic_data_complete", 0).fillna(0).eq(0)
            | econ.get("has_population_match", 0).fillna(0).eq(0)
            | econ.get("has_gdp_total_match", 0).fillna(0).eq(0)
        )
    ].copy()
    for _, row in econ_risk.iterrows():
        year = int(row["year"]) if pd.notna(row.get("year")) else "NA"
        country = row.get("country_name", "NA")
        noc = row.get("noc", "NA")
        level = "high" if year == 2024 or row.get("has_gdp_total_match", 0) == 0 else "medium"
        add(
            "economic_interpretation",
            f"{year}|{noc}|{country}",
            "not_recommended_for_direct_presentation" if level == "high" else "show_with_limitations",
            level,
            "GDP total and/or population match is missing or incomplete; economic-normalized ratios and overperformance baselines are fragile.",
            f"For {country} {year}, describe medal scale only or use economic metrics as incomplete-data context, not as a strong economic mechanism.",
            True,
        )

    hp = host_panel.copy()
    for column in ["host_year", "pre_year", "post_year", "host_year_medals", "host_lift_vs_baseline", "host_lift_vs_peers"]:
        if column in hp.columns:
            hp[column] = pd.to_numeric(hp[column], errors="coerce")
    host_window_risk = hp[
        hp["pre_year"].isna()
        | hp["post_year"].isna()
        | hp.get("host_effect_strength", "").astype(str).isin(["insufficient_window", "weak_candidate"])
    ].copy()
    for _, row in host_window_risk.iterrows():
        year = int(row["host_year"]) if pd.notna(row.get("host_year")) else "NA"
        country = row.get("host_country", "NA")
        sport = row.get("sport", "NA")
        early_or_missing = (year != "NA" and int(year) <= 1920) or pd.isna(row.get("pre_year")) or pd.isna(row.get("post_year"))
        level = "high" if early_or_missing else "medium"
        add(
            "host_effect_panel",
            f"{year}|{country}|{sport}",
            "not_recommended_for_direct_presentation" if level == "high" else "show_with_limitations",
            level,
            f"Host comparison window is incomplete or weak: pre_year={row.get('pre_year')}, post_year={row.get('post_year')}, strength={row.get('host_effect_strength')}.",
            f"Present {country} {year} {sport} only as a host-year descriptive contrast; avoid ranking it as a robust host-effect case.",
            True,
        )

    hs = host_summary.copy()
    for column in ["host_year", "total_medals_pre", "total_medals_post"]:
        if column in hs.columns:
            hs[column] = pd.to_numeric(hs[column], errors="coerce")
    limited_host_summary = hs[hs.get("host_effect_confidence", "").astype(str).str.contains("limited|missing|low", case=False, na=False)]
    for _, row in limited_host_summary.iterrows():
        year = int(row["host_year"]) if pd.notna(row.get("host_year")) else "NA"
        country = row.get("host_country", "NA")
        level = "high" if year != "NA" and int(year) <= 1920 else "medium"
        add(
            "host_effect_summary",
            f"{year}|{country}",
            "not_recommended_for_direct_presentation" if level == "high" else "show_with_limitations",
            level,
            f"Host summary confidence is {row.get('host_effect_confidence')}; pre or post Games total may be missing.",
            f"Use {country} {year} as context for host-window comparison, not as a headline host-effect conclusion.",
            True,
        )

    gd = gender_decomp.copy()
    for column in ["period_start", "period_end", "female_medal_growth_total"]:
        if column in gd.columns:
            gd[column] = pd.to_numeric(gd[column], errors="coerce")
    component_columns = [
        c
        for c in [
            "opportunity_expansion_component",
            "participation_expansion_component",
            "conversion_improvement_component",
            "unexplained_component",
        ]
        if c in gd.columns
    ]
    low_gender = gd[
        gd.get("decomposition_confidence", "").astype(str).str.contains("low", case=False, na=False)
        | (gd[component_columns].isna().any(axis=1) if component_columns else False)
    ].copy()
    for _, row in low_gender.iterrows():
        start = int(row["period_start"]) if pd.notna(row.get("period_start")) else "NA"
        end = int(row["period_end"]) if pd.notna(row.get("period_end")) else "NA"
        country = row.get("country_name", "NA")
        add(
            "gender_decomposition",
            f"{country}|{start}-{end}",
            "show_with_limitations",
            "medium",
            f"Gender decomposition confidence is {row.get('decomposition_confidence')}; one or more opportunity/participation/conversion inputs may be missing.",
            f"For {country} {start}-{end}, present female medal growth as a descriptive decomposition with missing-input caveats.",
            False,
        )

    leap = leap_enhanced.copy()
    if "year" in leap.columns:
        leap["year"] = pd.to_numeric(leap["year"], errors="coerce")
    low_mech = leap[
        leap.get("confidence_level", "").astype(str).eq("low")
        & ~leap.get("candidate_primary_mechanism", "").astype(str).eq("unexplained_by_current_tables")
    ].copy()
    for _, row in low_mech.iterrows():
        year = int(row["year"]) if pd.notna(row.get("year")) else "NA"
        country = row.get("country_name", "NA")
        sport = row.get("sport", "NA")
        mechanism = row.get("candidate_primary_mechanism", "NA")
        add(
            "leap_mechanism_low_confidence",
            f"{year}|{row.get('noc', 'NA')}|{country}|{sport}",
            "not_recommended_for_direct_presentation",
            "high",
            f"Mechanism label is {mechanism} but confidence_level=low; risk_flags={row.get('risk_flags', 'none')}.",
            f"Describe {country} {year} {sport} as a leap anomaly with possible {mechanism} signal; do not use it as a main mechanism example.",
            True,
        )

    sb = sport_barrier_modern.copy()
    for column in [
        "breakthrough_friendliness_index_modern",
        "entry_barrier_index_modern",
        "repeat_winner_rate_modern",
        "active_modern_sport_flag",
    ]:
        if column in sb.columns:
            sb[column] = pd.to_numeric(sb[column], errors="coerce")
    poor_policy = sb[
        sb.get("active_modern_sport_flag", 1).fillna(1).eq(0)
        | sb.get("entry_barrier_index_modern", 0).fillna(0).ge(0.75)
        | sb.get("breakthrough_friendliness_index_modern", 1).fillna(1).le(0.35)
        | sb.get("repeat_winner_rate_modern", 0).fillna(0).ge(0.85)
    ].copy()
    for _, row in poor_policy.iterrows():
        sport = row.get("sport", "NA")
        barrier = pd.to_numeric(row.get("entry_barrier_index_modern"), errors="coerce")
        friendly = pd.to_numeric(row.get("breakthrough_friendliness_index_modern"), errors="coerce")
        level = "high" if pd.notna(barrier) and barrier >= 0.85 else "medium"
        add(
            "modern_sport_policy_risk",
            str(sport),
            "not_recommended_for_direct_presentation" if level == "high" else "show_with_limitations",
            level,
            f"Modern sport but policy transfer is risky: entry_barrier={barrier:.2f} if available, breakthrough_friendliness={friendly:.2f} if available, type={row.get('modern_sport_type')}.",
            f"Use {sport} as a high-barrier or incumbent-advantage example, not as a generic breakthrough recommendation.",
            True,
        )

    if not rows:
        add(
            "reporting_filter_status",
            "all_checked",
            "direct_show",
            "low",
            "No high-risk presentation records were flagged by current reporting rules.",
            "Proceed with standard candidate-mechanism caveats.",
            False,
        )

    priority = {
        "not_recommended_for_direct_presentation": 0,
        "show_with_limitations": 1,
        "direct_show": 2,
    }
    out = pd.DataFrame(rows).drop_duplicates(["record_type", "entity_id", "reporting_risk_reason"])
    out["_sort"] = out["recommended_for_presentation"].map(priority).fillna(9)
    out = out.sort_values(["_sort", "record_type", "entity_id"]).drop(columns=["_sort"]).reset_index(drop=True)
    return select_export(
        out,
        [
            "record_type",
            "entity_id",
            "recommended_for_presentation",
            "reporting_risk_level",
            "reporting_risk_reason",
            "safer_interpretation",
            "requires_external_validation",
        ],
    )


def build_quality_report(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for name, df in tables.items():
        rows.append(
            {
                "table_name": name,
                "rows": len(df),
                "columns": len(df.columns),
                "year_min": pd.to_numeric(df["year"], errors="coerce").min() if "year" in df.columns else np.nan,
                "year_max": pd.to_numeric(df["year"], errors="coerce").max() if "year" in df.columns else np.nan,
                "missing_country_name_rate": missing_rate(df, "country_name"),
                "missing_noc_rate": missing_rate(df, "noc"),
                "missing_sport_rate": missing_rate(df, "sport"),
                "missing_gdp_per_capita_rate": missing_rate(df, "gdp_per_capita"),
                "missing_population_rate": missing_rate(df, "population"),
            }
        )
    return pd.DataFrame(rows)


def build_table_manifest(tables: dict[str, pd.DataFrame], config: dict[str, Any]) -> pd.DataFrame:
    output_names = config["outputs"]
    rows = []
    for key, df in tables.items():
        if key in {"table_manifest", "data_dictionary"}:
            continue
        rows.append(
            {
                "table_name": key,
                "file_name": output_names.get(key, f"{key}.csv"),
                "rows": len(df),
                "columns": len(df.columns),
                "grain": infer_grain(key),
                "primary_keys_recommended": infer_primary_key(key),
            }
        )
    return pd.DataFrame(rows)


def infer_grain(table_name: str) -> str:
    grains = {
        "clean_olympic_events": "athlete-event record",
        "clean_athlete_events_summer": "athlete-event record, 1896-2016 athlete source",
        "official_event_medals": "country-event-medal record after team medal deduplication",
        "medal_unique_event_country": "country-event-medal record after team medal deduplication",
        "olympic_timeline": "olympic year",
        "country_year_medals": "country-year",
        "country_sport_year_medals": "country-sport-year",
        "sport_year_country_share": "sport-year-country",
        "athlete_demographics": "country-year",
        "country_gdp_population_medal_efficiency": "country-year",
        "country_economic_efficiency": "country-year",
        "country_expected_vs_actual_medals": "country-year",
        "country_multi_normalized_efficiency": "country-year multi-normalized efficiency profile",
        "host_years": "host-year",
        "host_effect_country_sport_panel": "host-year-sport before/during/after panel",
        "host_effect_summary": "host-year summary of before/during/after candidate host effect",
        "host_effect_summary_modern": "modern host-year summary with complete pre/post windows for reporting",
        "event_added_removed": "program-change event",
        "event_opportunity_refined": "refined program-change event with opportunity classification",
        "coach_events_template": "manual coach-event template",
        "coach_events_template_examples": "non-data examples for coach-event template",
        "gender_event_expansion_by_year": "year-sport-sex program opportunity",
        "country_gender_opportunity_capture": "country-year gender medal capture",
        "country_gender_medal_structure": "country-year gender medal structure and growth attribution",
        "country_gender_growth_decomposition": "country-period female medal growth shift-share decomposition",
        "country_profile_features": "country profile feature vector",
        "country_cluster_labels": "country typology cluster assignment",
        "sport_entry_barrier_index": "sport-level barrier and breakthrough-friendliness profile",
        "sport_entry_barrier_modern": "modern active sport-level entry barrier and breakthrough-friendliness profile",
        "sport_breakthrough_cases": "country-sport-year breakthrough case with sport barrier context",
        "leap_mechanism_decomposition": "country-sport-year leap mechanism candidate decomposition",
        "leap_mechanism_enhanced": "country-sport-year evidence-tiered leap mechanism interpretation",
        "leap_case_library": "curated candidate leap case library",
        "mechanism_case_cards": "curated front-end mechanism case card",
        "mechanism_reporting_filter": "record-level reporting risk and presentation guidance",
        "country_year_sport_concentration": "country-year",
        "sport_globalization_metrics": "sport-year",
        "leap_index_country_sport": "country-sport-year",
        "data_quality_report": "table-level quality metrics",
    }
    return grains.get(table_name, "unspecified")


def infer_primary_key(table_name: str) -> str:
    keys = {
        "country_year_medals": "year,noc",
        "country_sport_year_medals": "year,noc,sport",
        "sport_year_country_share": "year,sport,noc",
        "athlete_demographics": "year,noc",
        "country_gdp_population_medal_efficiency": "year,noc",
        "country_economic_efficiency": "year,noc",
        "country_expected_vs_actual_medals": "year,noc",
        "country_multi_normalized_efficiency": "year,noc",
        "host_years": "year,host_noc",
        "host_effect_country_sport_panel": "host_year,host_noc,sport",
        "host_effect_summary": "host_year,host_country",
        "host_effect_summary_modern": "host_year,host_country",
        "event_added_removed": "year,sport,event,sex,change_type",
        "event_opportunity_refined": "year,sport,event,sex,refined_change_type",
        "gender_event_expansion_by_year": "year,sport,sex_category",
        "country_gender_opportunity_capture": "year,noc",
        "country_gender_medal_structure": "year,noc",
        "country_gender_growth_decomposition": "country_name,noc,period_start,period_end",
        "country_profile_features": "noc",
        "country_cluster_labels": "noc",
        "sport_entry_barrier_index": "sport",
        "sport_entry_barrier_modern": "sport",
        "sport_breakthrough_cases": "year,sport,noc",
        "leap_mechanism_decomposition": "year,noc,sport",
        "leap_mechanism_enhanced": "year,noc,sport",
        "leap_case_library": "case_id",
        "mechanism_case_cards": "case_id",
        "mechanism_reporting_filter": "record_type,entity_id",
        "country_year_sport_concentration": "year,noc",
        "sport_globalization_metrics": "year,sport",
        "leap_index_country_sport": "year,noc,sport",
        "olympic_timeline": "year",
    }
    return keys.get(table_name, "")


def build_data_dictionary(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    descriptions = {
        "year": "Olympic year.",
        "season": "Olympic season. P0 standard output uses Summer.",
        "noc": "Olympic NOC code.",
        "iso3": "ISO3-like country code used for joining external economic and population data.",
        "country_name": "Standardized country or team display name.",
        "sport": "Olympic sport.",
        "event": "Olympic event.",
        "medal": "Gold, Silver, Bronze, or No medal.",
        "total_medals": "Gold + Silver + Bronze medal count after team-event deduplication where applicable.",
        "gold_medals": "Gold medal count.",
        "silver_medals": "Silver medal count.",
        "bronze_medals": "Bronze medal count.",
        "weighted_medal_score": "3 * gold + 2 * silver + bronze.",
        "gold_share": "Gold medals divided by total medals.",
        "country_sport_hhi": "HHI of a country's medals across sports in a year.",
        "sport_country_hhi": "HHI of a sport's medals across countries in a year.",
        "female_athlete_share": "Female athletes divided by female plus male athletes.",
        "female_share": "Backward-compatible alias for female_athlete_share.",
        "gdp_per_capita": "GDP per capita joined by iso3 and year.",
        "gdp_total": "Total GDP joined by iso3 and year, using the Penn World Table source where available.",
        "population": "Population joined by iso3 and year.",
        "medals_per_million_people": "Total medals per one million people.",
        "gold_per_million_people": "Gold medals per one million people.",
        "medals_per_10k_gdp_per_capita": "Total medals per 10,000 units of GDP per capita.",
        "gold_per_10k_gdp_per_capita": "Gold medals per 10,000 units of GDP per capita.",
        "medals_per_10b_gdp": "Total medals per 10 billion units of GDP.",
        "gold_per_10b_gdp": "Gold medals per 10 billion units of GDP.",
        "medals_per_100b_gdp": "Total medals per 100 billion units of GDP.",
        "gold_per_100b_gdp": "Gold medals per 100 billion units of GDP.",
        "expected_medals": "Descriptive baseline medal count from a year-specific economic model.",
        "medal_overperformance": "Actual medals minus expected medals from the descriptive baseline.",
        "sports_entered": "Count of sports a country entered in that Olympic year.",
        "model_note": "Text note describing which expected-medal baseline was used.",
        "city": "Host city name as recorded in Olympic event data.",
        "host_country": "Standardized host country name compatible with country_name where possible.",
        "host_noc": "Host country NOC code.",
        "host_noc_compatible": "Historical NOC code used to match country_year_medals when it differs from host_noc.",
        "host_continent": "Host continent from curated host mapping.",
        "is_home_games": "1 if the host NOC appears in country-year medal outputs for that year.",
        "source_note": "Source or evidence note for manually curated or derived fields.",
        "needs_manual_review": "1 if a derived mechanism event row should be manually checked.",
        "host_year": "Olympic year in which the country hosted the Summer Games.",
        "pre_year": "Previous observed Summer Olympic year used as the pre-host comparison; blank when unavailable.",
        "post_year": "Next observed Summer Olympic year used as the post-host comparison; blank when unavailable.",
        "pre_host_medals": "Host country's medals in the sport in the previous observed Games.",
        "host_year_medals": "Host country's medals in the sport in the host Games.",
        "post_host_medals": "Host country's medals in the sport in the next observed Games.",
        "pre_country_share_in_sport": "Host country's sport medal share in the previous observed Games.",
        "host_country_share_in_sport": "Host country's sport medal share in the host Games.",
        "post_country_share_in_sport": "Host country's sport medal share in the next observed Games.",
        "host_lift_vs_pre": "Host-year sport medal lift relative to the previous Games.",
        "host_lift_vs_post": "Host-year sport medal lift relative to the next Games.",
        "host_lift_vs_baseline": "Host-year sport medal lift relative to the average of available pre and post values.",
        "non_host_peer_change": "Average medal change from pre to host year among same-sport non-host medal-winning peers.",
        "host_lift_vs_peers": "Host lift versus pre minus non-host peer change; candidate peer-adjusted host evidence.",
        "host_effect_strength": "Descriptive strength tier for host-effect candidate evidence, not a causal estimate.",
        "host_effect_interpretation": "Human-readable before/during/after host-effect candidate interpretation.",
        "total_medals_pre": "Host country's total medals in the previous observed Games.",
        "total_medals_host": "Host country's total medals in the host Games.",
        "total_medals_post": "Host country's total medals in the next observed Games.",
        "host_total_lift_vs_pre": "Host country's total medal lift relative to the previous Games.",
        "host_total_lift_vs_baseline": "Host country's total medal lift relative to the average of available pre and post values.",
        "sports_with_positive_host_lift": "Number of host sports with positive lift versus the pre/post baseline.",
        "strongest_host_effect_sport": "Sport with the largest positive host_lift_vs_baseline for the host year.",
        "host_effect_confidence": "Host-year summary confidence tier for candidate host-effect evidence.",
        "sex": "Event sex category: men, women, mixed, or unknown.",
        "sex_category": "Event sex category: men, women, mixed, or open_unknown.",
        "change_type": "Program change type: added, removed, or restored.",
        "original_change_type": "Raw adjacent-Games change type from event_added_removed.",
        "refined_change_type": "Refined opportunity change type: true_added, restored, removed, likely_renamed_or_reclassified, or uncertain.",
        "event_family": "Cleaned event family after removing sport and sex labels where possible.",
        "standard_event_family": "Conservative normalized event-family string used to flag likely renames, reclassifications or splits.",
        "true_new_medal_opportunity_flag": "1 when the refined rules treat the event as a likely true new medal opportunity.",
        "restored_event_flag": "1 when the event is treated as restored rather than brand-new.",
        "rename_or_split_likely_flag": "1 when same-transition event-family evidence suggests rename, reclassification or split risk.",
        "opportunity_delta_vs_prev_games": "Refined opportunity delta versus previous Games: usually +1, 0, -1, or blank when uncertain.",
        "confidence_level": "Evidence confidence tier for a mechanism or opportunity classification.",
        "event_count": "Count of unique events in a year-sport-sex category.",
        "medal_opportunities": "Count of deduplicated country-event-medal opportunities in a year-sport-sex category.",
        "countries_winning_medals": "Number of countries winning medals in the category.",
        "female_medals": "Medals won in women's events.",
        "male_medals": "Medals won in men's events.",
        "mixed_medals": "Medals won in mixed events.",
        "open_unknown_medals": "Medals won in open or unclassified events.",
        "female_medal_share": "Women's-event medals divided by total medals.",
        "female_event_opportunity_share": "Women's events divided by all events in that Olympic year.",
        "female_medal_opportunity_share": "Women's medal opportunities divided by all medal opportunities in that Olympic year.",
        "female_capture_ratio": "female_medal_share divided by female_event_opportunity_share.",
        "female_medal_share_minus_opportunity_share": "Difference between female medal share and female event opportunity share.",
        "female_medal_growth": "Change in women's-event medal count from the previous Games for the country.",
        "female_opportunity_share_growth": "Change in women's-event opportunity share from the previous Games.",
        "female_capture_ratio_growth": "Change in female_capture_ratio from the previous Games.",
        "growth_interpretation": "Descriptive label separating opportunity expansion from capture improvement.",
        "period_start": "Start Olympic year for a period-level decomposition.",
        "period_end": "End Olympic year for a period-level decomposition.",
        "female_medals_start": "Women's-event medals at the start of the decomposition period.",
        "female_medals_end": "Women's-event medals at the end of the decomposition period.",
        "female_medal_growth_total": "End minus start women's-event medal count.",
        "female_event_opportunity_start": "Women's event opportunity share at period start.",
        "female_event_opportunity_end": "Women's event opportunity share at period end.",
        "female_athlete_share_start": "Female athlete share at period start.",
        "female_athlete_share_end": "Female athlete share at period end.",
        "female_capture_ratio_start": "Female medal share divided by women's event opportunity share at period start.",
        "female_capture_ratio_end": "Female medal share divided by women's event opportunity share at period end.",
        "opportunity_expansion_component": "Descriptive female medal growth component associated with women's opportunity expansion.",
        "participation_expansion_component": "Descriptive female medal growth component associated with female athlete share expansion.",
        "conversion_improvement_component": "Descriptive female medal growth component associated with medal conversion improvement.",
        "unexplained_component": "Residual component including total medal scale change, interactions and data limitations.",
        "dominant_growth_driver": "Largest positive decomposition component, or no-positive-growth label.",
        "decomposition_confidence": "Confidence tier for the descriptive decomposition based on endpoint data completeness and growth size.",
        "total_medals_mean": "Mean total medals across medal-winning Olympic years.",
        "gold_share_mean": "Mean gold share across medal-winning Olympic years.",
        "medal_volatility": "Standard deviation of total medals across medal-winning Olympic years.",
        "sport_concentration_hhi": "Mean country sport concentration HHI.",
        "medal_sports_count": "Mean number of medal-winning sports.",
        "recent_growth_rate": "Mean medal growth rate in the most recent 12-year window.",
        "medals_per_100_athletes": "Total medals per 100 unique athletes.",
        "gold_per_100_athletes": "Gold medals per 100 unique athletes.",
        "medals_per_entered_sport": "Total medals divided by number of sports entered.",
        "medals_per_entered_event": "Total medals divided by number of events entered.",
        "weighted_score_per_athlete": "Weighted medal score divided by unique athletes.",
        "weighted_score_per_entered_sport": "Weighted medal score divided by number of sports entered.",
        "rank_medals_per_million_people": "Within-year rank by medals per million people.",
        "rank_medals_per_100_athletes": "Within-year rank by medals per 100 athletes.",
        "rank_medals_per_entered_sport": "Within-year rank by medals per entered sport.",
        "rank_weighted_score_per_athlete": "Within-year rank by weighted score per athlete.",
        "small_sample_flag": "1 if delegation/event sample is small enough for ratio metrics to be unstable.",
        "extreme_single_medal_flag": "1 if a one-medal country has very high population- or athlete-normalized ratios.",
        "efficiency_label": "Business-readable country-year label comparing scale and efficiency.",
        "efficiency_interpretation_note": "Caveat note for interpreting multi-normalized efficiency metrics.",
        "years_with_medals": "Number of Olympic years in which the country won medals.",
        "recent_total_medals_mean": "Mean total medals in the most recent 12-year window.",
        "economic_data_complete_rate": "Share of country-year rows with complete economic data.",
        "athletes_mean": "Mean unique athletes across Olympic years.",
        "profile_note": "Notes on country profile feature construction.",
        "cluster_id": "Numeric country typology cluster id.",
        "cluster_name": "Business-readable country typology label.",
        "cluster_k": "Number of clusters used.",
        "typical_countries": "Representative countries in the same cluster.",
        "clustering_method": "Clustering method used to assign typology labels.",
        "cluster_note": "Caveat note for interpreting typology clusters.",
        "sport_country_hhi_mean": "Mean sport-level medal concentration HHI across Olympic years.",
        "countries_winning_medals_mean": "Mean number of medal-winning countries in the sport across Olympic years.",
        "top1_share_mean": "Mean medal share of the leading country in the sport-year.",
        "new_country_medal_count": "Number of countries whose first recorded sport medal appears in this sport.",
        "medal_opportunity_count": "Total medal opportunities observed for the sport across Olympic years.",
        "modern_year_min": "First Olympic year observed for the sport in the modern analysis window.",
        "modern_year_max": "Last Olympic year observed for the sport in the modern analysis window.",
        "active_modern_sport_flag": "1 if the sport appears in the latest observed Summer Games in the data.",
        "countries_winning_medals_mean_modern": "Mean number of medal-winning countries per sport-year from 1992 onward.",
        "sport_country_hhi_mean_modern": "Mean sport-level medal concentration HHI from 1992 onward.",
        "top1_share_mean_modern": "Mean top-country medal share in the sport from 1992 onward.",
        "new_country_medal_count_modern": "Number of country first-sport-medal cases whose first year is in the modern window.",
        "female_event_growth_rate_modern": "Growth rate of women's event count within the modern window.",
        "medal_opportunity_count_modern": "Total medal opportunities in the sport within the modern window.",
        "repeat_winner_rate_modern": "Share of modern country-sport rows where the country had prior sport medals before that row.",
        "breakthrough_friendliness_index_modern": "Modern composite index where higher means the sport appears more accessible for new medal-winning countries.",
        "entry_barrier_index_modern": "Modern composite index where higher means the sport appears harder for new medal-winning countries.",
        "modern_sport_type": "Modern sport opportunity/barrier type for strategy discussion.",
        "event_growth_rate": "Growth rate of event count from first observed year to latest observed year.",
        "female_event_growth_rate": "Growth rate of women's event count from first observed women's-event year to latest observed year.",
        "median_population_of_winners": "Median population of medal-winning countries in the sport-year rows with matched data.",
        "median_gdp_of_winners": "Median total GDP of medal-winning countries in the sport-year rows with matched data.",
        "median_medals_per_million_of_winners": "Median population-normalized medal efficiency among sport medal winners.",
        "median_medals_per_100b_gdp_of_winners": "Median GDP-normalized medal efficiency among sport medal winners.",
        "repeat_winner_rate": "Share of country-sport medal rows where the country had already won a medal in the sport before.",
        "latest_event_count": "Latest observed total event count for the sport.",
        "latest_women_event_count": "Latest observed women's event count for the sport.",
        "latest_total_medal_opportunities": "Latest observed medal opportunities for the sport.",
        "breakthrough_friendliness_index": "Composite descriptive index where higher means the sport appears more accessible for new or smaller medal-winning countries.",
        "entry_barrier_index": "Composite descriptive index where higher means the sport appears harder for new or smaller medal-winning countries.",
        "sport_type": "Business-readable sport opportunity/barrier type.",
        "recommendation_note": "Short advisory note for project selection interpretation.",
        "index_note": "Caveat note for interpreting the composite sport barrier index.",
        "country_first_sport_medal_year": "First Olympic year when the country won a recorded medal in this sport.",
        "previous_country_sport_medals": "Cumulative country medals in the sport before the current Olympic year.",
        "is_first_country_sport_medal": "1 if this is the country's first recorded medal year in the sport.",
        "case_type": "Breakthrough case type such as first sport medal or early-stage repeat breakthrough.",
        "case_note": "Short note describing why the row is treated as a breakthrough or established case.",
        "start_year": "Start year for a manually curated coach event.",
        "end_year": "End year for a manually curated coach event; blank if ongoing or unknown.",
        "coach_name": "Coach name for manual curation.",
        "event_type": "Coach event type such as appointment, departure, program_change, or tenure.",
        "evidence_level": "Evidence quality marker such as official_source, secondary_source, or unverified.",
        "olympic_leap_index": "Candidate anomaly score for country-sport performance jumps; not a causal estimate.",
        "host_effect_score": "Candidate mechanism score for host-year overlap.",
        "event_expansion_effect_score": "Candidate mechanism score for sport event additions or restorations in the same year.",
        "female_expansion_effect_score": "Candidate mechanism score for women's event expansion in the sport.",
        "coach_effect_score": "Candidate mechanism score from manually curated coach template rows; 0 when no evidence is entered.",
        "economic_growth_effect_score": "Candidate mechanism score from improvement versus the descriptive economic baseline.",
        "continuity_score": "1 if the country continued winning medals in the sport in the next observed Games.",
        "one_off_spike_risk_score": "Risk score for a leap that may be a one-off spike rather than a sustained shift.",
        "primary_mechanism_label": "Highest-scoring candidate mechanism label, or one_off_spike_risk when risk is high.",
        "mechanism_explanation_text": "Human-readable candidate explanation assembled from structured mechanism signals.",
        "mechanism_evidence_level": "Evidence level for the mechanism explanation.",
        "coach_evidence_level": "Evidence level from coach_events_template when available.",
        "coach_source_note": "Source note from coach_events_template when available.",
        "added_or_restored_events": "Backward-compatible count now populated from refined true new medal opportunity events.",
        "added_women_events": "Count of refined true new women's events in the sport-year.",
        "restored_events": "Count of restored events in the sport-year from event_opportunity_refined.",
        "uncertain_opportunity_events": "Count of uncertain opportunity-change rows in the sport-year from event_opportunity_refined.",
        "women_event_growth": "Change in women's event count from previous Games for the sport.",
        "economic_overperformance_growth": "Change in medal_overperformance from previous Games for the country.",
        "candidate_primary_mechanism": "Primary candidate mechanism after excluding continuity and one-off risk as main causes.",
        "confidence_level": "Analyst-facing confidence tier for the candidate mechanism interpretation.",
        "evidence_stack": "Concise stacked summary separating descriptive evidence, internal candidates, internal comparative support, external evidence and risk.",
        "supporting_signals": "Secondary supporting evidence such as continuity or positive overperformance; not a primary mechanism.",
        "risk_flags": "Interpretation risks and caveats such as one-off spike risk or incomplete economic data.",
        "alternative_explanations": "Plausible alternative non-causal explanations not ruled out by current tables.",
        "host_evidence": "Host-related evidence statement for the row.",
        "event_expansion_evidence": "Program expansion evidence statement for the row.",
        "female_expansion_evidence": "Women's opportunity expansion evidence statement for the row.",
        "economic_evidence": "Economic-baseline evidence statement with data-completeness caveats.",
        "coach_evidence": "Coach/program evidence statement; only non-none when external evidence is curated in the template.",
        "continuity_signal": "Support-only continuity note, never a primary mechanism label.",
        "one_off_spike_risk": "Human-readable risk tier for a possibly unsustained leap.",
        "interpretation_text": "Narrative interpretation separating leap description, candidate mechanism and caveats.",
        "interpretation_scope_note": "Explicit scope note stating that the row is not a causal claim.",
        "case_id": "Stable id for a leap case card.",
        "case_priority": "Priority rank within the generated leap case library.",
        "case_title": "Human-readable leap case title.",
        "short_title": "Compact front-end title for a mechanism case card.",
        "ready_for_presentation": "Presentation readiness flag: yes, limited, or no.",
        "presentation_priority": "Analyst-facing priority rank for defense/reporting use; lower is higher priority.",
        "case_card_prompt": "Template prompt for analysts or AI assistants reviewing the case.",
        "recommended_next_research": "Recommended manual follow-up research for the case.",
        "year_or_period": "Year or period label used for front-end case-card display.",
        "phenomenon_summary": "Short summary of the descriptive performance phenomenon shown by the case.",
        "one_sentence_takeaway": "One-sentence defense-friendly takeaway that stays within candidate-mechanism wording.",
        "safest_claim": "Safest version of the claim suitable for reporting or defense.",
        "not_allowed_claim": "Over-strong claim that should not be made from current project tables.",
        "key_metrics": "Compact metric string for case-card display.",
        "evidence_summary_short": "Short evidence summary optimized for front-end cards and oral defense.",
        "candidate_mechanism": "Primary candidate mechanism phrased for the case card.",
        "risk_note": "Case-specific caveat explaining what can be misread or what evidence is missing.",
        "external_evidence_needed": "Explicit note about the external evidence still needed before stronger claims.",
        "chart_recommendation": "Suggested chart type or configuration for front-end display.",
        "frontend_card_text": "Front-end ready summary text for the case card.",
        "reporting_note": "Presentation-safe note explaining how to describe the row in a report or defense.",
        "record_type": "Source category for a reporting-risk row, such as economic interpretation, host panel, gender decomposition, low-confidence leap mechanism, or modern sport policy risk.",
        "entity_id": "Stable composite identifier for the flagged record.",
        "recommended_for_presentation": "Presentation recommendation: direct_show, show_with_limitations, or not_recommended_for_direct_presentation.",
        "reporting_risk_level": "Reporting risk tier: low, medium, or high.",
        "reporting_risk_reason": "Plain-language reason the record needs caveats or should not be presented directly.",
        "safer_interpretation": "Suggested safer wording for reports or front-end notes.",
        "requires_external_validation": "1 if the record needs external evidence or manual validation before stronger claims.",
    }
    rows = []
    for table_name, df in tables.items():
        for column in df.columns:
            rows.append(
                {
                    "table_name": table_name,
                    "column_name": column,
                    "dtype": str(df[column].dtype),
                    "description": descriptions.get(column, ""),
                }
            )
    return pd.DataFrame(rows)


def write_tables(tables: dict[str, pd.DataFrame], config: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for key, df in tables.items():
        filename = config["outputs"].get(key, f"{key}.csv")
        if filename.endswith(".json"):
            continue
        df.to_csv(output_dir / filename, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)


def write_metadata_run_log(
    tables: dict[str, pd.DataFrame],
    config: dict[str, Any],
    output_dir: Path,
    input_paths: dict[str, Path],
) -> None:
    log = {
        "project": config["project"]["name"],
        "pipeline_version": "p0.1",
        "run_started_at_utc": datetime.now(timezone.utc).isoformat(),
        "project_root": str(PROJECT_ROOT),
        "config_path": str(DEFAULT_CONFIG_PATH),
        "inputs": {
            name: {
                "path": str(path),
                "exists": path.exists(),
                "size_bytes": path.stat().st_size if path.exists() else None,
                "modified_time_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
                if path.exists()
                else None,
            }
            for name, path in input_paths.items()
        },
        "outputs": {
            key: {
                "file_name": config["outputs"].get(key, f"{key}.csv"),
                "rows": len(df),
                "columns": list(df.columns),
            }
            for key, df in tables.items()
        },
        "field_contract": "lowercase_snake_case",
        "notes": [
            "Medal tables use country-event-medal deduplication to reduce team event double counting.",
            "Olympic Leap Index is descriptive anomaly detection, not causal inference.",
            "Coach event data is not generated in P0 because it requires manual curation.",
        ],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / config["outputs"]["metadata_run_log"]).write_text(
        json.dumps(log, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to project_config.yaml")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path
    config = deep_merge(DEFAULT_CONFIG, load_simple_yaml(config_path))
    tables = build_pipeline(config)
    print("Done. Outputs saved to:", (PROJECT_ROOT / config["paths"]["output_dir"]).resolve())
    print(tables["data_quality_report"].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
