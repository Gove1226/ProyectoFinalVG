"""
Limpieza y procesamiento de datos de calidad del aire en México.
Fuente: SINAICA – INECC (https://sinaica.inecc.gob.mx/)
Autor: Alexander Góngora Venegas
Curso: Visualización gráfica para IA – Universidad Iberoamericana León

Uso:
    python src/data_processing.py

Si no hay CSVs en data/raw/, se generan datos sintéticos realistas basados
en patrones documentados de SINAICA para las 6 ciudades más contaminadas.
Para forzar la regeneración: python src/data_processing.py --generate
"""

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# ─── Configuración de ciudades ────────────────────────────────────────────────
CITIES = {
    "Mexicali": {
        "lat": 32.66, "lon": -115.47, "estado": "Baja California",
        "base": {"PM25": 35.0, "O3": 55.0, "NO2": 40.0, "CO": 1.8, "SO2": 12.0},
    },
    "Monterrey": {
        "lat": 25.67, "lon": -100.31, "estado": "Nuevo León",
        "base": {"PM25": 28.0, "O3": 60.0, "NO2": 45.0, "CO": 1.5, "SO2": 10.0},
    },
    "CDMX": {
        "lat": 19.43, "lon": -99.13, "estado": "Ciudad de México",
        "base": {"PM25": 25.0, "O3": 65.0, "NO2": 50.0, "CO": 1.2, "SO2": 8.0},
    },
    "Guadalajara": {
        "lat": 20.66, "lon": -103.35, "estado": "Jalisco",
        "base": {"PM25": 20.0, "O3": 50.0, "NO2": 35.0, "CO": 1.0, "SO2": 6.0},
    },
    "Puebla": {
        "lat": 19.04, "lon": -98.19, "estado": "Puebla",
        "base": {"PM25": 18.0, "O3": 45.0, "NO2": 30.0, "CO": 0.9, "SO2": 5.0},
    },
    "Tijuana": {
        "lat": 32.52, "lon": -117.03, "estado": "Baja California",
        "base": {"PM25": 15.0, "O3": 40.0, "NO2": 28.0, "CO": 0.8, "SO2": 4.0},
    },
}

POLLUTANTS = ["PM25", "O3", "NO2", "CO", "SO2"]

# Límites máximos permitidos — valores por encima se consideran registros corruptos
MAX_VALUES = {"PM25": 500.0, "O3": 300.0, "NO2": 400.0, "CO": 50.0, "SO2": 200.0}


# ─── Factores del modelo sintético ───────────────────────────────────────────

def _seasonal(month: np.ndarray, pollutant: str) -> np.ndarray:
    """
    Factor estacional.
    PM2.5/CO/SO2: pico en invierno (inversiones térmicas).
    O3: pico en verano (mayor radiación UV para fotoquímica).
    NO2: variación moderada en invierno.
    """
    if pollutant in ("PM25", "CO", "SO2"):
        return 1 + 0.35 * np.cos(2 * np.pi * (month - 1) / 12)
    elif pollutant == "O3":
        return 1 + 0.45 * np.sin(2 * np.pi * (month - 5) / 12 + np.pi / 2)
    else:  # NO2
        return 1 + 0.15 * np.cos(2 * np.pi * (month - 1) / 12)


def _weekly(dayofweek: np.ndarray, pollutant: str) -> np.ndarray:
    """
    Factor día de la semana. 0 = lunes, 6 = domingo.
    PM2.5/NO2/CO/SO2: pico martes-jueves (acumulado laboral), caída fin de semana.
    O3: efecto fin de semana invertido (menos NO que lo destruya → más O3).
    """
    if pollutant == "O3":
        return np.where(dayofweek >= 5, 1.08, 1.0)

    monday = np.where(dayofweek == 0, 1.05, 1.0)          # arranque semana
    midweek = np.where(np.isin(dayofweek, [1, 2, 3]), 1.12, 1.0)  # martes-jueves
    weekend = np.where(dayofweek >= 5, 0.78, 1.0)         # sábado-domingo
    return monday * midweek * weekend


def _hourly(hour: np.ndarray, pollutant: str) -> np.ndarray:
    """
    Patrón intradiario.
    PM2.5/NO2/CO/SO2: dos picos (hora punta mañana 8h y tarde 19h).
    O3: un pico fotoquímico vespertino (~15h).
    """
    if pollutant in ("PM25", "NO2", "CO", "SO2"):
        morning = np.exp(-((hour - 8) ** 2) / 6)
        evening = np.exp(-((hour - 19) ** 2) / 8)
        return 1 + 0.55 * (morning + evening)
    else:  # O3
        return 1 + 0.70 * np.exp(-((hour - 15) ** 2) / 20)


def _covid(timestamps: pd.DatetimeIndex, pollutant: str) -> np.ndarray:
    """
    Factor de confinamiento COVID.
    Confinamiento estricto: 23 mar – 1 jun 2020.
    Reapertura gradual: 2 jun – 1 sep 2020.
    O3 sube levemente por menor titulación con NO.
    """
    factor = np.ones(len(timestamps))
    strict = (timestamps >= "2020-03-23") & (timestamps <= "2020-06-01")
    partial = (timestamps >= "2020-06-02") & (timestamps <= "2020-09-01")

    reductions = {
        "NO2": (0.55, 0.78),
        "CO":  (0.58, 0.80),
        "PM25": (0.65, 0.82),
        "SO2": (0.70, 0.85),
        "O3":  (1.10, 1.05),  # invierte: sube
    }
    s_factor, p_factor = reductions.get(pollutant, (0.75, 0.88))
    factor[strict] = s_factor
    factor[partial] = p_factor
    return factor


# ─── Generación de datos sintéticos ──────────────────────────────────────────

def generate_synthetic_raw(start: str = "2019-01-01", end: str = "2024-12-31") -> None:
    """Genera CSVs horarios sintéticos en data/raw/ por ciudad."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)
    timestamps = pd.date_range(start, end, freq="h")

    month_arr = timestamps.month.values.astype(float)
    dow_arr = timestamps.dayofweek.values
    hour_arr = timestamps.hour.values.astype(float)
    year_arr = timestamps.year.values

    for city, info in CITIES.items():
        log.info(f"  Generando datos para {city} ({len(timestamps):,} registros)...")
        series_dict = {"fecha_hora": timestamps}

        for poll in POLLUTANTS:
            base = info["base"][poll]
            seas = _seasonal(month_arr, poll)
            week = _weekly(dow_arr, poll)
            hour_f = _hourly(hour_arr, poll)
            cov = _covid(timestamps, poll)
            # Mejora ambiental gradual ~2 % por año a partir de 2019
            trend = 1 - 0.02 * (year_arr - 2019)

            signal = base * seas * week * hour_f * cov * trend
            noise = rng.normal(0, base * 0.08, len(timestamps))
            values = np.maximum(0.0, signal + noise)

            # ~3 % de NaN simulando fallas de sensor
            nan_idx = rng.random(len(timestamps)) < 0.03
            values = values.astype(float)
            values[nan_idx] = np.nan

            series_dict[poll] = values

        df = pd.DataFrame(series_dict)
        df["ciudad"] = city
        df["estado"] = info["estado"]
        df["lat"] = info["lat"]
        df["lon"] = info["lon"]
        df["estacion"] = f"Est_{city}_01"

        out = RAW_DIR / f"{city.lower()}_2019_2024.csv"
        df.to_csv(out, index=False)
        log.info(f"    Guardado: {out.name}")


# ─── Carga ────────────────────────────────────────────────────────────────────

def load_raw() -> pd.DataFrame:
    """Carga todos los CSVs de data/raw/."""
    csvs = sorted(RAW_DIR.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No hay CSVs en {RAW_DIR}.")
    dfs = [pd.read_csv(p, parse_dates=["fecha_hora"]) for p in csvs]
    df = pd.concat(dfs, ignore_index=True)
    log.info(f"CSVs cargados: {len(csvs)} archivos, {len(df):,} filas totales")
    return df


# ─── Limpieza ─────────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Criterios de limpieza (documentados):
    1. Eliminar filas con fecha_hora inválida (NaT).
    2. Reemplazar valores negativos por NaN — físicamente imposibles.
    3. Reemplazar valores sobre límite máximo por NaN — sensor corrupto.
    4. Interpolar huecos cortos (≤ 3 horas consecutivas) por ciudad.
    5. Descartar filas donde >3 contaminantes son simultáneamente NaN.
    6. Generar columnas temporales derivadas.
    """
    df = df.copy()
    df.columns = df.columns.str.strip().str.replace(" ", "_")

    # 1. Timestamps inválidos
    n_before = len(df)
    df = df.dropna(subset=["fecha_hora"])
    log.info(f"Paso 1 — Timestamps inválidos eliminados: {n_before - len(df)}")

    # 2-3. Valores fuera de rango
    for poll, maxval in MAX_VALUES.items():
        if poll in df.columns:
            mask_neg = df[poll] < 0
            mask_max = df[poll] > maxval
            df.loc[mask_neg | mask_max, poll] = np.nan
            n_bad = (mask_neg | mask_max).sum()
            if n_bad > 0:
                log.info(f"Paso 2-3 — {poll}: {n_bad} valores fuera de rango → NaN")

    # 4. Interpolación de huecos cortos (por ciudad, máx 3 horas)
    poll_cols = [p for p in POLLUTANTS if p in df.columns]
    df = df.sort_values(["ciudad", "fecha_hora"]).reset_index(drop=True)
    for poll in poll_cols:
        df[poll] = df.groupby("ciudad")[poll].transform(
            lambda s: s.interpolate(method="linear", limit=3)
        )

    # 5. Descartar filas con mayoría de contaminantes faltantes
    df["_n_nan"] = df[poll_cols].isna().sum(axis=1)
    n_before = len(df)
    df = df[df["_n_nan"] <= 3].drop(columns=["_n_nan"])
    log.info(f"Paso 5 — Filas con >3 contaminantes NaN eliminadas: {n_before - len(df)}")

    # 6. Columnas temporales derivadas
    df["año"] = df["fecha_hora"].dt.year
    df["mes"] = df["fecha_hora"].dt.month
    df["dia_semana"] = df["fecha_hora"].dt.dayofweek          # 0 = lunes
    df["nombre_dia"] = df["fecha_hora"].dt.day_name()         # English (para indexado)
    df["hora"] = df["fecha_hora"].dt.hour
    df["fecha"] = df["fecha_hora"].dt.normalize()
    df["semana"] = df["fecha_hora"].dt.isocalendar().week.astype(int)
    df["estacion_año"] = df["mes"].map({
        12: "Invierno", 1: "Invierno", 2: "Invierno",
        3: "Primavera", 4: "Primavera", 5: "Primavera",
        6: "Verano", 7: "Verano", 8: "Verano",
        9: "Otoño", 10: "Otoño", 11: "Otoño",
    })

    log.info(f"Limpieza completada. Filas resultantes: {len(df):,}")
    return df


# ─── Agregados ────────────────────────────────────────────────────────────────

def build_aggregates(df: pd.DataFrame):
    """
    Genera tres datasets procesados:
    - daily_avg: promedio diario por ciudad y contaminante
    - weekly_pattern: promedio por ciudad × día-semana × hora (para heatmap)
    - monthly_series: promedio mensual por ciudad (para serie temporal)
    """
    poll_cols = [p for p in POLLUTANTS if p in df.columns]

    # Promedio diario
    daily = (
        df.groupby(
            ["ciudad", "estado", "lat", "lon",
             "fecha", "año", "mes", "dia_semana",
             "nombre_dia", "semana", "estacion_año"]
        )[poll_cols]
        .mean()
        .reset_index()
    )

    # Patrón semanal (día × hora) — base del heatmap
    weekly_pattern = (
        df.groupby(["ciudad", "dia_semana", "nombre_dia", "hora"])[poll_cols]
        .mean()
        .reset_index()
    )

    # Serie mensual
    monthly = (
        df.groupby(["ciudad", "año", "mes"])[poll_cols]
        .mean()
        .reset_index()
    )
    monthly["fecha_mes"] = pd.to_datetime(
        monthly["año"].astype(str) + "-" +
        monthly["mes"].astype(str).str.zfill(2) + "-01"
    )

    log.info(f"Agregados: daily={len(daily):,} | weekly_pattern={len(weekly_pattern):,} | monthly={len(monthly):,}")
    return daily, weekly_pattern, monthly


# ─── Guardado ─────────────────────────────────────────────────────────────────

def save(daily: pd.DataFrame, weekly_pattern: pd.DataFrame, monthly: pd.DataFrame) -> None:
    """Guarda los datasets en data/processed/ como parquet y CSV."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    for name, frame in [("daily_avg", daily), ("weekly_pattern", weekly_pattern), ("monthly_series", monthly)]:
        frame.to_parquet(PROCESSED_DIR / f"{name}.parquet", index=False)
        frame.to_csv(PROCESSED_DIR / f"{name}.csv", index=False)

    log.info(f"Archivos guardados en {PROCESSED_DIR}")


# ─── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline de datos SINAICA – calidad del aire México")
    parser.add_argument("--generate", action="store_true",
                        help="Fuerza la regeneración de datos sintéticos en data/raw/")
    args = parser.parse_args()

    if args.generate or not list(RAW_DIR.glob("*.csv")):
        log.info("Generando datos sintéticos en data/raw/ ...")
        generate_synthetic_raw()
    else:
        log.info(f"Se usarán los CSVs existentes en {RAW_DIR}")

    log.info("Cargando datos crudos...")
    df_raw = load_raw()

    log.info("Limpiando datos...")
    df_clean = clean(df_raw)

    log.info("Generando agregados...")
    daily, weekly_pattern, monthly = build_aggregates(df_clean)

    log.info("Guardando resultados...")
    save(daily, weekly_pattern, monthly)

    log.info("¡Proceso completado exitosamente!")


if __name__ == "__main__":
    main()
