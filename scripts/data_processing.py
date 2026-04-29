"""
Limpieza y procesamiento de datos reales de SINAICA (calidad del aire México).
Fuente: SINAICA – INECC (https://sinaica.inecc.gob.mx/)
Autor: Alexander Góngora Venegas
Curso: Visualización gráfica para IA – Universidad Iberoamericana León

Estructura esperada en data/raw/base de datos VG/:
    {ciudad}/{contaminante}/Datos SINAICA-{poll}-CH-{año}.csv

Uso:
    python src/data_processing.py          # usa datos reales si existen
    python src/data_processing.py --synthetic  # fuerza datos sintéticos

Salidas en data/processed/:
    datos_limpios.csv    — promedios horarios por ciudad+contaminante (formato largo)
    datos_diarios.csv    — promedios diarios por ciudad+contaminante (formato largo)
    daily_avg.parquet    — diarios en formato ancho (un contaminante por columna)
    weekly_pattern.parquet — patrón día×hora en formato ancho
    monthly_series.parquet — serie mensual en formato ancho
"""

import argparse
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_DIR     = BASE_DIR / "data" / "raw" / "base de datos VG"
RAW_DIR    = BASE_DIR / "data" / "raw"        # fallback CSVs sintéticos
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# Mapeo: nombre de carpeta → nombre de ciudad normalizado
CITY_MAP = {
    "cdmx":        "CDMX",
    "guadalajara": "Guadalajara",
    "monterrey":   "Monterrey",
}

# Mapeo: nombre de carpeta de contaminante → nombre de columna en el output
POLL_MAP = {
    "PM2.5": "PM25",
    "CO":    "CO",
    "NO2":   "NO2",
    "O3":    "O3",
    "SO2":   "SO2",
}

POLLUTANTS = ["PM25", "CO", "NO2", "O3", "SO2"]

# Límites físicos máximos razonables (valores mayores = sensor corrupto)
# Gaseous pollutants in ppm; PM2.5 in µg/m³
MAX_VALUES = {
    "PM25": 500.0,
    "O3":     1.0,   # ppm → 0-0.5 típico
    "NO2":    1.0,
    "CO":    50.0,
    "SO2":    1.0,
}

# Patrón de valores faltantes de SINAICA: "- - - -" con variaciones de espacios
_MISSING_RE = re.compile(r"^\s*(-\s*){2,}\s*$")

ESTACION_ANIO = {
    12: "Invierno", 1: "Invierno",  2: "Invierno",
    3:  "Primavera", 4: "Primavera", 5: "Primavera",
    6:  "Verano",   7: "Verano",    8: "Verano",
    9:  "Otono",   10: "Otono",    11: "Otono",
}


# ─── Lectura de un CSV SINAICA ─────────────────────────────────────────────

def _read_sinaica_csv(path: Path) -> pd.DataFrame:
    """
    Lee un CSV de SINAICA y devuelve el promedio horario de todas las estaciones.

    Estructura del archivo:
        col 0: Parámetro  |  col 1: Fecha  |  col 2: Hora
        cols 3..-2: estaciones de monitoreo
        col -1: Unidad

    Retorna DataFrame con columnas: fecha, hora, valor
    donde 'valor' es el promedio de estaciones válidas en esa hora.
    """
    df = pd.read_csv(path, encoding="latin-1", dtype=str)

    # Las columnas de estación son las que están entre Hora y Unidad (índices 3..-1)
    station_cols = list(df.columns[3:-1])
    if not station_cols:
        log.warning(f"  Sin columnas de estación en {path.name}")
        return pd.DataFrame(columns=["fecha", "hora", "valor"])

    # Reemplazar valores faltantes por NaN
    def clean_val(v):
        s = str(v).strip()
        if _MISSING_RE.match(s) or s in ("nan", "", "NaN"):
            return np.nan
        return v

    for col in station_cols:
        df[col] = df[col].map(clean_val)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Calcular promedio de estaciones por fila (ignorando NaN)
    df["valor"] = df[station_cols].mean(axis=1, skipna=True)

    # Parsear Fecha y Hora
    df["fecha"] = pd.to_datetime(df["Fecha"], format="%Y-%m-%d", errors="coerce")
    df["hora"]  = pd.to_numeric(df["Hora"], errors="coerce").astype("Int64")

    return df[["fecha", "hora", "valor"]].copy()


# ─── Pipeline principal: datos reales ─────────────────────────────────────

def process_real_data() -> pd.DataFrame:
    """
    Recorre ciudad → contaminante → año y agrega todos los CSVs en un
    DataFrame largo con columnas: ciudad, contaminante, fecha, hora, valor.
    """
    chunks = []

    for city_dir in sorted(DB_DIR.iterdir()):
        if not city_dir.is_dir():
            continue
        ciudad = CITY_MAP.get(city_dir.name.lower())
        if ciudad is None:
            log.warning(f"Carpeta desconocida ignorada: {city_dir.name}")
            continue

        for poll_dir in sorted(city_dir.iterdir()):
            if not poll_dir.is_dir():
                continue
            poll_col = POLL_MAP.get(poll_dir.name)
            if poll_col is None:
                log.warning(f"  Contaminante desconocido ignorado: {poll_dir.name}")
                continue

            csvs = sorted(poll_dir.glob("*.csv"))
            log.info(f"  {ciudad}/{poll_dir.name}: {len(csvs)} archivos")

            for csv_path in csvs:
                try:
                    df_chunk = _read_sinaica_csv(csv_path)
                    df_chunk["ciudad"]      = ciudad
                    df_chunk["contaminante"] = poll_col
                    chunks.append(df_chunk)
                except Exception as exc:
                    log.warning(f"    ERROR leyendo {csv_path.name}: {exc}")

    if not chunks:
        raise RuntimeError(f"No se encontraron CSVs en {DB_DIR}")

    df = pd.concat(chunks, ignore_index=True)
    log.info(f"Total filas concatenadas: {len(df):,}")
    return df


# ─── Limpieza y columnas derivadas ────────────────────────────────────────

def clean_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Criterios de limpieza:
    1. Eliminar filas con fecha o hora inválidas.
    2. Eliminar filas donde valor es NaN (sin ninguna estación disponible).
    3. Reemplazar valores negativos por NaN.
    4. Reemplazar valores sobre el límite máximo por NaN (sensor corrupto).
    5. Agregar columnas temporales derivadas.
    """
    df = df.copy()

    # 1. Fechas/horas inválidas
    n0 = len(df)
    df = df.dropna(subset=["fecha", "hora", "valor"])
    log.info(f"Paso 1 — Filas con fecha/hora/valor NaT eliminadas: {n0 - len(df)}")

    # 2-3. Valores físicamente imposibles
    for poll, maxval in MAX_VALUES.items():
        mask = df["contaminante"] == poll
        n_neg = ((df.loc[mask, "valor"] < 0)).sum()
        n_max = ((df.loc[mask, "valor"] > maxval)).sum()
        if n_neg + n_max > 0:
            df.loc[mask & (df["valor"] < 0),      "valor"] = np.nan
            df.loc[mask & (df["valor"] > maxval),  "valor"] = np.nan
            log.info(f"Paso 2-3 — {poll}: {n_neg} negativos + {n_max} sobre límite → NaN")

    df = df.dropna(subset=["valor"])

    # 4. Columnas temporales
    df["anio"]        = df["fecha"].dt.year
    df["mes"]         = df["fecha"].dt.month
    df["dia_semana"]  = df["fecha"].dt.dayofweek          # 0 = lunes
    df["nombre_dia"]  = df["fecha"].dt.day_name()         # English
    df["semana"]      = df["fecha"].dt.isocalendar().week.astype(int)
    df["estacion_anio"] = df["mes"].map(ESTACION_ANIO)

    log.info(f"Filas tras limpieza: {len(df):,}")
    return df


# ─── Agregados ────────────────────────────────────────────────────────────

def build_all_aggregates(df: pd.DataFrame):
    """
    Genera 5 datasets de salida desde el DataFrame largo limpio.

    Returns:
        datos_limpios   — horarios por ciudad+contaminante (largo)
        datos_diarios   — diarios por ciudad+contaminante (largo)
        daily_wide      — diarios en ancho (un contaminante por columna)
        weekly_wide     — patrón día×hora en ancho
        monthly_wide    — serie mensual en ancho
    """
    grp_keys_hourly = ["ciudad", "contaminante", "fecha", "hora",
                       "anio", "mes", "dia_semana", "nombre_dia", "semana", "estacion_anio"]
    grp_keys_daily  = ["ciudad", "contaminante", "fecha",
                       "anio", "mes", "dia_semana", "nombre_dia", "semana", "estacion_anio"]

    # ── Largo horario ──
    datos_limpios = (
        df.groupby(grp_keys_hourly)["valor"]
        .mean()
        .reset_index()
    )

    # ── Largo diario ──
    datos_diarios = (
        df.groupby(grp_keys_daily)["valor"]
        .mean()
        .reset_index()
    )

    # ── Ancho diario para app.py ──
    daily_wide = datos_diarios.pivot_table(
        index=["ciudad", "fecha", "anio", "mes",
               "dia_semana", "nombre_dia", "semana", "estacion_anio"],
        columns="contaminante",
        values="valor",
        aggfunc="mean",
    ).reset_index()
    daily_wide.columns.name = None
    # Asegura que todas las columnas de contaminante estén presentes
    for p in POLLUTANTS:
        if p not in daily_wide.columns:
            daily_wide[p] = np.nan

    # ── Patrón semanal (día × hora) para heatmap ──
    weekly_wide = (
        df.groupby(["ciudad", "contaminante", "dia_semana", "nombre_dia", "hora"])["valor"]
        .mean()
        .reset_index()
        .pivot_table(
            index=["ciudad", "dia_semana", "nombre_dia", "hora"],
            columns="contaminante",
            values="valor",
            aggfunc="mean",
        )
        .reset_index()
    )
    weekly_wide.columns.name = None
    for p in POLLUTANTS:
        if p not in weekly_wide.columns:
            weekly_wide[p] = np.nan

    # ── Serie mensual ──
    monthly_wide = (
        df.groupby(["ciudad", "contaminante", "anio", "mes"])["valor"]
        .mean()
        .reset_index()
        .pivot_table(
            index=["ciudad", "anio", "mes"],
            columns="contaminante",
            values="valor",
            aggfunc="mean",
        )
        .reset_index()
    )
    monthly_wide.columns.name = None
    for p in POLLUTANTS:
        if p not in monthly_wide.columns:
            monthly_wide[p] = np.nan
    monthly_wide["fecha_mes"] = pd.to_datetime(
        monthly_wide["anio"].astype(str) + "-" +
        monthly_wide["mes"].astype(str).str.zfill(2) + "-01"
    )

    log.info(
        f"Agregados — horario: {len(datos_limpios):,} | diario: {len(datos_diarios):,} | "
        f"daily_wide: {len(daily_wide):,} | weekly: {len(weekly_wide):,} | monthly: {len(monthly_wide):,}"
    )
    return datos_limpios, datos_diarios, daily_wide, weekly_wide, monthly_wide


# ─── Guardado ─────────────────────────────────────────────────────────────

def save_all(datos_limpios, datos_diarios, daily_wide, weekly_wide, monthly_wide):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    datos_limpios.to_csv(PROCESSED_DIR / "datos_limpios.csv", index=False)
    datos_diarios.to_csv(PROCESSED_DIR / "datos_diarios.csv", index=False)

    daily_wide.to_parquet(PROCESSED_DIR / "daily_avg.parquet",      index=False)
    daily_wide.to_csv(   PROCESSED_DIR / "daily_avg.csv",           index=False)

    weekly_wide.to_parquet(PROCESSED_DIR / "weekly_pattern.parquet", index=False)
    weekly_wide.to_csv(   PROCESSED_DIR / "weekly_pattern.csv",      index=False)

    monthly_wide.to_parquet(PROCESSED_DIR / "monthly_series.parquet", index=False)
    monthly_wide.to_csv(   PROCESSED_DIR / "monthly_series.csv",       index=False)

    log.info(f"Todos los archivos guardados en {PROCESSED_DIR}")


# ─── Fallback: datos sintéticos (para testing / deploy sin datos reales) ──

def _seasonal(month, poll):
    if poll in ("PM25", "CO", "SO2"):
        return 1 + 0.35 * np.cos(2 * np.pi * (month - 1) / 12)
    elif poll == "O3":
        return 1 + 0.45 * np.sin(2 * np.pi * (month - 5) / 12 + np.pi / 2)
    return 1 + 0.15 * np.cos(2 * np.pi * (month - 1) / 12)

def _weekly(dow, poll):
    if poll == "O3":
        return np.where(dow >= 5, 1.08, 1.0)
    return np.where(dow == 0, 1.05, 1.0) * np.where(np.isin(dow, [1,2,3]), 1.12, 1.0) * np.where(dow >= 5, 0.78, 1.0)

def _hourly(hour, poll):
    if poll == "O3":
        return 1 + 0.70 * np.exp(-((hour - 15)**2) / 20)
    return 1 + 0.55 * (np.exp(-((hour - 8)**2)/6) + np.exp(-((hour - 19)**2)/8))

def _covid(ts, poll):
    f = np.ones(len(ts))
    s = (ts >= "2020-03-23") & (ts <= "2020-06-01")
    p = (ts >= "2020-06-02") & (ts <= "2020-09-01")
    reductions = {"NO2":(0.55,0.78),"CO":(0.58,0.80),"PM25":(0.65,0.82),"SO2":(0.70,0.85),"O3":(1.10,1.05)}
    sf, pf = reductions.get(poll, (0.75, 0.88))
    f[s] = sf; f[p] = pf
    return f

SYNTHETIC_BASES = {
    "CDMX":        {"PM25": 0.025, "O3": 0.065, "NO2": 0.050, "CO": 1.2,  "SO2": 0.008},
    "Monterrey":   {"PM25": 0.028, "O3": 0.060, "NO2": 0.045, "CO": 1.5,  "SO2": 0.010},
    "Guadalajara": {"PM25": 0.020, "O3": 0.050, "NO2": 0.035, "CO": 1.0,  "SO2": 0.006},
}

def generate_synthetic_data() -> pd.DataFrame:
    """Genera datos sintéticos en el mismo formato largo que process_real_data()."""
    log.info("Generando datos sintéticos (fallback) ...")
    rng = np.random.default_rng(42)
    ts  = pd.date_range("2019-01-01", "2024-12-31", freq="h")
    month_a = ts.month.values.astype(float)
    dow_a   = ts.dayofweek.values
    hour_a  = ts.hour.values.astype(float)
    year_a  = ts.year.values

    rows = []
    for ciudad, bases in SYNTHETIC_BASES.items():
        for poll, base in bases.items():
            sig = (base
                   * _seasonal(month_a, poll)
                   * _weekly(dow_a, poll)
                   * _hourly(hour_a, poll)
                   * _covid(ts, poll)
                   * (1 - 0.02 * (year_a - 2019)))
            vals = np.maximum(0.0, sig + rng.normal(0, base * 0.08, len(ts)))
            vals[rng.random(len(ts)) < 0.03] = np.nan
            chunk = pd.DataFrame({"fecha": ts, "hora": ts.hour, "valor": vals,
                                  "ciudad": ciudad, "contaminante": poll})
            rows.append(chunk)

    df = pd.concat(rows, ignore_index=True).dropna(subset=["valor"])
    log.info(f"Datos sintéticos generados: {len(df):,} filas")
    return df


# ─── Entry point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pipeline SINAICA – calidad del aire México")
    parser.add_argument("--synthetic", action="store_true",
                        help="Fuerza el uso de datos sintéticos aunque existan CSVs reales")
    args = parser.parse_args()

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    if args.synthetic or not DB_DIR.exists():
        if not DB_DIR.exists():
            log.info(f"No se encontró {DB_DIR}. Usando datos sintéticos.")
        df_raw = generate_synthetic_data()
    else:
        log.info(f"Leyendo datos reales de {DB_DIR} ...")
        df_raw = process_real_data()

    log.info("Limpiando y enriqueciendo ...")
    df_clean = clean_and_enrich(df_raw)

    log.info("Construyendo agregados ...")
    outputs = build_all_aggregates(df_clean)

    log.info("Guardando ...")
    save_all(*outputs)

    log.info("¡Proceso completado exitosamente!")


if __name__ == "__main__":
    main()
