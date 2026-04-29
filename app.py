"""
Pipeline de limpieza y procesamiento de datos de calidad del aire.

Cada CSV tiene:
    col 0: Parámetro  |  col 1: Fecha (YYYY-MM-DD)  |  col 2: Hora (0-23)
    cols 3..-1: una columna por estación de monitoreo
    Valores faltantes marcados como "- - - -" o variaciones con espacios

SALIDAS EN data/processed/

    datos_limpios.csv       — promedios horarios en formato largo
    datos_diarios.csv       — promedios diarios en formato largo
    daily_avg.parquet       — diarios en formato ancho (un contaminante/columna)
    weekly_pattern.parquet  — patrón día × hora en formato ancho
    monthly_series.parquet  — serie mensual en formato ancho

"""

import re
from pathlib import Path
import numpy as np
import pandas as pd

# Rutas
BASE_DIR      = Path(__file__).resolve().parent.parent
DB_DIR        = BASE_DIR / "data" / "raw" / "base de datos VG"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# Constantes
CITY_MAP = {
    "cdmx":        "CDMX",
    "guadalajara": "Guadalajara",
    "monterrey":   "Monterrey",
}

POLL_MAP = {
    "PM2.5": "PM25",
    "CO":    "CO",
    "NO2":   "NO2",
    "O3":    "O3",
    "SO2":   "SO2",
}

POLLUTANTS = ["PM25", "CO", "NO2", "O3", "SO2"]

# Límites físicos máximos — valores mayores indican sensor corrupto
# Gases en ppm; PM2.5 en µg/m³
MAX_VALUES = {
    "PM25": 500.0,
    "O3":     1.0,
    "NO2":    1.0,
    "CO":    50.0,
    "SO2":    1.0,
}

# Regex para detectar valores faltantes de SINAICA ("- - - -" y variantes)
_MISSING_RE = re.compile(r"^\s*(-\s*){2,}\s*$")

ESTACION_ANIO = {
    12: "Invierno", 1: "Invierno",  2: "Invierno",
     3: "Primavera", 4: "Primavera", 5: "Primavera",
     6: "Verano",    7: "Verano",    8: "Verano",
     9: "Otoño",    10: "Otoño",    11: "Otoño",
}


# PASO 1: Lectura de CSVs --------------------------------------------------------------------------

def _leer_csv_sinaica(path: Path) -> pd.DataFrame:
    """
    Lee un CSV individual y devuelve el promedio horario
    de todas las estaciones de monitoreo disponibles.

    - Detecta columnas de estación (entre 'Hora' y 'Unidad')
    - Reemplaza "- - - -" y variantes por NaN
    - Convierte valores a numérico
    - Calcula el promedio de todas las estaciones por hora ignorando NaN

    Retorna DataFrame con columnas: fecha, hora, valor
    """
    df = pd.read_csv(path, encoding="latin-1", dtype=str)

    station_cols = list(df.columns[3:-1])
    if not station_cols:
        return pd.DataFrame(columns=["fecha", "hora", "valor"])

    def _limpiar(v):
        s = str(v).strip()
        if _MISSING_RE.match(s) or s in ("nan", "", "NaN"):
            return np.nan
        return v

    for col in station_cols:
        df[col] = df[col].map(_limpiar)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["valor"] = df[station_cols].mean(axis=1, skipna=True)
    df["fecha"] = pd.to_datetime(df["Fecha"], format="%Y-%m-%d", errors="coerce")
    df["hora"]  = pd.to_numeric(df["Hora"], errors="coerce").astype("Int64")

    return df[["fecha", "hora", "valor"]].copy()


def cargar_datos_crudos() -> pd.DataFrame:
    """
    PASO 1 — Recorre ciudad → contaminante → año y concatena todos los CSVs.
    Retorna DataFrame largo con columnas: ciudad, contaminante, fecha, hora, valor
    """
    chunks = []

    for city_dir in sorted(DB_DIR.iterdir()):
        if not city_dir.is_dir():
            continue
        ciudad = CITY_MAP.get(city_dir.name.lower())
        if ciudad is None:
            continue

        for poll_dir in sorted(city_dir.iterdir()):
            if not poll_dir.is_dir():
                continue
            poll_col = POLL_MAP.get(poll_dir.name)
            if poll_col is None:
                continue

            for csv_path in sorted(poll_dir.glob("*.csv")):
                try:
                    chunk = _leer_csv_sinaica(csv_path)
                    chunk["ciudad"]       = ciudad
                    chunk["contaminante"] = poll_col
                    chunks.append(chunk)
                except Exception:
                    pass

    if not chunks:
        raise RuntimeError(f"No se encontraron CSVs en {DB_DIR}")

    return pd.concat(chunks, ignore_index=True)


# PASO 2: Limpieza ---------------------------------------------------------------------------------

def limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """
    PASO 2 — Limpieza y enriquecimiento del DataFrame crudo.

    Criterios:
    1. Eliminar filas con fecha, hora o valor NaN
       → Sin timestamp o valor el registro no tiene información útil
    2. Reemplazar valores negativos por NaN
       → Concentraciones negativas no son posibles
    3. Reemplazar valores sobre el límite máximo por NaN
       → Indican falla de sensor o dato corrupto
    4. Agregar columnas temporales derivadas
    """
    df = df.copy()

    # 1. Eliminar filas incompletas
    df = df.dropna(subset=["fecha", "hora", "valor"])

    # 2 y 3. Valores físicamente imposibles
    for poll, maxval in MAX_VALUES.items():
        mask = df["contaminante"] == poll
        df.loc[mask & (df["valor"] < 0),     "valor"] = np.nan
        df.loc[mask & (df["valor"] > maxval), "valor"] = np.nan

    df = df.dropna(subset=["valor"])

    # 4. Columnas temporales
    df["anio"]          = df["fecha"].dt.year
    df["mes"]           = df["fecha"].dt.month
    df["dia_semana"]    = df["fecha"].dt.dayofweek    # 0 = lunes
    df["nombre_dia"]    = df["fecha"].dt.day_name()   # inglés para ordenamiento
    df["semana"]        = df["fecha"].dt.isocalendar().week.astype(int)
    df["estacion_anio"] = df["mes"].map(ESTACION_ANIO)

    return df


# PASO 3: Agregación -------------------------------------------------------------------------------

def construir_agregados(df: pd.DataFrame):
    """
    PASO 3 — Genera 5 datasets desde el DataFrame limpio.

    Retorna:
        datos_limpios  — promedios horarios (largo)
        datos_diarios  — promedios diarios (largo)
        daily_wide     — diarios en ancho, un contaminante por columna
        weekly_wide    — patrón día×hora para el heatmap semanal
        monthly_wide   — serie mensual para la gráfica de COVID
    """
    # Promedios horarios
    datos_limpios = (
        df.groupby([
            "ciudad", "contaminante", "fecha", "hora",
            "anio", "mes", "dia_semana", "nombre_dia", "semana", "estacion_anio"
        ])["valor"].mean().reset_index()
    )

    # Promedios diarios
    datos_diarios = (
        df.groupby([
            "ciudad", "contaminante", "fecha",
            "anio", "mes", "dia_semana", "nombre_dia", "semana", "estacion_anio"
        ])["valor"].mean().reset_index()
    )

    # Diarios en ancho — secciones 1, 2 y 3 de la app
    daily_wide = datos_diarios.pivot_table(
        index=["ciudad", "fecha", "anio", "mes",
               "dia_semana", "nombre_dia", "semana", "estacion_anio"],
        columns="contaminante", values="valor", aggfunc="mean",
    ).reset_index()
    daily_wide.columns.name = None
    for p in POLLUTANTS:
        if p not in daily_wide.columns:
            daily_wide[p] = np.nan

    # Patrón semanal día×hora — heatmap sección 2
    weekly_wide = (
        df.groupby(["ciudad", "contaminante", "dia_semana", "nombre_dia", "hora"])["valor"]
        .mean().reset_index()
        .pivot_table(
            index=["ciudad", "dia_semana", "nombre_dia", "hora"],
            columns="contaminante", values="valor", aggfunc="mean",
        ).reset_index()
    )
    weekly_wide.columns.name = None
    for p in POLLUTANTS:
        if p not in weekly_wide.columns:
            weekly_wide[p] = np.nan

    # Serie mensual — gráfica COVID sección 4
    monthly_wide = (
        df.groupby(["ciudad", "contaminante", "anio", "mes"])["valor"]
        .mean().reset_index()
        .pivot_table(
            index=["ciudad", "anio", "mes"],
            columns="contaminante", values="valor", aggfunc="mean",
        ).reset_index()
    )
    monthly_wide.columns.name = None
    for p in POLLUTANTS:
        if p not in monthly_wide.columns:
            monthly_wide[p] = np.nan
    monthly_wide["fecha_mes"] = pd.to_datetime(
        monthly_wide["anio"].astype(str) + "-" +
        monthly_wide["mes"].astype(str).str.zfill(2) + "-01"
    )

    return datos_limpios, datos_diarios, daily_wide, weekly_wide, monthly_wide


# PASO 4: Guardado ---------------------------------------------------------------------------------

def guardar_resultados(datos_limpios, datos_diarios, daily_wide, weekly_wide, monthly_wide):
    """
    PASO 4 — Guarda todos los datasets en data/processed/.
    CSV para revisón manual, Parquet para carga rápida en la app.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    datos_limpios.to_csv(PROCESSED_DIR / "datos_limpios.csv", index=False)
    datos_diarios.to_csv(PROCESSED_DIR / "datos_diarios.csv", index=False)

    daily_wide.to_parquet(PROCESSED_DIR / "daily_avg.parquet",       index=False)
    daily_wide.to_csv(    PROCESSED_DIR / "daily_avg.csv",            index=False)

    weekly_wide.to_parquet(PROCESSED_DIR / "weekly_pattern.parquet",  index=False)
    weekly_wide.to_csv(    PROCESSED_DIR / "weekly_pattern.csv",       index=False)

    monthly_wide.to_parquet(PROCESSED_DIR / "monthly_series.parquet", index=False)
    monthly_wide.to_csv(    PROCESSED_DIR / "monthly_series.csv",      index=False)


# Ejecución ----------------------------------------------------------------------------------------
df_raw   = cargar_datos_crudos()
df_clean = limpiar_datos(df_raw)
outputs  = construir_agregados(df_clean)
guardar_resultados(*outputs)