"""
¿Respiramos peor en lunes? — Visualización interactiva de calidad del aire en México
Autor: Alexander Góngora Venegas
Curso: Visualización gráfica para IA – Universidad Iberoamericana León
Docente: Dra. Dora Alvarado
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─── Configuración de página ──────────────────────────────────────────────────

st.set_page_config(
    page_title="¿Respiramos peor en lunes?",
    page_icon="💨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

PROCESSED_DIR = Path(__file__).parent / "data" / "processed"

POLLUTANT_LABELS = {
    "PM25": "PM2.5 (μg/m³)",
    "O3":   "Ozono O₃ (ppb)",
    "NO2":  "Dióxido de nitrógeno NO₂ (ppb)",
    "CO":   "Monóxido de carbono CO (ppm)",
    "SO2":  "Dióxido de azufre SO₂ (ppb)",
}

POLLUTANT_DESC = {
    "PM25": "partículas finas suspendidas (PM2.5) — las más dañinas para el sistema respiratorio",
    "O3":   "ozono troposférico (O₃) — formado por reacción fotoquímica en días soleados",
    "NO2":  "dióxido de nitrógeno (NO₂) — marcador directo del tráfico vehicular",
    "CO":   "monóxido de carbono (CO) — producto de combustión incompleta",
    "SO2":  "dióxido de azufre (SO₂) — asociado a industria y generación eléctrica",
}

DAY_ORDER_EN = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DAY_ES = {
    "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
    "Thursday": "Jueves", "Friday": "Viernes", "Saturday": "Sábado", "Sunday": "Domingo",
}

COLOR_SCALE = "Oranges"
CITY_COLORS = px.colors.qualitative.Set2

# ─── CSS personalizado ────────────────────────────────────────────────────────

st.markdown("""
<style>
  .block-container { padding-top: 1.8rem; max-width: 1200px; }
  h1 { font-size: 2.5rem !important; line-height: 1.2; }
  h2 { font-size: 1.55rem !important; color: #c0392b;
       border-bottom: 2px solid #e8765a; padding-bottom: 6px; margin-top: 2rem; }
  .narr { font-size: 1.05rem; line-height: 1.75; color: #1a1a1a; }
  .callout {
    background: #fff8f0; border-left: 4px solid #e67e22;
    padding: 14px 18px; border-radius: 6px; margin: 14px 0;
    font-size: 1rem; line-height: 1.65;
  }
  .footer {
    font-size: 0.82rem; color: #888; margin-top: 50px;
    border-top: 1px solid #eee; padding-top: 14px;
  }
  .tag {
    display: inline-block; background: #fde8d8;
    color: #c0392b; border-radius: 4px;
    padding: 2px 8px; font-size: 0.85rem; font-weight: 600;
  }
</style>
""", unsafe_allow_html=True)


# ─── Carga de datos ───────────────────────────────────────────────────────────

@st.cache_data
def load_daily() -> pd.DataFrame:
    return pd.read_parquet(PROCESSED_DIR / "daily_avg.parquet")


@st.cache_data
def load_weekly_pattern() -> pd.DataFrame:
    return pd.read_parquet(PROCESSED_DIR / "weekly_pattern.parquet")


@st.cache_data
def load_monthly() -> pd.DataFrame:
    return pd.read_parquet(PROCESSED_DIR / "monthly_series.parquet")


def check_data() -> bool:
    return all((PROCESSED_DIR / f"{n}.parquet").exists()
               for n in ("daily_avg", "weekly_pattern", "monthly_series"))


# ─── Guard: datos no procesados ───────────────────────────────────────────────

if not check_data():
    st.error(
        "No se encontraron datos procesados en `data/processed/`. "
        "Ejecuta primero:\n\n```\npython src/data_processing.py\n```"
    )
    st.stop()


# ─── ENCABEZADO ───────────────────────────────────────────────────────────────

st.markdown('<span class="tag">Calidad del aire · México 2019–2024</span>', unsafe_allow_html=True)
st.title("💨 ¿Respiramos peor en lunes?")
st.subheader("Patrones temporales de contaminación en las ciudades más sucias de México")

st.markdown('<div class="narr">', unsafe_allow_html=True)
st.markdown("""
El aire que respiramos no es igual todos los días. En las ciudades mexicanas con peor calidad del
aire, la concentración de contaminantes sigue ritmos casi tan predecibles como el tráfico: sube
cuando arrancamos el motor, baja cuando nos quedamos en casa y colapsa cuando el mundo entero
se detiene. Esta historia analiza datos de **SINAICA** (Sistema Nacional de Información de la
Calidad del Aire, INECC) para seis ciudades durante el período 2019–2024.

Seguimos cinco contaminantes clave: PM2.5, O₃, NO₂, CO y SO₂. La pregunta que nos guía es
deceptivamente simple: **¿hay un día de la semana en que el aire está peor?** La respuesta,
como veremos, es más matizada —y más interesante— de lo que parece.
""")
st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# ─── SECCIÓN 1: Ranking de ciudades ──────────────────────────────────────────

st.markdown("## 1 · ¿Cuáles son las ciudades más contaminadas?")

st.markdown('<div class="narr">', unsafe_allow_html=True)
st.markdown("""
Antes de entrar al análisis semanal, conviene ubicarnos en el mapa. No todas las ciudades
contaminan igual, y los factores que explican la diferencia van más allá del tamaño poblacional.
**Mexicali**, en la frontera con California, sufre contaminación transfronteriza sumada al uso
intensivo de calefacción y quema de biomasa en invierno. **Monterrey** tiene un perfil industrial
marcado con emisiones de fundidoras y cementeras. La **Ciudad de México**, contra la intuición popular,
ha mejorado considerablemente gracias al programa de verificación vehicular y la expansión del transporte público.
""")
st.markdown('</div>', unsafe_allow_html=True)

daily = load_daily()

col_chart, col_ctrl = st.columns([3, 1])

with col_ctrl:
    st.markdown("**Controles**")
    poll_s1 = st.selectbox(
        "Contaminante", list(POLLUTANT_LABELS.keys()),
        format_func=lambda x: POLLUTANT_LABELS[x], key="s1_poll"
    )
    available_years = sorted(daily["anio"].unique(), reverse=True)
    year_s1 = st.selectbox("Año", available_years, key="s1_year")
    st.caption(f"Mostrando: {POLLUTANT_DESC[poll_s1]}")

ranking = (
    daily[daily["anio"] == year_s1]
    .groupby("ciudad")[poll_s1]
    .mean()
    .reset_index()
    .sort_values(poll_s1, ascending=True)
)

fig1 = px.bar(
    ranking, x=poll_s1, y="ciudad", orientation="h",
    color=poll_s1, color_continuous_scale=COLOR_SCALE,
    labels={poll_s1: POLLUTANT_LABELS[poll_s1], "ciudad": ""},
    title=f"Promedio anual de {POLLUTANT_LABELS[poll_s1]} por ciudad — {year_s1}",
    text=poll_s1,
)
fig1.update_traces(texttemplate="%{text:.1f}", textposition="outside", marker_line_width=0)
fig1.update_layout(
    coloraxis_showscale=False, plot_bgcolor="white",
    margin=dict(l=10, r=60, t=50, b=20), height=360,
    font=dict(family="Inter, sans-serif"),
)
fig1.update_xaxes(showgrid=True, gridcolor="#f0f0f0", title_font_size=12)
fig1.update_yaxes(tickfont_size=13)

with col_chart:
    st.plotly_chart(fig1, use_container_width=True)

top_city = ranking.iloc[-1]["ciudad"]
top_val = ranking.iloc[-1][poll_s1]
unit = POLLUTANT_LABELS[poll_s1].split("(")[-1].replace(")", "")
st.markdown(
    f'<div class="callout">📍 <strong>{top_city}</strong> encabeza el ranking con '
    f'un promedio de <strong>{top_val:.1f} {unit}</strong> en {year_s1}. '
    f'El rango entre la ciudad más limpia y la más contaminada puede ser de hasta '
    f'<strong>{ranking[poll_s1].max() / ranking[poll_s1].min():.1f}×</strong>.</div>',
    unsafe_allow_html=True,
)

st.divider()

# ─── SECCIÓN 2: Patrón semanal (heatmap) ─────────────────────────────────────

st.markdown("## 2 · ¿Respiramos peor en lunes? El patrón semanal")

st.markdown('<div class="narr">', unsafe_allow_html=True)
st.markdown("""
Si la contaminación fuera aleatoria, no veríamos ningún patrón consistente por día de semana.
Pero los motores de combustión, las fábricas y las cocinas industriales siguen horarios laborales.

El **mapa de calor** de abajo muestra el nivel promedio de contaminante hora a hora y día a día.
Los cuadros más oscuros son las horas más sucias. Busca los **martes y miércoles por la mañana**
en casi todas las ciudades para PM2.5 y NO₂: esas son las horas pico. El lunes arranca la semana,
pero el máximo acumulado llega a mitad. El sábado y domingo, el mapa se aclara notablemente.

El **ozono** tiene un comportamiento opuesto: aumenta en fines de semana porque hay menos
monóxido de nitrógeno (NO) que lo destruya —el llamado "efecto fin de semana".
""")
st.markdown('</div>', unsafe_allow_html=True)

wdf = load_weekly_pattern()

col1, col2 = st.columns(2)
with col1:
    city_s2 = st.selectbox("Ciudad", sorted(wdf["ciudad"].unique()), key="s2_city")
with col2:
    poll_s2 = st.selectbox(
        "Contaminante", list(POLLUTANT_LABELS.keys()),
        format_func=lambda x: POLLUTANT_LABELS[x], key="s2_poll"
    )

pivot = (
    wdf[wdf["ciudad"] == city_s2]
    .pivot_table(index="nombre_dia", columns="hora", values=poll_s2)
    .reindex([d for d in DAY_ORDER_EN if d in wdf["nombre_dia"].values])
)
pivot.index = [DAY_ES.get(d, d) for d in pivot.index]

fig2 = px.imshow(
    pivot,
    aspect="auto",
    color_continuous_scale=COLOR_SCALE,
    labels=dict(x="Hora del día", y="", color=POLLUTANT_LABELS[poll_s2]),
    title=f"Patrón semanal — {POLLUTANT_LABELS[poll_s2]} en {city_s2}",
)
fig2.update_layout(
    margin=dict(l=10, r=10, t=55, b=40), height=360,
    font=dict(family="Inter, sans-serif"),
    coloraxis_colorbar=dict(title=dict(text=unit, side="right")),
)
fig2.update_xaxes(
    tickvals=list(range(0, 24, 2)),
    ticktext=[f"{h:02d}h" for h in range(0, 24, 2)],
    title_text="Hora del día",
)

st.plotly_chart(fig2, use_container_width=True)

st.markdown('<div class="narr">', unsafe_allow_html=True)
st.markdown("""
**¿Por qué el lunes no siempre es el peor?** El lunes concentra el *arranque* de la semana:
motores fríos, más viajes en auto, industria a plena capacidad después del descanso. Pero el
acumulado de emisiones a lo largo de los días laborales puede hacer que martes o miércoles
sean aún peores —especialmente en ciudades con mucha industria como Monterrey.
""")
st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# ─── SECCIÓN 3: Serie temporal y COVID ───────────────────────────────────────

st.markdown("## 3 · El COVID-19 como experimento natural")

st.markdown('<div class="narr">', unsafe_allow_html=True)
st.markdown("""
En marzo de 2020, México entró en confinamiento. De la noche a la mañana, el tráfico
desapareció, las fábricas pararon y los cielos se despejaron. Fue el experimento científico
involuntario más grande de la historia: **¿qué pasa con el aire cuando los humanos se detienen?**

La respuesta fue inmediata. Las concentraciones de NO₂ —el marcador más directo del tráfico
vehicular— cayeron entre 30 y 45 % en todas las ciudades. El PM2.5 también bajó, aunque
menos, porque parte viene de fuentes que no pararon (industria pesada, incendios forestales).
El ozono, paradójicamente, *subió* en algunas ciudades: sin NO que lo destruya, se acumula.
""")
st.markdown('</div>', unsafe_allow_html=True)

monthly = load_monthly()

col1, col2, col3 = st.columns([2, 2, 1])
with col1:
    all_cities = sorted(monthly["ciudad"].unique())
    cities_s3 = st.multiselect(
        "Ciudades", all_cities,
        default=["Mexicali", "Monterrey", "CDMX"],
        key="s3_cities",
    )
with col2:
    poll_s3 = st.selectbox(
        "Contaminante", list(POLLUTANT_LABELS.keys()),
        format_func=lambda x: POLLUTANT_LABELS[x], key="s3_poll"
    )
with col3:
    all_years = sorted(monthly["anio"].unique())
    year_range = st.select_slider(
        "Rango de años", options=all_years,
        value=(all_years[0], all_years[-1]), key="s3_years"
    )

if not cities_s3:
    st.warning("Selecciona al menos una ciudad.")
else:
    mdf = monthly[
        monthly["ciudad"].isin(cities_s3) &
        (monthly["anio"] >= year_range[0]) &
        (monthly["anio"] <= year_range[1])
    ]

    fig3 = px.line(
        mdf, x="fecha_mes", y=poll_s3, color="ciudad",
        color_discrete_sequence=CITY_COLORS,
        labels={"fecha_mes": "", poll_s3: POLLUTANT_LABELS[poll_s3], "ciudad": "Ciudad"},
        title=f"Evolución mensual de {POLLUTANT_LABELS[poll_s3]} — {year_range[0]}–{year_range[1]}",
        markers=False,
    )

    # Franja de confinamiento COVID
    fig3.add_vrect(
        x0="2020-03-23", x1="2020-06-01",
        fillcolor="steelblue", opacity=0.13,
        layer="below", line_width=0,
        annotation_text="Confinamiento COVID-19",
        annotation_position="top left",
        annotation=dict(font_size=11, font_color="steelblue", font=dict(family="Inter")),
    )
    # Línea de inicio de reapertura
    fig3.add_vline(
        x="2020-06-01", line_dash="dot", line_color="steelblue", line_width=1,
        annotation_text="Reapertura gradual", annotation_position="top right",
        annotation=dict(font_size=10, font_color="steelblue"),
    )

    fig3.update_layout(
        plot_bgcolor="white", height=420,
        margin=dict(l=10, r=10, t=55, b=30),
        hovermode="x unified",
        font=dict(family="Inter, sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig3.update_xaxes(showgrid=True, gridcolor="#f0f0f0")
    fig3.update_yaxes(showgrid=True, gridcolor="#f0f0f0")

    st.plotly_chart(fig3, use_container_width=True)

st.markdown('<div class="narr">', unsafe_allow_html=True)
st.markdown("""
Después del confinamiento, los niveles regresaron casi al punto de partida en la mayoría
de las ciudades. Esto confirma que el problema es **estructural**: mientras dependamos de
combustibles fósiles para movernos e industrializarnos, el aire no mejorará por sí solo.
El COVID nos mostró que *sí es posible* respirar mejor —solo que la solución no puede
ser quedarnos en casa para siempre.
""")
st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# ─── CONCLUSIONES ─────────────────────────────────────────────────────────────

st.markdown("## Conclusiones")

st.markdown('<div class="narr">', unsafe_allow_html=True)
st.markdown("""
Los datos responden a nuestra pregunta con matices importantes:

**1. La contaminación sí tiene ritmo semanal**, pero el día más sucio no es el lunes:
el pico suele estar a mitad de semana (martes–jueves), cuando el acumulado de emisiones
laborales es mayor y la atmósfera lleva días absorbiendo. El lunes es el *arranque*, no el máximo.

**2. El COVID-19 fue la prueba definitiva** de que la fuente principal de contaminación
urbana en México es el transporte y la industria, no el clima ni la geografía. En pocas
semanas, el aire mejoró más de lo que han logrado décadas de política ambiental.

**3. Las ciudades no son iguales.** Mexicali sufre contaminación mixta (transfronteriza +
calefacción local). Monterrey tiene un perfil industrial marcado. La CDMX ha reducido
emisiones con política pública activa. Tijuana muestra que la cercanía a California tiene
efectos ambientales en ambas direcciones.

La conclusión es incómoda: **el aire mejora cuando paramos, y empeora cuando arrancamos**.
La pregunta de política pública ya no es técnica —es de voluntad colectiva.
""")
st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# ─── PIE DE PÁGINA ────────────────────────────────────────────────────────────

st.markdown(
    '<div class="footer">'
    '📊 <strong>Fuente de datos:</strong> SINAICA – Sistema Nacional de Información de la '
    'Calidad del Aire, INECC México '
    '(<a href="https://sinaica.inecc.gob.mx/" target="_blank">sinaica.inecc.gob.mx</a>) '
    '· Datos descargados: abril 2026<br>'
    '✍️ <strong>Autor:</strong> Alexander Góngora Venegas · '
    'Visualización gráfica para IA – Universidad Iberoamericana León<br>'
    '👩‍🏫 <strong>Docente:</strong> Dra. Dora Alvarado · Proyecto Final 3er Parcial 2026'
    '</div>',
    unsafe_allow_html=True,
)
