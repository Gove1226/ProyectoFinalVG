"""
 Visualización interactiva de calidad del aire en México
Autor: Alexander Góngora Venegas
Curso: Visualización gráfica para IA – Universidad Iberoamericana León
Docente: Dra. Dora Alvarado
"""

from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─── Configuración ────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="¿En qué día de la semana respiramos peor?",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

PROCESSED_DIR = Path(__file__).parent / "data" / "processed"

POLLUTANT_LABELS = {
    "PM25": "PM2.5 (μg/m³)",
    "O3":   "Ozono O₃ (ppm)",
    "NO2":  "Dióxido de nitrógeno NO₂ (ppm)",
    "CO":   "Monóxido de carbono CO (ppm)",
    "SO2":  "Dióxido de azufre SO₂ (ppm)",
}

POLLUTANT_DESC = {
    "PM25": "partículas finas suspendidas — las más dañinas para el sistema respiratorio",
    "O3":   "ozono troposférico — formado por reacción fotoquímica en días soleados",
    "NO2":  "dióxido de nitrógeno — marcador directo del tráfico vehicular",
    "CO":   "monóxido de carbono — producto de combustión incompleta",
    "SO2":  "dióxido de azufre — asociado a industria y generación eléctrica",
}

NORMAS = {
    "PM25": 45.0,
    "NO2":  0.21,
    "O3":   0.095,
    "CO":   11.0,
    "SO2":  0.2,
}

DAY_ORDER_EN = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DAY_ES = {
    "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
    "Thursday": "Jueves", "Friday": "Viernes", "Saturday": "Sábado", "Sunday": "Domingo",
}

MONTH_NAMES_ES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                  "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

CITY_COLORS = px.colors.qualitative.Set2


def _unit(poll: str) -> str:
    return POLLUTANT_LABELS[poll].split("(")[-1].replace(")", "")


# ─── CSS ──────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
  .block-container { padding-top: 1.8rem; max-width: 1200px; }
  h1 { font-size: 2.5rem !important; line-height: 1.2; }
  h2 { font-size: 1.55rem !important; color: #FF6B35 !important;
       border-bottom: 2px solid #FF6B35; padding-bottom: 6px; margin-top: 2rem; }
  .narr { font-size: 1.05rem; line-height: 1.75; color: #e0e0e0; }
  .callout {
    background: #2a2a2a;
    color: #ffffff;
    border-left: 4px solid #FF6B35;
    padding: 16px 18px;
    border-radius: 8px;
    margin: 14px 0;
    font-size: 1rem;
    line-height: 1.65;
  }
  .callout strong { color: #FF6B35; }
  .footer {
    font-size: 0.82rem; color: #888; margin-top: 50px;
    border-top: 1px solid #333; padding-top: 14px;
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


if not all((PROCESSED_DIR / f"{n}.parquet").exists()
           for n in ("daily_avg", "weekly_pattern", "monthly_series")):
    st.error(
        "No se encontraron datos procesados en `data/processed/`. "
        "Ejecuta primero:\n\n```\npython src/data_processing.py\n```"
    )
    st.stop()


# ─── ENCABEZADO ───────────────────────────────────────────────────────────────

st.title("¿En qué día de la semana respiramos peor?")
st.subheader("Patrones temporales de contaminación en CDMX, Monterrey y Guadalajara")

st.markdown("""
El aire que respiramos no es igual todos los días. En las ciudades mexicanas con peor calidad del
aire, la concentración de contaminantes sigue ritmos casi tan predecibles como el tráfico: sube
cuando arrancamos el motor, baja cuando nos quedamos en casa y colapsa cuando el mundo entero
se detiene.

Este análisis usa **datos reales de SINAICA** (Sistema Nacional de Información de la
Calidad del Aire, INECC) de estaciones de monitoreo en tres zonas metropolitanas:
**Ciudad de México**, **Monterrey** y **Guadalajara** — cubriendo 2019 a 2024
con cinco contaminantes: PM2.5, O₃, NO₂, CO y SO₂.

La pregunta que nos guía es simple: **¿hay un día de la semana en que el aire está peor?**
La respuesta es más interesante de lo que parece.
""")

m1, m2, m3 = st.columns(3)
m1.metric("Estaciones analizadas", "  47")
m2.metric("Período cubierto", "2019 – 2024")
m3.metric("Zonas metropolitanas", "3 ciudades")

st.divider()


# SECCIÓN 1 · Ranking de ciudades ------------------------------------------------------------------

st.markdown("## 1 · ¿Cuáles son las ciudades más contaminadas?")

st.markdown("""
Antes de entrar al análisis semanal, conviene ubicarnos en el mapa. No todas las ciudades
contaminan igual, y los factores que explican la diferencia van más allá del tamaño poblacional.

**Guadalajara** presenta en años recientes niveles de PM2.5 sorprendentemente altos —superando
a la CDMX—, ligados al crecimiento vehicular y a la quema agrícola estacional en el Bajío.

**Monterrey** tiene un perfil industrial marcado con emisiones de fundidoras, cementeras y la
refinería de Cadereyta. 

La **Ciudad de México**, contra la intuición popular, ha mejorado
considerablemente gracias al programa de verificación vehicular y la expansión del transporte público.
""")

daily = load_daily()

col_chart, col_ctrl = st.columns([3, 1])

with col_ctrl:
    st.markdown("**Controles**")
    poll_s1 = st.selectbox(
        "Contaminante", list(POLLUTANT_LABELS.keys()),
        format_func=lambda x: POLLUTANT_LABELS[x], key="s1_poll"
    )
    year_s1 = st.selectbox(
        "Año", sorted(daily["anio"].unique(), reverse=True), key="s1_year"
    )
    st.caption(f"Mostrando: {POLLUTANT_DESC[poll_s1]}")

unit_s1 = _unit(poll_s1)
min_year = int(daily["anio"].min())

current_avgs = daily[daily["anio"] == year_s1].groupby("ciudad")[poll_s1].mean().dropna()
prev_avgs = (
    daily[daily["anio"] == year_s1 - 1].groupby("ciudad")[poll_s1].mean().dropna()
    if year_s1 > min_year else pd.Series(dtype=float)
)

ranking = current_avgs.reset_index()
ranking.columns = ["ciudad", "valor"]
ranking = ranking.sort_values("valor", ascending=False)


def _yoy(city: str) -> str:
    if city not in prev_avgs.index:
        return "Sin dato año anterior"
    pct = (current_avgs[city] - prev_avgs[city]) / prev_avgs[city] * 100
    arrow = "↑" if pct > 0 else "↓"
    return f"{arrow} {abs(pct):.1f} % vs {year_s1 - 1}"


ranking["yoy"] = ranking["ciudad"].apply(_yoy)

fig1 = go.Figure(go.Bar(
    y=ranking["ciudad"],
    x=ranking["valor"],
    orientation="h",
    marker=dict(color=ranking["valor"], colorscale="Oranges", showscale=False),
    customdata=np.stack([ranking["ciudad"], ranking["yoy"]], axis=1),
    hovertemplate=(
        "<b>%{customdata[0]}</b><br>"
        f"Promedio {year_s1}: %{{x:.4g}} {unit_s1}<br>"
        "%{customdata[1]}<extra></extra>"
    ),
    text=ranking["valor"],
    texttemplate="%{text:.4g}",
    textposition="outside",
))
fig1.update_yaxes(autorange="reversed", tickfont_size=13, color="white")
fig1.update_xaxes(showgrid=True, gridcolor="#333333", color="white")
fig1.update_layout(
    title=f"Promedio anual de {POLLUTANT_LABELS[poll_s1]} por ciudad — {year_s1}",
    plot_bgcolor="#1e1e1e",
    paper_bgcolor="#1e1e1e",
    font=dict(color="white", family="Inter, sans-serif"),
    margin=dict(l=10, r=80, t=50, b=20), height=280,
)

with col_chart:
    st.plotly_chart(fig1, use_container_width=True)

if len(ranking) > 0:
    top_city = ranking.iloc[0]["ciudad"]
    top_val = ranking.iloc[0]["valor"]
    ratio_str = (
        f" El rango entre la más limpia y la más contaminada es de "
        f"<strong>{ranking['valor'].max() / ranking['valor'].min():.1f}×</strong>."
        if len(ranking) > 1 else ""
    )
    st.markdown(
        f'<div class="callout"> <strong>{top_city}</strong> encabeza el ranking con '
        f'un promedio de <strong>{top_val:.4g} {unit_s1}</strong> en {year_s1}.{ratio_str}</div>',
        unsafe_allow_html=True,
    )

st.divider()


# ─── SECCIÓN 2 · Heatmap semanal ─────────────────────────────────────────────

st.markdown("## 2 · El patrón semanal")

st.markdown("""
Si la contaminación fuera aleatoria, no veríamos ningún patrón consistente por día de semana.
Pero los motores de combustión, las fábricas y las cocinas industriales siguen horarios laborales.

El **mapa de calor** muestra el nivel promedio de contaminante hora a hora, día a día.
Las celdas más oscuras son las horas más sucias. Busca los **martes y miércoles por la mañana**
en casi todas las ciudades para PM2.5 y NO₂: esas son las horas pico. El sábado y domingo,
el mapa se aclara notablemente.

El **ozono** tiene un comportamiento opuesto: aumenta en fines de semana porque hay menos
monóxido de nitrógeno (NO) que lo destruya —el llamado "efecto fin de semana".
""")

wdf = load_weekly_pattern()

col1, col2 = st.columns(2)
with col1:
    city_s2 = st.selectbox("Ciudad", sorted(wdf["ciudad"].unique()), key="s2_city")
with col2:
    poll_s2 = st.selectbox(
        "Contaminante", list(POLLUTANT_LABELS.keys()),
        format_func=lambda x: POLLUTANT_LABELS[x], key="s2_poll"
    )

unit_s2 = _unit(poll_s2)
sub2 = wdf[wdf["ciudad"] == city_s2].copy()
pivot2 = (
    sub2.pivot_table(index="nombre_dia", columns="hora", values=poll_s2)
        .reindex([d for d in DAY_ORDER_EN if d in sub2["nombre_dia"].values])
)
pivot2.index = [DAY_ES.get(d, d) for d in pivot2.index]

vals2 = pivot2.values
global_avg2 = float(np.nanmean(vals2)) if not np.all(np.isnan(vals2)) else 1.0

hover2 = []
for day_es in pivot2.index:
    row = []
    for h in pivot2.columns:
        val = pivot2.loc[day_es, h]
        if pd.isna(val):
            row.append("Sin datos")
        else:
            pct = (val - global_avg2) / global_avg2 * 100
            sign = "sobre" if pct >= 0 else "bajo"
            row.append(
                f"<b>{day_es} {h:02d}h</b><br>"
                f"{poll_s2}: {val:.4g} {unit_s2}<br>"
                f"{abs(pct):.1f}% {sign} el promedio semanal"
            )
    hover2.append(row)

fig2 = go.Figure(go.Heatmap(
    z=vals2,
    x=[f"{h:02d}h" for h in pivot2.columns],
    y=list(pivot2.index),
    colorscale="Reds",
    text=hover2,
    hovertemplate="%{text}<extra></extra>",
    colorbar=dict(title=dict(text=unit_s2, side="right", font=dict(color="white")), tickfont=dict(color="white")),
))

if not np.all(np.isnan(vals2)):
    max_idx = np.unravel_index(np.nanargmax(vals2), vals2.shape)
    fig2.add_annotation(
        x=f"{pivot2.columns[max_idx[1]]:02d}h",
        y=list(pivot2.index)[max_idx[0]],
        text="pico máximo",
        showarrow=False,
        font=dict(color="white", size=9, family="Inter"),
    )

fig2.update_layout(
    title=f"Patrón semanal — {POLLUTANT_LABELS[poll_s2]} en {city_s2}",
    margin=dict(l=10, r=20, t=55, b=40), height=340,
    font=dict(color="white", family="Inter, sans-serif"),
    paper_bgcolor="#1e1e1e",
    plot_bgcolor="#1e1e1e",
    yaxis=dict(autorange="reversed", color="white"),
    xaxis=dict(
        tickvals=["00h", "06h", "12h", "18h", "23h"],
        ticktext=["00h", "06h", "12h", "18h", "23h"],
        title_text="Hora del día",
        color="white",
    ),
)

st.plotly_chart(fig2, use_container_width=True)

st.markdown("""
**¿Por qué el lunes no siempre es el peor?** El lunes concentra el *arranque* de la semana:
motores fríos, más viajes en auto, industria a plena capacidad después del descanso. Pero el
acumulado de emisiones a lo largo de los días laborales puede hacer que martes o miércoles
sean aún peores (especialmente en ciudades con mucha industria como Monterrey.)
""")

st.divider()


# ─── SECCIÓN 3 · Boxplot mensual ─────────────────────────────────────────────

st.markdown("## 3 · ¿Qué meses respiramos peor?")

st.markdown("""
No todos los meses son iguales. La quema agrícola de primavera, los incendios forestales de
verano y la inversión térmica de invierno crean patrones estacionales tan consistentes como el
calendario mismo. Cada caja muestra la dispersión real de todos los días de ese mes entre 2019
y 2024: la línea central es la mediana, la caja contiene el 50% de los días, y los puntos son
episodios extraordinarios.
""")

poll_s3 = st.selectbox(
    "Contaminante", list(POLLUTANT_LABELS.keys()),
    format_func=lambda x: POLLUTANT_LABELS[x], key="s3_poll"
)
unit_s3 = _unit(poll_s3)

box_df = daily[["ciudad", "mes", poll_s3]].dropna(subset=[poll_s3]).copy()
box_df["mes_nombre"] = box_df["mes"].apply(lambda m: MONTH_NAMES_ES[int(m) - 1])
box_df["mes_nombre"] = pd.Categorical(box_df["mes_nombre"], categories=MONTH_NAMES_ES, ordered=True)
box_df = box_df.sort_values("mes_nombre")

fig_box = px.box(
    box_df, x="mes_nombre", y=poll_s3, color="ciudad",
    points="outliers",
    color_discrete_sequence=CITY_COLORS,
    labels={"mes_nombre": "Mes", poll_s3: POLLUTANT_LABELS[poll_s3], "ciudad": "Ciudad"},
    title=f"Distribución mensual de {POLLUTANT_LABELS[poll_s3]} por ciudad (2019–2024)",
    category_orders={"mes_nombre": MONTH_NAMES_ES},
)

if poll_s3 in NORMAS:
    fig_box.add_hline(
        y=NORMAS[poll_s3],
        line_dash="dot", line_color="#aaaaaa", line_width=1.5,
        annotation_text=f"Norma NOM-025: {NORMAS[poll_s3]} {unit_s3}",
        annotation_position="bottom right",
        annotation=dict(font_size=10, font_color="#aaaaaa"),
    )

fig_box.update_layout(
    plot_bgcolor="#1e1e1e",
    paper_bgcolor="#1e1e1e",
    height=480,
    margin=dict(l=10, r=10, t=55, b=40),
    font=dict(color="white", family="Inter, sans-serif"),
    legend=dict(title_text="Ciudad", orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=dict(showgrid=False, color="white"),
    yaxis=dict(showgrid=True, gridcolor="#333333", color="white"),
    boxmode="group",
)

st.plotly_chart(fig_box, use_container_width=True)

st.markdown("""
Guadalajara muestra sus peores meses entre marzo y mayo — la temporada de quema de caña y
rastrojo agrícola en el Bajío. Monterrey, en cambio, tiene picos en invierno por la inversión
térmica que atrapa las emisiones industriales cerca del suelo. La CDMX es la más estable del
grupo: sus programas de verificación vehicular han aplanado los picos estacionales.
""")

st.divider()


# ─── SECCIÓN 4 · Serie temporal y COVID ──────────────────────────────────────

st.markdown("## 4 · El COVID-19 como experimento natural")

st.markdown("""
En marzo de 2020, México entró en confinamiento. De la noche a la mañana, el tráfico
desapareció, las fábricas pararon y los cielos se despejaron. Fue el experimento científico
involuntario más grande de la historia: **¿qué pasa con el aire cuando los humanos se detienen?**

La respuesta fue inmediata. Las concentraciones de NO₂ —el marcador más directo del tráfico
vehicular— cayeron entre 30 y 45 % en todas las ciudades. El PM2.5 también bajó, aunque
menos, porque parte viene de fuentes que no pararon (industria pesada, incendios forestales).
El ozono, paradójicamente, *subió* en algunas ciudades: sin NO que lo destruya, se acumula.
""")

monthly = load_monthly()
monthly = monthly[monthly["anio"] <= 2024].copy()

col1, col2 = st.columns(2)
with col1:
    all_cities = sorted(monthly["ciudad"].unique())
    cities_s4 = st.multiselect(
        "Ciudades", all_cities,
        default=[c for c in ["CDMX", "Monterrey", "Guadalajara"] if c in all_cities],
        key="s4_cities"
    )
with col2:
    poll_s4 = st.selectbox(
        "Contaminante", list(POLLUTANT_LABELS.keys()),
        format_func=lambda x: POLLUTANT_LABELS[x], key="s4_poll"
    )

all_years = sorted(monthly["anio"].unique())
years_s4 = st.multiselect("Años a mostrar", all_years, default=all_years, key="s4_years")

unit_s4 = _unit(poll_s4)

if not cities_s4:
    st.warning("Selecciona al menos una ciudad.")
elif not years_s4:
    st.warning("Selecciona al menos un año.")
else:
    mdf = monthly[
        monthly["ciudad"].isin(cities_s4) &
        monthly["anio"].isin(years_s4)
    ].dropna(subset=[poll_s4])

    fig3 = px.line(
        mdf, x="fecha_mes", y=poll_s4, color="ciudad",
        color_discrete_sequence=CITY_COLORS,
        labels={"fecha_mes": "", poll_s4: POLLUTANT_LABELS[poll_s4], "ciudad": "Ciudad"},
        title=f"Evolución mensual de {POLLUTANT_LABELS[poll_s4]}",
        markers=True,
    )

    # Banda confinamiento estricto (mar-jun 2020)
    fig3.add_vrect(
        x0="2020-03", x1="2020-06",
        fillcolor="rgba(255,100,100,0.25)", line_width=0, layer="below",
    )

    # Banda restricciones parciales (jun 2020-may 2023)
    fig3.add_vrect(
        x0="2020-06", x1="2023-05",
        fillcolor="rgba(255,100,100,0.08)", line_width=0, layer="below",
    )

    # Línea vertical inicio
    fig3.add_shape(
        type="line",
        x0="2020-03", x1="2020-03", y0=0, y1=1,
        xref="x", yref="paper",
        line=dict(color="rgba(255,255,255,0.6)", width=2, dash="dash"),
    )

    # Anotación inicio confinamiento
    fig3.add_annotation(
        x="2020-03", y=1, xref="x", yref="paper",
        text="← Confinamiento estricto",
        showarrow=False,
        font=dict(color="#ff6b6b", size=10, family="Inter"),
        bgcolor="rgba(60,20,20,0.85)",
        bordercolor="#e74c3c", borderwidth=1, borderpad=4,
        yanchor="bottom", xanchor="left",
    )

    # Anotación fin emergencia sanitaria
    fig3.add_annotation(
        x="2023-05", y=0.92, xref="x", yref="paper",
        text="Fin emergencia sanitaria →",
        showarrow=False,
        font=dict(color="#ff6b6b", size=10, family="Inter"),
        bgcolor="rgba(60,20,20,0.85)",
        bordercolor="#e74c3c", borderwidth=1, borderpad=4,
        yanchor="bottom", xanchor="right",
    )
    )

    if poll_s4 in NORMAS:
        fig3.add_hline(
            y=NORMAS[poll_s4],
            line_dash="dot", line_color="#aaaaaa", line_width=1.5,
            annotation_text=f"Norma NOM-025: {NORMAS[poll_s4]} {unit_s4}",
            annotation_position="bottom right",
            annotation=dict(font_size=10, font_color="#aaaaaa"),
        )

    fig3.update_layout(
        plot_bgcolor="#1e1e1e",
        paper_bgcolor="#1e1e1e",
        height=450,
        margin=dict(l=10, r=10, t=55, b=30),
        hovermode="x unified",
        font=dict(color="white", family="Inter, sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig3.update_xaxes(showgrid=True, gridcolor="#333333", color="white")
    fig3.update_yaxes(showgrid=True, gridcolor="#333333", color="white")

    st.plotly_chart(fig3, use_container_width=True)

st.markdown("""
Después del confinamiento, los niveles regresaron casi al punto de partida en la mayoría
de las ciudades. Esto confirma que el problema es **estructural**: mientras dependamos de
combustibles fósiles para movernos e industrializarnos, el aire no mejorará por sí solo.
El COVID nos mostró que *sí es posible* respirar mejor —solo que la solución no puede
ser quedarnos en casa para siempre.
""")

st.divider()


# ─── CONCLUSIONES ─────────────────────────────────────────────────────────────

st.markdown("## Conclusiones")

st.markdown("""
Los datos responden a nuestra pregunta con matices importantes:

**1. La contaminación sí tiene ritmo semanal**, pero el día más sucio no es el lunes:
el pico suele estar a mitad de semana (martes–jueves), cuando el acumulado de emisiones
laborales es mayor y la atmósfera lleva días absorbiendo. El lunes es el *arranque*, no el máximo.

**2. El COVID-19 fue la prueba definitiva** de que la fuente principal de contaminación
urbana en México es el transporte y la industria, no el clima ni la geografía. En pocas
semanas, el aire mejoró más de lo que han logrado décadas de política ambiental.

**3. Las ciudades no son iguales.** Guadalajara, sorprendentemente, supera a la CDMX en PM2.5
en años recientes: su crecimiento urbano no planificado y la quema agrícola regional explican
el deterioro. Monterrey mantiene niveles altos de CO y SO₂ ligados a su actividad industrial.
La CDMX ha logrado reducciones reales de PM2.5 gracias a política pública activa, aunque
el rebote post-COVID fue rápido y completo.

La conclusión es incómoda: **el aire mejora cuando paramos, y empeora cuando arrancamos**.
La pregunta de política pública ya no es técnica —es de voluntad colectiva.
""")

st.divider()


# ─── PIE DE PÁGINA ────────────────────────────────────────────────────────────

st.markdown(
    '<div class="footer">'
    'Autor: Alexander Góngora Venegas &nbsp;|&nbsp; '
    'Fuente: SINAICA – INECC '
    '(<a href="https://sinaica.inecc.gob.mx/" target="_blank">sinaica.inecc.gob.mx</a>) '
    '&nbsp;|&nbsp; Datos descargados: abril 2026'
    '</div>',
    unsafe_allow_html=True,
)