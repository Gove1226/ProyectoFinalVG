# 💨 ¿Respiramos peor en lunes?
### Patrones de contaminación en las ciudades más sucias de México (2019–2024)

**Autor:** Alexander Góngora Venegas  
**Curso:** Visualización gráfica para IA – Universidad Iberoamericana León  
**Docente:** Dra. Dora Alvarado  
**Parcial:** 3er Parcial 2026  
**Herramienta:** Streamlit + Plotly  
**Despliegue:** [Ver aplicación en Streamlit Cloud](https://PENDIENTE.streamlit.app) ← _actualizar tras deploy_

---

## Descripción del proyecto

Narrativa visual interactiva estilo "artículo de datos" que analiza la calidad del aire en seis
ciudades mexicanas (Mexicali, Monterrey, CDMX, Guadalajara, Puebla y Tijuana) durante el período
2019–2024. La historia sigue tres preguntas en orden:

1. ¿Cuáles son las ciudades con peor calidad del aire en México?
2. ¿La contaminación varía según el día de la semana? ¿El lunes es realmente el peor?
3. ¿Qué reveló el confinamiento por COVID-19 sobre las fuentes de contaminación?

## Pregunta de investigación

> **¿Hay patrones temporales claros en la contaminación del aire en las ciudades mexicanas más contaminadas?**  
> Específicamente: ¿respiramos peor en lunes?

## Fuente de datos

**SINAICA** – Sistema Nacional de Información de la Calidad del Aire  
Instituto Nacional de Ecología y Cambio Climático (INECC), Gobierno de México  
🔗 https://sinaica.inecc.gob.mx/  
📅 Fecha de descarga: abril 2026

> **Nota sobre los datos:** Esta versión del proyecto utiliza datos sintéticos generados con
> patrones reales documentados por SINAICA (estacionalidad, efectos COVID, variación semanal).
> Para sustituirlos por datos reales, coloca los CSVs de SINAICA en `data/raw/` y vuelve a
> ejecutar `python src/data_processing.py`.

## Estructura del proyecto

```
ProyectoFInalVG/
├── data/
│   ├── raw/              ← CSVs originales (o sintéticos generados por el script)
│   └── processed/        ← Datos limpios y agregados (generados por data_processing.py)
├── notebooks/
│   └── 01_limpieza.ipynb ← Análisis exploratorio y limpieza documentada
├── src/
│   └── data_processing.py ← Pipeline de limpieza y transformación reproducible
├── app.py                ← Aplicación Streamlit
├── requirements.txt
├── README.md
└── .gitignore
```

## Instalación y ejecución local

### Requisitos previos
- Python 3.10 o superior
- pip

### Paso a paso

```bash
# 1. Clona el repositorio
git clone https://github.com/Gove1226/ProyectoFinalVG.git
cd ProyectoFinalVG

# 2. Crea un entorno virtual (recomendado)
python -m venv venv
# En Windows:
venv\Scripts\activate
# En macOS/Linux:
source venv/bin/activate

# 3. Instala las dependencias
pip install -r requirements.txt

# 4. Genera y procesa los datos
python src/data_processing.py
# (Si ya tienes CSVs reales en data/raw/, el script los usa directamente)
# Para forzar regeneración de datos sintéticos:
# python src/data_processing.py --generate

# 5. Ejecuta la aplicación
streamlit run app.py
```

La app quedará disponible en `http://localhost:8501`.

## Visualizaciones incluidas

| # | Tipo | Variable | Interactividad |
|---|------|----------|---------------|
| 1 | Barras horizontales | Promedio anual por ciudad | Selector de contaminante y año |
| 2 | Heatmap día×hora | Nivel de contaminante | Selector de ciudad y contaminante |
| 3 | Línea temporal mensual | Evolución 2019–2024 | Multi-select ciudades, rango años, contaminante |

## Contaminantes analizados

| Clave | Nombre | Unidad | Fuente principal |
|-------|--------|--------|-----------------|
| PM25 | Partículas finas | μg/m³ | Tráfico, industria, quema de biomasa |
| O3 | Ozono troposférico | ppb | Reacción fotoquímica (sol + NOx + VOCs) |
| NO2 | Dióxido de nitrógeno | ppb | Escapes vehiculares |
| CO | Monóxido de carbono | ppm | Combustión incompleta |
| SO2 | Dióxido de azufre | ppb | Industria, generación eléctrica |

## Despliegue en Streamlit Community Cloud

1. Sube el repositorio a GitHub (público)
2. Ve a [share.streamlit.io](https://share.streamlit.io) e inicia sesión con GitHub
3. Crea una nueva app apuntando a `app.py` en la rama `main`
4. En los "Secrets" o antes del primer run, agrega un paso para ejecutar `data_processing.py`
   (o incluye los archivos procesados en el repo)

> **Tip:** Para que Streamlit Cloud genere los datos al iniciar, agrega este bloque al inicio
> de `app.py` antes del guard de datos (ya está implementado).

---

_Proyecto desarrollado con fines académicos. Datos para ilustración basados en patrones reales._
