# ¿En qué día de la semana respiramos peor?
### Patrones de contaminación en las ciudades más sucias de México (2019–2024)

**Autor:** Alexander Góngora Venegas  
**Materia:** Visualización gráfica para IA – Universidad Iberoamericana León  
**Profesor:** Dra. Dora Alvarado  
**Herramienta:** Streamlit + Plotly  
**Despliegue:** https://proyecto-final-vg-cuando-respiramos-peor.streamlit.app/

---

## Descripción del proyecto

Narrativa visual interactiva que analiza la calidad del aire en 47 estaciones de monitoreo
distribuidas en tres zonas metropolitanas: Ciudad de México, Monterrey y Guadalajara, durante
el período 2019–2024. La historia sigue cuatro preguntas en orden:

1. ¿Cuáles son las ciudades con peor calidad del aire en México?
2. ¿La contaminación varía según el día de la semana? ¿El lunes es realmente el peor?
3. ¿Qué meses del año respiramos peor?
4. ¿Qué reveló el confinamiento por COVID-19 sobre las fuentes de contaminación?

## Pregunta de investigación

> **¿Hay patrones temporales claros en la contaminación del aire en las ciudades mexicanas más contaminadas?**  


## Fuente de datos

**SINAICA** – Sistema Nacional de Información de la Calidad del Aire  
Instituto Nacional de Ecología y Cambio Climático (INECC), Gobierno de México  
https://sinaica.inecc.gob.mx/  
Fecha de descarga: abril 2026

## Estructura del proyecto

```
ProyectoFinalVG/
├── data/
│   ├── raw/
│   │   └── base de datos VG/  ← CSVs originales
│   └── processed/             ← Datos limpios y agregados generados por data_processing.py
├── scripts/
│   └── data_processing.py     ← Pipeline de limpieza y transformación reproducible
├── app.py                     ← Aplicación Streamlit
├── requirements.txt
├── README.md
└── .gitignore
```

## Instalación y ejecución local

### Paso a paso

```bash
# 1. Clona el repositorio
git clone https://github.com/Gove1226/ProyectoFinalVG.git
cd ProyectoFinalVG

# 2. Crea un entorno virtual 
python -m venv venv
# En Windows:
venv\Scripts\activate
# En macOS/Linux:
source venv/bin/activate

# 3. Instalar las dependencias
pip install -r requirements.txt

# 4. Genera y procesa los datos
python scripts/data_processing.py

# 5. Ejecuta la aplicación
streamlit run app.py
```

La app quedará disponible en "http://localhost:8501".

## Visualizaciones incluidas

| # | Tipo | Sección | Interactividad |
|---|------|---------|---------------|
| 1 | Barras horizontales | Ciudades más contaminadas | Selector de contaminante y año |
| 2 | Heatmap día×hora | Patrón semanal | Selector de ciudad y contaminante |
| 3 | Box plot mensual | Distribución por mes (2019–2024) | Selector de contaminante |
| 4 | Línea temporal mensual | Evolución COVID-19 | Selector de ciudad, contaminante y rango de años |

## Contaminantes analizados

| Clave | Nombre | Unidad | Fuente principal |
|-------|--------|--------|-----------------|
| PM25 | Partículas finas | μg/m³ | Tráfico, industria, quema de biomasa |
| O3 | Ozono troposférico | ppb | Reacción fotoquímica (sol + NOx + VOCs) |
| NO2 | Dióxido de nitrógeno | ppb | Escapes vehiculares |
| CO | Monóxido de carbono | ppm | Combustión incompleta |
| SO2 | Dióxido de azufre | ppb | Industria, generación eléctrica |

## Despliegue en Streamlit Community Cloud

1. Subir el repositorio a GitHub 
2. Entrar a streamlit e iniciar sesión con GitHub
3. Crea una nueva app apuntando a "app.py" en la rama "main"
4. En "Advanced settings" forzar a python 3.12 para evitar errores

