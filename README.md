# 📊 PEC 2: De la Técnica hacia el Dato

> **Asignatura:** Visualización de Datos - Master en Ciencia de Datos

> **Estudiante:** Sebastián Serrentino Mangino  

> **Institución:** Universitat Oberta de Catalunya (UOC)  

> **Docente:** José Manuel Pina López  

---

## 🚀 Descripción del Proyecto

Este repositorio contiene el **motor de procesamiento de datos (ETL)** desarrollado para la resolución de la PEC 2. El proyecto se aleja del enfoque convencional de "cargar y dibujar", adoptando la estrategia **"de la técnica hacia el dato"**. 

A partir del dataset público de **Gapminder** (https://www.gapminder.org/data/), el script transforma la información original en tres topologías distintas, optimizadas para cumplir con los requisitos geométricos de las visualizaciones interactivas en **Flourish**(https://flourish.studio/).

---

## 🛠️ Arquitectura del Pipeline (`codigo.py`)

El script implementa un flujo de ingeniería de datos modular bajo el paradigma **Extract, Transform, Load (ETL)**:

### 1. Extracción (Extract)
* **Fuente:** Dataset `gapminder` obtenido a través de la librería `plotly.express`. (https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv)
* **Carga:** Ingesta automática para asegurar la reproducibilidad del análisis.

### 2. Transformación (Transform)
* **Validación de Calidad:** Limpieza de nulos, tipado estricto de variables numéricas y comprobación de integridad de columnas.
* **Especialización:**
    * **Area Chart:** Pivotaje de formato *Long* a *Wide* para series temporales por continente.
    * **Circular Dendrogram:** Construcción de una jerarquía multinivel `Mundo > Continente > País`.
    * **Beeswarm Chart:** Filtrado al año de referencia (2007) preservando la granularidad del microdato.

### 3. Carga (Load)
* Generación de 3 datasets específicos en formato `.csv`.
* Creación automatizada de un **Log de Auditoría** (`resumen_calidad_datos.txt`) que garantiza la trazabilidad del volumen de datos y los registros filtrados.

---

## 📁 Estructura de Archivos

| Archivo | Descripción |
| :--- | :--- |
| `codigo.py` | Script principal en Python 3 con la lógica ETL. |
| `requirements.txt` | Dependencias del proyecto (`pandas`, `plotly`). |
| `tecnica1_area_flourish_gapminder.csv` | Dataset para el Gráfico de Áreas Apiladas. |
| `tecnica2_dendro_jerarquia_gapminder.csv` | Dataset jerárquico para el Dendrograma Circular. |
| `tecnica3_beeswarm_gapminder.csv` | Dataset granular para el Gráfico de Enjambre. |
| `resumen_calidad_datos.txt` | Reporte de trazabilidad y calidad de datos. |

---

## 💻 Instalación y Uso

1. **Clonar el repositorio:**
   ```bash
   git clone <TU_URL_DE_GITLAB>
   python codigo.py

## 🔗 Visualizaciones Finales (Flourish)

* Los resultados interactivos y el análisis visual de este procesamiento pueden consultarse en los siguientes enlaces:

   * 🌐 Técnica 1: Area Chart - Evolución poblacional - url: https://public.flourish.studio/visualisation/28153341/
   * 🌳 Técnica 2: Circular Dendrogram - Estructura jerárquica global - url: https://public.flourish.studio/visualisation/28154374/
   * 🐝 Técnica 3: Beeswarm Chart - Distribución de la esperanza de vida - url: https://public.flourish.studio/visualisation/28154890/


**Este repositorio ha sido creado exclusivamente para fines académicos en el marco de la asignatura Visualización de Datos - Master en Ciencia de Datos - OUC.**
