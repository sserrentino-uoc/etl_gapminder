"""ETL para la PEC 2 de Visualización de Datos.

Genera, a partir del dataset público Gapminder, cuatro salidas:

1. Dataset original depurado.
2. Dataset para Area Chart (Grupo I).
3. Dataset para Circular Dendrogram (Grupo II).
4. Dataset para Beeswarm Chart (Grupo III).

El script sigue una lógica ETL (Extract, Transform, Load) y agrega un
resumen básico de calidad de datos para poder documentar el proceso en la
memoria de la PEC.

Requisitos:
    pip install pandas plotly
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pandas as pd
import plotly.express as px

OUTPUT_DIR: Final[Path] = Path(__file__).resolve().parent
ORIGINAL_DATASET_NAME: Final[str] = "gapminder_original.csv"
AREA_OUTPUT_NAME: Final[str] = "tecnica1_area_flourish_gapminder.csv"
DENDRO_OUTPUT_NAME: Final[str] = "tecnica2_dendro_jerarquia_gapminder.csv"
BEESWARM_OUTPUT_NAME: Final[str] = "tecnica3_beeswarm_gapminder.csv"
QUALITY_REPORT_NAME: Final[str] = "resumen_calidad_datos.txt"
REFERENCE_YEAR: Final[int] = 2007

REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "country",
    "continent",
    "year",
    "lifeExp",
    "pop",
    "gdpPercap",
)

NUMERIC_COLUMNS: Final[tuple[str, ...]] = (
    "year",
    "lifeExp",
    "pop",
    "gdpPercap",
)


class DataValidationError(Exception):
    """Error lanzado cuando el dataset no cumple requisitos mínimos."""


def extract_data() -> pd.DataFrame:
    """Extrae el dataset original.

    Se utiliza el dataset público Gapminder incluido en ``plotly.express``.
    Se parte de un único origen para mantener coherencia entre las tres
    visualizaciones exigidas por la PEC.

    Returns:
        Dataset original cargado en memoria.
    """
    return px.data.gapminder().copy()


def validate_columns(df: pd.DataFrame) -> None:
    """Valida la existencia de columnas requeridas.

    Args:
        df: Dataset a validar.

    Raises:
        DataValidationError: Si falta alguna columna crítica.
    """
    missing_columns: list[str] = [
        column for column in REQUIRED_COLUMNS if column not in df.columns
    ]

    if missing_columns:
        missing_str: str = ", ".join(missing_columns)
        raise DataValidationError(
            f"Faltan columnas requeridas en el dataset: {missing_str}."
        )


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos de datos en columnas cuantitativas.

    Args:
        df: Dataset original.

    Returns:
        Copia del dataset con columnas numéricas convertidas.
    """
    normalized_df: pd.DataFrame = df.copy()

    for column in NUMERIC_COLUMNS:
        normalized_df[column] = pd.to_numeric(
            normalized_df[column],
            errors="coerce",
        )

    return normalized_df


def drop_critical_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina registros con nulos en variables críticas.

    Args:
        df: Dataset previamente normalizado.

    Returns:
        Dataset depurado.
    """
    return df.dropna(subset=list(REQUIRED_COLUMNS)).copy()


def standardize_text(df: pd.DataFrame) -> pd.DataFrame:
    """Estandariza columnas categóricas relevantes.

    Args:
        df: Dataset depurado.

    Returns:
        Dataset con texto estandarizado.
    """
    standardized_df: pd.DataFrame = df.copy()
    standardized_df["country"] = standardized_df["country"].astype(str).str.strip()
    standardized_df["continent"] = standardized_df["continent"].astype(str).str.strip()

    return standardized_df


def run_quality_checks(df: pd.DataFrame) -> None:
    """Ejecuta chequeos mínimos de calidad.

    Args:
        df: Dataset listo para transformar.

    Raises:
        DataValidationError: Si se detecta un problema crítico.
    """
    if df.empty:
        raise DataValidationError("El dataset quedó vacío tras la limpieza.")

    if (df["pop"] <= 0).any():
        raise DataValidationError(
            "Se detectaron valores no válidos en 'pop' (<= 0)."
        )

    if (df["lifeExp"] <= 0).any():
        raise DataValidationError(
            "Se detectaron valores no válidos en 'lifeExp' (<= 0)."
        )

    if df["year"].isna().any():
        raise DataValidationError(
            "Se detectaron valores nulos en 'year' tras la normalización."
        )


def prepare_clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica la limpieza base del dataset original.

    Args:
        df: Dataset original.

    Returns:
        Dataset limpio y validado.
    """
    validate_columns(df)
    cleaned_df: pd.DataFrame = normalize_types(df)
    cleaned_df = drop_critical_nulls(cleaned_df)
    cleaned_df = standardize_text(cleaned_df)
    run_quality_checks(cleaned_df)

    return cleaned_df


def build_area_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Construye el dataset para Area Chart.

    La población se agrega por año y continente, y luego se pivotea a formato
    ancho para facilitar la carga en Flourish.

    Args:
        df: Dataset limpio.

    Returns:
        Dataset en formato wide para Area Chart.
    """
    return (
        df.groupby(["year", "continent"], as_index=False)["pop"]
        .sum()
        .pivot(index="year", columns="continent", values="pop")
        .reset_index()
        .fillna(0)
        .sort_values("year")
    )


def build_dendrogram_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Construye el dataset para Circular Dendrogram.

    Se selecciona un único año para evitar múltiples observaciones por país y
    construir una jerarquía estática coherente.

    Args:
        df: Dataset limpio.

    Returns:
        Dataset jerárquico con variable de tamaño.

    Raises:
        DataValidationError: Si no hay datos para el año de referencia.
    """
    reference_df: pd.DataFrame = df[df["year"] == REFERENCE_YEAR].copy()

    if reference_df.empty:
        raise DataValidationError(
            f"No existen datos para el año de referencia {REFERENCE_YEAR}."
        )

    dendro_df: pd.DataFrame = (
        reference_df.groupby(["continent", "country"], as_index=False)["pop"]
        .sum()
    )
    dendro_df.insert(0, "nivel1", "Mundo")
    dendro_df = dendro_df.rename(
        columns={
            "continent": "nivel2",
            "country": "nivel3",
            "pop": "valor",
        }
    )

    return dendro_df


def build_beeswarm_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Construye el dataset para Beeswarm Chart.

    A diferencia del Area Chart, aquí no se agrega. Se conserva una fila por
    país en el año de referencia para preservar la granularidad.

    Args:
        df: Dataset limpio.

    Returns:
        Dataset para Beeswarm.

    Raises:
        DataValidationError: Si no hay datos para el año de referencia.
    """
    reference_df: pd.DataFrame = df[df["year"] == REFERENCE_YEAR].copy()

    if reference_df.empty:
        raise DataValidationError(
            f"No existen datos para el año de referencia {REFERENCE_YEAR}."
        )

    return reference_df[
        ["continent", "country", "lifeExp", "pop", "gdpPercap"]
    ].copy()


def build_quality_summary(
    original_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    area_df: pd.DataFrame,
    dendro_df: pd.DataFrame,
    beeswarm_df: pd.DataFrame,
) -> str:
    """Genera un resumen textual de calidad y transformación de datos.

    Args:
        original_df: Dataset antes de limpiar.
        cleaned_df: Dataset limpio.
        area_df: Dataset derivado para Area Chart.
        dendro_df: Dataset derivado para Circular Dendrogram.
        beeswarm_df: Dataset derivado para Beeswarm.

    Returns:
        Texto listo para guardar o citar en la memoria.
    """
    dropped_rows: int = len(original_df) - len(cleaned_df)
    null_counts = cleaned_df[list(REQUIRED_COLUMNS)].isna().sum()

    lines: list[str] = [
        "Resumen básico de calidad de datos - PEC 2",
        "",
        f"Filas originales: {len(original_df)}",
        f"Filas tras limpieza: {len(cleaned_df)}",
        f"Filas eliminadas en limpieza: {dropped_rows}",
        "",
        "Columnas requeridas verificadas:",
    ]
    lines.extend([f"- {column}" for column in REQUIRED_COLUMNS])
    lines.extend(
        [
            "",
            "Nulos remanentes en columnas requeridas tras la limpieza:",
        ]
    )
    lines.extend([f"- {column}: {int(count)}" for column, count in null_counts.items()])
    lines.extend(
        [
            "",
            "Rangos y cardinalidades principales:",
            f"- Años disponibles: {int(cleaned_df['year'].min())} a "
            f"{int(cleaned_df['year'].max())}",
            f"- Continentes: {cleaned_df['continent'].nunique()}",
            f"- Países: {cleaned_df['country'].nunique()}",
            "",
            "Salidas generadas:",
            f"- Area Chart: {len(area_df)} filas",
            f"- Circular Dendrogram: {len(dendro_df)} filas",
            f"- Beeswarm: {len(beeswarm_df)} filas",
            "",
            "Nota metodológica:",
            "- El Area Chart agrega por año y continente, por lo que pierde "
            "granularidad a nivel país.",
            "- El Circular Dendrogram y el Beeswarm trabajan con una fotografía "
            f"del año {REFERENCE_YEAR} para evitar duplicidad temporal.",
        ]
    )

    return "\n".join(lines)


def save_dataset(df: pd.DataFrame, output_dir: Path, filename: str) -> Path:
    """Guarda un dataset en formato CSV.

    Args:
        df: DataFrame a guardar.
        output_dir: Carpeta de salida.
        filename: Nombre del archivo.

    Returns:
        Ruta del archivo generado.
    """
    output_path: Path = output_dir / filename
    df.to_csv(output_path, index=False)

    return output_path


def save_text_report(content: str, output_dir: Path, filename: str) -> Path:
    """Guarda un reporte textual.

    Args:
        content: Texto del reporte.
        output_dir: Carpeta de salida.
        filename: Nombre del archivo.

    Returns:
        Ruta del archivo generado.
    """
    output_path: Path = output_dir / filename
    output_path.write_text(content, encoding="utf-8")

    return output_path


def main() -> None:
    """Ejecuta el pipeline ETL completo."""
    output_dir: Path = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    original_df: pd.DataFrame = extract_data()
    cleaned_df: pd.DataFrame = prepare_clean_dataset(original_df)

    area_df: pd.DataFrame = build_area_dataset(cleaned_df)
    dendro_df: pd.DataFrame = build_dendrogram_dataset(cleaned_df)
    beeswarm_df: pd.DataFrame = build_beeswarm_dataset(cleaned_df)

    quality_summary: str = build_quality_summary(
        original_df=original_df,
        cleaned_df=cleaned_df,
        area_df=area_df,
        dendro_df=dendro_df,
        beeswarm_df=beeswarm_df,
    )

    generated_files: dict[str, Path] = {
        "original": save_dataset(cleaned_df, output_dir, ORIGINAL_DATASET_NAME),
        "area": save_dataset(area_df, output_dir, AREA_OUTPUT_NAME),
        "dendrogram": save_dataset(dendro_df, output_dir, DENDRO_OUTPUT_NAME),
        "beeswarm": save_dataset(beeswarm_df, output_dir, BEESWARM_OUTPUT_NAME),
        "quality_report": save_text_report(
            quality_summary,
            output_dir,
            QUALITY_REPORT_NAME,
        ),
    }

    print("Proceso ETL completado correctamente.")
    print("Archivos generados:")
    for label, path in generated_files.items():
        print(f"- {label}: {path}")

    print("\nResumen de calidad de datos:")
    print(quality_summary)


if __name__ == "__main__":
    main()
