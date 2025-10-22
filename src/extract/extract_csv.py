import pandas as pd


def extraer_csv(ruta_csv):
    datos = pd.read_csv(ruta_csv)
    return datos


if __name__ == "__main__":
    # Ejemplo de uso
    datos = extraer_csv("notebooks/data/consumosustancias.csv")
    print(f"\nDatos extraídos exitosamente desde CSV")
    print(f"Dimensiones: {datos.shape[0]} filas x {datos.shape[1]} columnas")