import pandas as pd
import logging
import numpy as np
from typing import Tuple, Optional


def clean_mortality_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting mortality data cleaning")
    initial_rows = len(df)
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Convert year to integer
    if 'YEAR (DISPLAY)' in df_clean.columns:
        df_clean['year'] = pd.to_numeric(df_clean['YEAR (DISPLAY)'], errors='coerce')
        df_clean = df_clean.dropna(subset=['year'])
        df_clean['year'] = df_clean['year'].astype(int)
    
    # Convert numeric value
    if 'Numeric' in df_clean.columns:
        df_clean['value'] = pd.to_numeric(df_clean['Numeric'], errors='coerce')
    
    # Extract indicator information
    if 'GHO (CODE)' in df_clean.columns:
        df_clean['indicator_code'] = df_clean['GHO (CODE)']
    
    if 'GHO (DISPLAY)' in df_clean.columns:
        df_clean['indicator_name'] = df_clean['GHO (DISPLAY)']
    
    # Extract region information
    if 'REGION (DISPLAY)' in df_clean.columns:
        df_clean['region'] = df_clean['REGION (DISPLAY)']
    
    # Extract gender/dimension information
    if 'DIMENSION (NAME)' in df_clean.columns:
        df_clean['dimension'] = df_clean['DIMENSION (NAME)']
    
    # Remove rows with missing essential data
    essential_cols = ['year', 'value']
    df_clean = df_clean.dropna(subset=essential_cols)
    
    final_rows = len(df_clean)
    logging.info(f"Cleaned mortality data: {initial_rows} -> {final_rows} rows ({initial_rows - final_rows} removed)")
    
    return df_clean


def clean_socioeconomic_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean socioeconomic data from World Bank API.
    Handles both long format (year, indicator, value) and wide format (year, indicator1, indicator2, ...).
    """
    logging.info("Starting socioeconomic data cleaning")
    
    if df.empty:
        logging.warning("Socioeconomic data is empty")
        return df
    
    initial_rows = len(df)
    
    # Check if data is already in wide format (pivoted)
    if 'value' not in df.columns and 'year' in df.columns:
        # Data is already pivoted (wide format)
        df_clean = df.copy()
        
        # Convert year to integer
        df_clean['year'] = df_clean['year'].astype(int)
        
        # Convert all numeric columns to float
        for col in df_clean.columns:
            if col != 'year':
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        logging.info(f"Cleaned socioeconomic data (wide format): {initial_rows} rows, {len(df_clean.columns)} columns")
        
    else:
        # Data is in long format (year, indicator, value)
        # Remove rows with missing values
        df_clean = df.dropna(subset=['year', 'value'])
        
        # Convert year to integer
        df_clean['year'] = df_clean['year'].astype(int)
        
        # Convert value to float
        df_clean['value'] = pd.to_numeric(df_clean['value'], errors='coerce')
        df_clean = df_clean.dropna(subset=['value'])
        
        final_rows = len(df_clean)
        logging.info(f"Cleaned socioeconomic data (long format): {initial_rows} -> {final_rows} rows")
    
    return df_clean


def aggregate_mortality_indicators(df: pd.DataFrame) -> pd.DataFrame:

    logging.info("Aggregating mortality indicators")
    
    # Select key mortality indicators for analysis
    key_indicators = [
        'MDG_0000000001',  # Infant mortality rate
        'MDG_0000000007',  # Under-five mortality rate
        'WHOSIS_000003',   # Neonatal mortality rate
        'CM_01',           # Number of under-five deaths
        'CM_02',           # Number of infant deaths
        'CHILDMORT5TO14',  # Mortality rate for 5-14 year-olds
        'DEATHADO',        # Deaths among adolescents
        'MORTADO'          # Adolescent mortality rate
    ]
    
    # Filter for key indicators
    df_key = df[df['indicator_code'].isin(key_indicators)].copy()
    
    logging.info(f"Selected {len(df_key)} records for key indicators")
    
    # Aggregate by year and indicator - take mean if multiple records
    df_agg = df_key.groupby(['year', 'indicator_code'])['value'].mean().reset_index()
    
    # Pivot to wide format
    df_wide = df_agg.pivot(
        index='year',
        columns='indicator_code',
        values='value'
    ).reset_index()
    
    # Rename columns for clarity
    column_mapping = {
        'MDG_0000000001': 'infant_mortality_rate',
        'MDG_0000000007': 'under_five_mortality_rate',
        'WHOSIS_000003': 'neonatal_mortality_rate',
        'CM_01': 'under_five_deaths_count',
        'CM_02': 'infant_deaths_count',
        'CHILDMORT5TO14': 'child_5_14_mortality_rate',
        'DEATHADO': 'adolescent_deaths_count',
        'MORTADO': 'adolescent_mortality_rate'
    }
    
    df_wide = df_wide.rename(columns=column_mapping)
    
    logging.info(f"Aggregated data shape: {df_wide.shape}")
    logging.info(f"Available indicators: {[col for col in df_wide.columns if col != 'year']}")
    
    return df_wide


def aggregate_socioeconomic_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate socioeconomic data.
    Handles both long format (needs pivoting) and wide format (already pivoted).
    """
    logging.info("Aggregating socioeconomic data")
    
    if df.empty:
        logging.warning("No socioeconomic data to aggregate")
        return pd.DataFrame({'year': []})
    
    # Check if data is already in wide format (pivoted)
    if 'indicator_name' not in df.columns and 'year' in df.columns:
        # Data is already pivoted (wide format) - no aggregation needed
        logging.info(f"Socioeconomic data already in wide format: {df.shape}")
        return df
    
    # Data is in long format - need to pivot
    df_wide = df.pivot(
        index='year',
        columns='indicator_name',
        values='value'
    ).reset_index()
    
    logging.info(f"Aggregated socioeconomic data shape: {df_wide.shape}")
    
    return df_wide


def merge_datasets(
    df_mortality: pd.DataFrame,
    df_socioeconomic: pd.DataFrame
) -> pd.DataFrame:
    logging.info("=" * 70)
    logging.info("MERGING DATASETS")
    logging.info("=" * 70)
    
    # Verify both dataframes have 'year' column
    if 'year' not in df_mortality.columns:
        raise ValueError("Mortality data missing 'year' column")
    
    if df_socioeconomic.empty:
        logging.warning("Socioeconomic data is empty. Returning mortality data only.")
        return df_mortality
    
    if 'year' not in df_socioeconomic.columns:
        raise ValueError("Socioeconomic data missing 'year' column")
    
    # Log data before merge
    logging.info(f"Mortality data: {df_mortality.shape} - Years: {df_mortality['year'].min()}-{df_mortality['year'].max()}")
    logging.info(f"Socioeconomic data: {df_socioeconomic.shape} - Years: {df_socioeconomic['year'].min()}-{df_socioeconomic['year'].max()}")
    
    # Find common years
    common_years = set(df_mortality['year']) & set(df_socioeconomic['year'])
    logging.info(f"Common years between datasets: {len(common_years)}")
    
    if len(common_years) == 0:
        logging.warning("No common years found between datasets!")
        logging.warning("Mortality years: " + str(sorted(df_mortality['year'].unique())[:10]))
        logging.warning("Socioeconomic years: " + str(sorted(df_socioeconomic['year'].unique())[:10]))
    
    # Perform LEFT merge on year
    # Left join keeps ALL mortality data, even if no socioeconomic data exists
    df_merged = pd.merge(
        df_mortality,
        df_socioeconomic,
        on='year',
        how='left',  # Keep ALL mortality records, fill missing socioeconomic with NaN
        suffixes=('_mortality', '_socioeconomic')
    )
    
    logging.info(f"Merged data shape: {df_merged.shape}")
    logging.info(f"Merged data years: {df_merged['year'].min()}-{df_merged['year'].max()}")
    logging.info(f"Total columns after merge: {len(df_merged.columns)}")
    
    # List all columns
    mortality_cols = [col for col in df_merged.columns if col in df_mortality.columns or col.endswith('_mortality')]
    socioeconomic_cols = [col for col in df_merged.columns if col in df_socioeconomic.columns and col != 'year']
    
    logging.info(f"Mortality indicators: {len(mortality_cols)}")
    logging.info(f"Socioeconomic indicators: {len(socioeconomic_cols)}")
    
    return df_merged


def handle_missing_values(df: pd.DataFrame, strategy: str = 'drop') -> pd.DataFrame:
    """
    Maneja valores faltantes con diferentes estrategias de imputación.
    
    Estrategias disponibles:
    - 'drop': Eliminar filas con valores faltantes
    - 'fill': Rellenar con 0
    - 'forward': Forward fill (propagación hacia adelante)
    - 'interpolate': Interpolación lineal inteligente (RECOMENDADO)
    """
    
    logging.info(f"Handling missing values using strategy: {strategy}")
    
    initial_rows = len(df)
    missing_summary = df.isnull().sum()
    cols_with_missing = missing_summary[missing_summary > 0]
    
    if len(cols_with_missing) > 0:
        logging.info("Columns with missing values:")
        for col, count in cols_with_missing.items():
            pct = (count / initial_rows) * 100
            logging.info(f"  - {col}: {count} ({pct:.1f}%)")
    else:
        logging.info("No missing values found")
        return df
    
    if strategy == 'drop':
        # Drop rows with any missing values
        df_clean = df.dropna()
        logging.info(f"Dropped rows: {initial_rows} -> {len(df_clean)} ({initial_rows - len(df_clean)} removed)")
        
    elif strategy == 'fill':
        # Fill numeric columns with 0 (para indicadores faltantes)
        df_clean = df.copy()
        for col in df_clean.columns:
            if df_clean[col].dtype in ['float64', 'int64']:
                # Fill numeric with 0 (más apropiado para indicadores no disponibles)
                df_clean[col] = df_clean[col].fillna(0)
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    logging.info(f"  Filled {col}: {missing_count} valores con 0")
            else:
                # Fill categorical with mode
                mode_val = df_clean[col].mode()[0] if not df_clean[col].mode().empty else 'Unknown'
                df_clean[col] = df_clean[col].fillna(mode_val)
                logging.info(f"  Filled {col} with mode: {mode_val}")
                
    elif strategy == 'forward':
        # Forward fill (useful for time series)
        df_clean = df.sort_values('year').fillna(method='ffill')
        logging.info("Applied forward fill based on year order")
    
    elif strategy == 'interpolate':
        # IMPUTACIÓN INTELIGENTE - Método recomendado para series temporales
        logging.info("Using INTELLIGENT INTERPOLATION for missing values")
        df_clean = df.copy()
        
        # Asegurar que esté ordenado por año
        if 'year' in df_clean.columns:
            df_clean = df_clean.sort_values('year').reset_index(drop=True)
        
        for col in cols_with_missing.index:
            if col == 'year':
                continue
                
            missing_count_before = df_clean[col].isnull().sum()
            
            if df_clean[col].dtype in ['float64', 'int64']:
                # Estrategia híbrida para columnas numéricas
                
                # 1. Primero intentar interpolación lineal (mejor para tendencias)
                df_clean[col] = df_clean[col].interpolate(
                    method='linear',
                    limit_direction='both',  # Interpolar en ambas direcciones
                    limit=5  # Limitar a 5 valores consecutivos
                )
                
                # 2. Para valores al inicio/final, usar forward/backward fill
                df_clean[col] = df_clean[col].ffill().bfill()
                
                # 3. Si aún quedan valores faltantes (muy raro), usar la mediana
                if df_clean[col].isnull().any():
                    median_val = df_clean[col].median()
                    df_clean[col] = df_clean[col].fillna(median_val)
                
                missing_count_after = df_clean[col].isnull().sum()
                imputed_count = missing_count_before - missing_count_after
                
                if imputed_count > 0:
                    logging.info(f"  [OK] {col}: Imputados {imputed_count} valores usando interpolacion")
            else:
                # Fill categorical with forward fill, then mode
                df_clean[col] = df_clean[col].ffill().bfill()
                if df_clean[col].isnull().any():
                    mode_val = df_clean[col].mode()[0] if not df_clean[col].mode().empty else 'Unknown'
                    df_clean[col] = df_clean[col].fillna(mode_val)
                logging.info(f"  [OK] {col}: Imputado con forward/backward fill + mode")
        
        # Verificar que no queden valores faltantes
        remaining_nulls = df_clean.isnull().sum().sum()
        if remaining_nulls == 0:
            logging.info("[OK] Todos los valores faltantes fueron imputados exitosamente")
        else:
            logging.warning(f"[!] Quedan {remaining_nulls} valores faltantes despues de la imputacion")
        
    else:
        raise ValueError(f"Unknown strategy: {strategy}. Use 'drop', 'fill', 'forward', or 'interpolate'")
    
    final_rows = len(df_clean)
    logging.info(f"Final dataset after handling missing values: {final_rows} rows")
    
    return df_clean


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    
    logging.info("Converting data types")
    
    df_converted = df.copy()
    
    # Ensure year is integer
    if 'year' in df_converted.columns:
        df_converted['year'] = df_converted['year'].astype(int)
    
    # Convert all numeric columns to float
    numeric_cols = df_converted.select_dtypes(include=['object']).columns
    for col in numeric_cols:
        if col != 'year':
            try:
                df_converted[col] = pd.to_numeric(df_converted[col], errors='ignore')
            except:
                pass
    
    logging.info("Data types converted successfully")
    logging.info(f"Data types:\n{df_converted.dtypes}")
    
    return df_converted


def full_transformation_pipeline_mortality(
    df_mortality_raw: pd.DataFrame,
    df_socioeconomic_raw: pd.DataFrame,
    missing_value_strategy: str = 'drop'
) -> pd.DataFrame:
    
    logging.info("=" * 70)
    logging.info("STARTING MORTALITY DATA TRANSFORMATION PIPELINE")
    logging.info("=" * 70)
    
    # Step 1: Clean mortality data
    logging.info("\n[Step 1/6] Cleaning mortality data...")
    df_mortality_clean = clean_mortality_data(df_mortality_raw)
    
    # Step 2: Clean socioeconomic data
    logging.info("\n[Step 2/6] Cleaning socioeconomic data...")
    df_socioeconomic_clean = clean_socioeconomic_data(df_socioeconomic_raw)
    
    # Step 3: Aggregate mortality indicators
    logging.info("\n[Step 3/6] Aggregating mortality indicators...")
    df_mortality_agg = aggregate_mortality_indicators(df_mortality_clean)
    
    # Step 4: Aggregate socioeconomic data
    logging.info("\n[Step 4/6] Aggregating socioeconomic data...")
    df_socioeconomic_agg = aggregate_socioeconomic_data(df_socioeconomic_clean)
    
    # Step 5: Merge datasets
    logging.info("\n[Step 5/6] Merging datasets...")
    df_merged = merge_datasets(df_mortality_agg, df_socioeconomic_agg)
    
    # Step 6: Handle missing values
    logging.info("\n[Step 6/6] Handling missing values...")
    df_final = handle_missing_values(df_merged, strategy=missing_value_strategy)
    
    # Step 7: Convert data types
    logging.info("\n[Step 7/6] Converting data types...")
    df_final = convert_data_types(df_final)
    
    # Final summary
    logging.info("\n" + "=" * 70)
    logging.info("TRANSFORMATION PIPELINE COMPLETED")
    logging.info("=" * 70)
    logging.info(f"Final dataset shape: {df_final.shape}")
    logging.info(f"Year range: {df_final['year'].min()} - {df_final['year'].max()}")
    logging.info(f"Total indicators: {len(df_final.columns) - 1}")  # Exclude year column
    
    # Show sample of available indicators
    indicator_cols = [col for col in df_final.columns if col != 'year']
    logging.info(f"\nAvailable indicators ({len(indicator_cols)} total):")
    for col in indicator_cols[:10]:
        logging.info(f"  - {col}")
    if len(indicator_cols) > 10:
        logging.info(f"  ... and {len(indicator_cols) - 10} more")
    
    return df_final
