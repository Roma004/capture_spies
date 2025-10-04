from loguru import logger
import pandas as pd

from backend.errors import UploadFileError

def load_file(upload_file, cols_required: set) -> pd.DataFrame:
    try:
        df = pd.read_csv(upload_file)
    except Exception as exc:
        logger.error("Can't load input file due to exception {exc}", exc=exc)
        raise UploadFileError(detail="File is not a valid CSV")
    cols_presented = set(df.columns)
    cols_missing = cols_required - cols_presented
    cols_odd = cols_presented - cols_required
    if cols_missing:
        logger.error(
            "Some columns are missing: {cols}",
            cols=', '.join(cols_missing)
        )
        raise UploadFileError(detail=f"Missing columns: {cols_missing}")
    if cols_odd:
        logger.error("Some columns are odd: {cols}", cols=', '.join(cols_odd))
        raise UploadFileError(detail=f"Missing columns: {cols_odd}")
    logger.info("File successfully loaded to DataFrame")
    return df

def check_non_empty(df: pd.DataFrame, *cols) -> None:
    if df.filter(cols).isna().any().any():
        raise UploadFileError(detail=f"Empty filed found in one of {cols}")

