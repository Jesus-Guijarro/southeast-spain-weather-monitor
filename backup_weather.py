import shutil
import subprocess
import os
from datetime import datetime
from pathlib import Path
import configparser
import platform

# Base directory and paths for configuration, backups, and logs
BASE_DIR       = Path(__file__).resolve().parent
CONFIG_PATH    = BASE_DIR / 'config.ini'
BACKUP_ROOT    = BASE_DIR / 'backup'
LOG_FILE       = BASE_DIR / 'logs' / 'pipeline.log'
KEEP_LAST      = 10  # number of ZIPs to keep

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Database connection settings
db_cfg = config['database']
DB_TYPE       = 'postgres'
DB_NAME       = db_cfg.get('dbname')
DB_USER       = db_cfg.get('user')
DB_PASSWORD   = db_cfg.get('password')
DB_HOST       = db_cfg.get('host', 'localhost')
DB_PORT       = db_cfg.get('port', '5432')

# Determine the appropriate pg_dump executable based on the OS
if platform.system() == 'Windows':
    PG_DUMP_EXE = r'C:\Program Files\PostgreSQL\17\bin\pg_dump.exe'
else:
    PG_DUMP_EXE = 'pg_dump'

def ensure_dirs():
    """
    Ensure that the backup root directory exists. Creates it (and any parents) if missing.
    """
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)

def timestamp() -> str:
    """
    Generate a date-based string for naming sessions (e.g., '2025-05-03').
    """
    return datetime.now().strftime('%Y-%m-%d')

def dump_database(session_dir: Path):
    """
    Dump the PostgreSQL database to a .sql file within the session directory.
    Uses pg_dump with credentials supplied via environment variable.
    """
    dump_path = session_dir / f'{DB_NAME}.sql'

    # Build the pg_dump command arguments
    cmd = [
        PG_DUMP_EXE,
        '-w',                  # no password prompt (use PGPASSWORD)
        '-U', DB_USER,         # database user
        '-h', DB_HOST,         # database host
        '-p', DB_PORT,         # database port
        '-f', str(dump_path),  # output file
        DB_NAME,               # database name
    ]

    # Copy current environment and inject the password for pg_dump
    env = os.environ.copy()
    env['PGPASSWORD'] = DB_PASSWORD

    # Execute the dump command, raising an error if it fails
    subprocess.run(cmd, check=True, env=env)

def copy_log(session_dir: Path):
    """
    Copy the pipeline log file into the session directory,
    appending the current date to avoid overwriting previous logs.
    """
    # Create a timestamped log filename (e.g., pipeline_2024-12-01.log)
    shutil.copy2(LOG_FILE, session_dir / f'{LOG_FILE.stem}_{timestamp()}{LOG_FILE.suffix}')

def compress_and_cleanup(session_dir: Path) -> Path:
    """
    Compress the session directory into a ZIP archive and remove the original files.
    Returns the path to the created ZIP.
    """
    # Name the archive using the session directory name (date)
    zip_name = f'backup_data_and_logs_{session_dir.name}.zip'
    zip_path = BACKUP_ROOT / zip_name

    # Create the ZIP archive (shutil.make_archive appends .zip)
    shutil.make_archive(str(zip_path.with_suffix('')), 'zip', session_dir)
    shutil.rmtree(session_dir)
    return zip_path

def rotate_log():
    """
    Clear the contents of the main log file after successful backup,
    starting fresh for the next run.
    """
    # Overwrite the log file with an empty string
    LOG_FILE.write_text('')

def purge_old_backups():
    """
    Remove oldest backup archives, keeping only the most recent KEEP_LAST files.
    """
    # Find all ZIPs matching the backup naming pattern
    archives = sorted(BACKUP_ROOT.glob('weather_backup_*.zip'))

    # Delete archives older than the most recent KEEP_LAST
    for old in archives[:-KEEP_LAST]:
        old.unlink()

def main():
    """
    Main orchestration function to run the complete backup pipeline:
    1. Prepare directories
    2. Dump database
    3. Copy latest logs
    4. Compress and clean up
    5. Rotate logs
    6. Purge old backups
    """
    ensure_dirs()

    # Create a session-specific directory named by the current date
    session = BACKUP_ROOT / timestamp()
    session.mkdir()

    # Perform backup steps
    dump_database(session)
    copy_log(session)
    zip_file = compress_and_cleanup(session)
    rotate_log()
    purge_old_backups()

    print(f'Backup completed: {zip_file}')

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'Error: {exc}')
        raise
